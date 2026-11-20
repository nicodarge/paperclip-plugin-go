package worker

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/nicodarge/paperclip-plugin-go/rpc"
)

// fakeHost is the host end of a Worker: it records what the worker sends and
// answers worker-to-host calls from answers.
type fakeHost struct {
	conn    *rpc.Conn
	mu      sync.Mutex
	calls   []rpc.Envelope
	notifs  chan rpc.Envelope
	answers map[string]string
}

func start(t *testing.T, setup func(*Worker)) *fakeHost {
	t.Helper()
	workerIn, hostOut := io.Pipe()
	hostIn, workerOut := io.Pipe()
	w := NewWorker(workerIn, workerOut)
	w.Stderr = io.Discard
	setup(w)
	h := &fakeHost{conn: rpc.NewConn(hostIn, hostOut), notifs: make(chan rpc.Envelope, 16), answers: map[string]string{}}
	go func() {
		_ = h.conn.Serve(rpc.Handlers{
			Request: func(req rpc.Envelope) {
				h.mu.Lock()
				h.calls = append(h.calls, req)
				answer, ok := h.answers[req.Method()]
				h.mu.Unlock()
				if !ok {
					_ = h.conn.Send(rpc.NewError(req.ID(), rpc.Errorf(rpc.CodeCapabilityDenied, "denied: %s", req.Method())))
					return
				}
				resp, _ := rpc.NewResult(req.ID(), json.RawMessage(answer))
				_ = h.conn.Send(resp)
			},
			Notification: func(n rpc.Envelope) { h.notifs <- n },
		})
	}()
	served := make(chan struct{})
	go func() { _ = w.Serve(); close(served) }()
	t.Cleanup(func() { _ = hostOut.Close(); <-served; _ = workerOut.Close() })
	return h
}

func (h *fakeHost) call(t *testing.T, line string) rpc.Envelope {
	t.Helper()
	req, err := rpc.Parse([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := h.conn.Call(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func (h *fakeHost) lastCall() rpc.Envelope {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.calls[len(h.calls)-1]
}

func TestHandlerCallsTheHostInsideItsInvocation(t *testing.T) {
	h := start(t, func(w *Worker) {
		w.Handle("runJob", func(ctx context.Context, _ json.RawMessage) (any, error) {
			var n int
			found, err := w.State().Get(ctx, ScopeKey{ScopeKind: "instance", StateKey: "counter"}, &n)
			if err != nil || !found {
				return nil, err
			}
			if err := w.State().Set(ctx, ScopeKey{ScopeKind: "company", ScopeID: "c1", Namespace: "ns", StateKey: "counter"}, n+1); err != nil {
				return nil, err
			}
			w.Logger().Info(ctx, "bumped", map[string]any{"n": n + 1})
			return map[string]int{"count": n + 1}, nil
		})
	})
	h.answers["state.get"] = "41"
	h.answers["state.set"] = "null"

	resp := h.call(t, `{"jsonrpc":"2.0","method":"runJob","params":{},"paperclipInvocation":{"id":"inv-7","scope":{"companyId":"c1"}}}`)
	if string(resp["result"]) != `{"count":42}` {
		t.Fatalf("result: %s (%v)", resp["result"], resp.Err())
	}
	set := h.lastCall()
	if string(set["paperclipInvocationId"]) != `"inv-7"` {
		t.Errorf("state.set did not echo the invocation: %s", set.Encode())
	}
	if string(set["params"]) != `{"scopeKind":"company","scopeId":"c1","namespace":"ns","stateKey":"counter","value":42}` {
		t.Errorf("state.set params: %s", set["params"])
	}
	if get := h.calls[0]; string(get["params"]) != `{"scopeKind":"instance","stateKey":"counter"}` || string(get["paperclipInvocationId"]) != `"inv-7"` {
		t.Errorf("state.get: %s", get.Encode())
	}
	log := <-h.notifs
	if log.Method() != "log" || string(log["params"]) != `{"level":"info","message":"bumped","meta":{"n":42}}` || string(log["paperclipInvocationId"]) != `"inv-7"` {
		t.Errorf("log notification: %s", log.Encode())
	}
}

func TestCallOutsideAnInvocationEchoesNothing(t *testing.T) {
	h := start(t, func(w *Worker) {
		w.Handle("health", func(ctx context.Context, _ json.RawMessage) (any, error) {
			var cfg map[string]any
			if err := w.Config().Get(ctx, "", &cfg); err != nil {
				return nil, err
			}
			secret, err := w.Secrets().Resolve(ctx, "ref-1", SecretOptions{CompanyID: "c9"})
			return map[string]any{"cfg": cfg, "secret": secret}, err
		})
	})
	h.answers["config.get"] = `{"a":1}`
	h.answers["secrets.resolve"] = `"s3cret"`
	resp := h.call(t, `{"jsonrpc":"2.0","method":"health"}`)
	if string(resp["result"]) != `{"cfg":{"a":1},"secret":"s3cret"}` {
		t.Fatalf("result: %s (%v)", resp["result"], resp.Err())
	}
	for _, c := range h.calls {
		if _, has := c["paperclipInvocationId"]; has {
			t.Errorf("%s carries an invocation id outside any invocation", c.Method())
		}
	}
	if got := string(h.calls[0]["params"]); got != `{}` {
		t.Errorf("config.get params: %s", got)
	}
	if got := string(h.calls[1]["params"]); got != `{"secretRef":"ref-1","companyId":"c9"}` {
		t.Errorf("secrets.resolve params: %s", got)
	}
}

// elementAt panics with "index out of range" when i is past the end of s.
func elementAt(s []int, i int) int { return s[i] }

func TestErrorsReachTheHostWithTheirCode(t *testing.T) {
	h := start(t, func(w *Worker) {
		w.Handle("plain", func(context.Context, json.RawMessage) (any, error) { return nil, io.ErrUnexpectedEOF })
		w.Handle("typed", func(context.Context, json.RawMessage) (any, error) {
			return nil, &rpc.Error{Code: rpc.CodeMethodNotImplemented, Message: "nope", Data: json.RawMessage(`[1]`)}
		})
		w.Handle("denied", func(ctx context.Context, _ json.RawMessage) (any, error) {
			return nil, w.State().Delete(ctx, ScopeKey{ScopeKind: "instance", StateKey: "k"})
		})
		w.Handle("panics", func(context.Context, json.RawMessage) (any, error) {
			return elementAt(nil, 3), nil
		})
	})
	want := map[string]int{"plain": rpc.CodeWorkerError, "typed": rpc.CodeMethodNotImplemented, "denied": rpc.CodeCapabilityDenied, "panics": rpc.CodeWorkerError, "unknown": rpc.CodeMethodNotFound}
	for method, code := range want {
		resp := h.call(t, `{"jsonrpc":"2.0","method":"`+method+`"}`)
		e := resp.Err()
		if e == nil || e.Code != code {
			t.Errorf("%s: %+v, want code %d", method, e, code)
		}
		if method == "typed" && string(e.Data) != `[1]` {
			t.Errorf("typed: data %s", e.Data)
		}
		if method == "panics" && !strings.Contains(e.Message, "index out of range") {
			t.Errorf("panics: %s", e.Message)
		}
	}
	// The worker survived the panic.
	if resp := h.call(t, `{"jsonrpc":"2.0","method":"plain"}`); resp.Err() == nil {
		t.Errorf("no answer after a panic")
	}
}

func TestUnownedTrafficGoesToTheForwardersInOrder(t *testing.T) {
	var mu sync.Mutex
	var seen []string
	done := make(chan struct{})
	h := start(t, func(w *Worker) {
		w.Handle("owned", func(context.Context, json.RawMessage) (any, error) { return "go", nil })
		w.Forward = func(req rpc.Envelope) {
			mu.Lock()
			seen = append(seen, "req:"+req.Method())
			mu.Unlock()
			resp, _ := rpc.NewResult(req.ID(), "forwarded")
			_ = w.Host().Send(resp)
		}
		w.ForwardNotification = func(n rpc.Envelope) {
			mu.Lock()
			seen = append(seen, "notif:"+n.Method())
			mu.Unlock()
			if n.Method() == "end" {
				close(done)
			}
		}
	})
	if resp := h.call(t, `{"jsonrpc":"2.0","method":"owned"}`); string(resp["result"]) != `"go"` {
		t.Errorf("owned: %s", resp.Encode())
	}
	for _, line := range []string{`{"jsonrpc":"2.0","method":"onEvent"}`, `{"jsonrpc":"2.0","id":"h-1","method":"runJob"}`, `{"jsonrpc":"2.0","method":"end"}`} {
		e, _ := rpc.Parse([]byte(line))
		_ = h.conn.Send(e)
	}
	<-done
	if got := strings.Join(seen, " "); got != "notif:onEvent req:runJob notif:end" {
		t.Errorf("order: %s", got)
	}
}

func TestAfterReplyRunsOnceTheResponseIsWritten(t *testing.T) {
	ran := make(chan struct{})
	h := start(t, func(w *Worker) {
		w.Handle("shutdown", func(ctx context.Context, _ json.RawMessage) (any, error) {
			AfterReply(ctx, func() { close(ran) })
			if Request(ctx).Method() != "shutdown" {
				t.Errorf("Request(ctx) is not the served request")
			}
			return nil, nil
		})
	})
	resp := h.call(t, `{"jsonrpc":"2.0","method":"shutdown"}`)
	if string(resp["result"]) != "null" {
		t.Errorf("result: %s", resp.Encode())
	}
	<-ran
}

func TestOnlyRegisteredJobsAreServedByTheWorker(t *testing.T) {
	forwarded := make(chan string, 4)
	h := start(t, func(w *Worker) {
		w.HandleJob("job-a", func(ctx context.Context, params json.RawMessage) (any, error) {
			var company string
			err := w.CallHost(ctx, "state.get", ScopeKey{ScopeKind: "instance", StateKey: "company"}, &company)
			return map[string]any{"company": company, "params": params}, err
		})
		w.Forward = func(req rpc.Envelope) {
			forwarded <- string(req["params"])
			resp, _ := rpc.NewResult(req.ID(), "forwarded")
			_ = w.Host().Send(resp)
		}
	})
	h.answers["state.get"] = `"co-1"`

	own := `{"job":{"jobKey":"job-a","runId":"r1"}}`
	resp := h.call(t, `{"jsonrpc":"2.0","method":"runJob","params":`+own+`,"paperclipInvocation":{"id":"inv-9"}}`)
	if got := string(resp["result"]); got != `{"company":"co-1","params":`+own+`}` {
		t.Errorf("owned job: %s", resp.Encode())
	}
	if got := string(h.lastCall()["paperclipInvocationId"]); got != `"inv-9"` {
		t.Errorf("the job's host call echoed invocation %s", got)
	}

	for _, params := range []string{`{"job":{"jobKey":"job-b"}}`, `{"job":{"jobKey":"job-a-2"}}`, `{"job":"job-a"}`, `{}`} {
		resp := h.call(t, `{"jsonrpc":"2.0","method":"runJob","params":`+params+`}`)
		if string(resp["result"]) != `"forwarded"` {
			t.Errorf("runJob %s: %s", params, resp.Encode())
		}
		if got := <-forwarded; got != params {
			t.Errorf("runJob %s was forwarded as %s", params, got)
		}
	}
	if resp := h.call(t, `{"jsonrpc":"2.0","method":"handleWebhook","params":{"job":{"jobKey":"job-a"}}}`); string(resp["result"]) != `"forwarded"` {
		t.Errorf("another method naming the job: %s", resp.Encode())
	}
	<-forwarded
}

func TestAgentAndIssueClientsSendTheParametersTheSDKSends(t *testing.T) {
	h := start(t, func(w *Worker) {
		w.Handle("health", func(ctx context.Context, _ json.RawMessage) (any, error) {
			var agents []map[string]string
			if err := w.Agents().ListPage(ctx, "c1", 200, 400, &agents); err != nil {
				return nil, err
			}
			var issue *map[string]string
			if err := w.Issues().Get(ctx, "c1", "i1", &issue); err != nil {
				return nil, err
			}
			var comments []map[string]string
			if err := w.Issues().ListComments(ctx, "c1", "i1", &comments); err != nil {
				return nil, err
			}
			if err := w.Issues().CreateComment(ctx, "c1", "i1", "hello"); err != nil {
				return nil, err
			}
			err := w.Issues().Update(ctx, "c1", "i1", map[string]string{"assigneeAgentId": "a2"})
			return map[string]any{"agents": agents, "issueIsNull": issue == nil, "comments": comments}, err
		})
	})
	h.answers["agents.list"] = `[{"id":"a1"}]`
	h.answers["issues.get"] = `null`
	h.answers["issues.listComments"] = `[{"body":"b"}]`
	h.answers["issues.createComment"] = `{"id":"k1"}`
	h.answers["issues.update"] = `{"id":"i1"}`

	resp := h.call(t, `{"jsonrpc":"2.0","method":"health"}`)
	if got := string(resp["result"]); got != `{"agents":[{"id":"a1"}],"comments":[{"body":"b"}],"issueIsNull":true}` {
		t.Fatalf("result: %s (%v)", got, resp.Err())
	}
	want := []string{
		`agents.list {"companyId":"c1","limit":200,"offset":400}`,
		`issues.get {"issueId":"i1","companyId":"c1"}`,
		`issues.listComments {"issueId":"i1","companyId":"c1"}`,
		`issues.createComment {"issueId":"i1","body":"hello","companyId":"c1"}`,
		`issues.update {"issueId":"i1","patch":{"assigneeAgentId":"a2"},"companyId":"c1"}`,
	}
	if len(h.calls) != len(want) {
		t.Fatalf("%d host calls, want %d", len(h.calls), len(want))
	}
	for i, c := range h.calls {
		if got := c.Method() + " " + string(c["params"]); got != want[i] {
			t.Errorf("call %d: %s, want %s", i, got, want[i])
		}
	}
}
