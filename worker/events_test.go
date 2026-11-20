package worker

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/nicodarge/paperclip-plugin-go/rpc"
)

const eventLine = `{"event":{"eventId":"e1","eventType":"agent.run.cancelled","occurredAt":"2026-11-20T10:00:00Z","entityId":"run-1","entityType":"run","companyId":"c1","payload":{"runId":"run-1"}}}`

func (h *fakeHost) push(t *testing.T, line string) {
	t.Helper()
	e, err := rpc.Parse([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	if err := h.conn.Send(e); err != nil {
		t.Fatal(err)
	}
}

func (h *fakeHost) nextNotification(t *testing.T) rpc.Envelope {
	t.Helper()
	select {
	case n := <-h.notifs:
		return n
	case <-time.After(5 * time.Second):
		t.Fatal("no notification from the worker")
		return nil
	}
}

type seenEvent struct {
	ctx   context.Context
	event Event
}

func TestEventsAreDeliveredAsRequestsAndAsNotifications(t *testing.T) {
	seen := make(chan seenEvent, 4)
	h := start(t, func(w *Worker) {
		w.HandleEvent(func(ctx context.Context, e Event) error {
			seen <- seenEvent{ctx, e}
			return nil
		})
	})

	resp := h.call(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+eventLine+`,"paperclipInvocation":{"id":"inv-1","scope":{"companyId":"c1"}}}`)
	if string(resp["result"]) != "null" {
		t.Errorf("request result: %s", resp.Encode())
	}
	got := <-seen
	if got.event.EventType != "agent.run.cancelled" || got.event.EntityID != "run-1" || got.event.CompanyID != "c1" ||
		got.event.EventID != "e1" || got.event.OccurredAt != "2026-11-20T10:00:00Z" || string(got.event.Payload) != `{"runId":"run-1"}` {
		t.Errorf("event: %+v", got.event)
	}
	if id := InvocationID(got.ctx); id != "inv-1" {
		t.Errorf("request invocation: %q", id)
	}

	h.push(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+eventLine+`,"paperclipInvocation":{"id":"inv-2","scope":{"companyId":"c1"}}}`)
	got = <-seen
	if got.event.EventType != "agent.run.cancelled" || InvocationID(got.ctx) != "inv-2" {
		t.Errorf("notification: %+v, invocation %q", got.event, InvocationID(got.ctx))
	}

	h.push(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+eventLine+`}`)
	if got = <-seen; InvocationID(got.ctx) != "" {
		t.Errorf("a notification without an invocation was scoped to %q", InvocationID(got.ctx))
	}
}

func TestAnEventNotificationNeverBlocksTheReadingGoroutine(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	h := start(t, func(w *Worker) {
		w.Handle("ping", func(context.Context, json.RawMessage) (any, error) { return "pong", nil })
		w.HandleEvent(func(context.Context, Event) error {
			close(started)
			<-release
			return nil
		})
	})
	h.push(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+eventLine+`}`)
	<-started
	if resp := h.call(t, `{"jsonrpc":"2.0","method":"ping"}`); string(resp["result"]) != `"pong"` {
		t.Errorf("ping: %s", resp.Encode())
	}
	close(release)
}

func TestAnEventHandlerFailureIsLoggedNotAnswered(t *testing.T) {
	h := start(t, func(w *Worker) {
		w.HandleEvent(func(_ context.Context, e Event) error {
			if e.EntityID == "boom" {
				panic("exploded")
			}
			return errors.New("refused")
		})
	})

	resp := h.call(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+eventLine+`,"paperclipInvocation":{"id":"inv-3"}}`)
	if resp.Err() != nil || string(resp["result"]) != "null" {
		t.Errorf("the request failed: %s", resp.Encode())
	}
	n := h.nextNotification(t)
	if n.Method() != "log" || string(n["paperclipInvocationId"]) != `"inv-3"` ||
		string(n["params"]) != `{"level":"error","message":"Event handler failed: refused","meta":{"eventType":"agent.run.cancelled"}}` {
		t.Errorf("log: %s", n.Encode())
	}

	h.push(t, `{"jsonrpc":"2.0","method":"onEvent","params":{"event":{"eventType":"x","entityId":"boom"}}}`)
	n = h.nextNotification(t)
	if !strings.Contains(string(n["params"]), "Event handler failed: panic: exploded") {
		t.Errorf("panic log: %s", n.Encode())
	}
}

func TestEventParamsThatAreNotAnEvent(t *testing.T) {
	called := make(chan struct{}, 4)
	h := start(t, func(w *Worker) {
		w.HandleEvent(func(context.Context, Event) error { called <- struct{}{}; return nil })
	})
	for _, params := range []string{`{}`, `"text"`, `{"event":"text"}`} {
		resp := h.call(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+params+`}`)
		if e := resp.Err(); e == nil || e.Code != rpc.CodeInvalidParams {
			t.Errorf("request with %s: %s", params, resp.Encode())
		}
		h.push(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+params+`}`)
		n := h.nextNotification(t)
		if n.Method() != "log" || !strings.Contains(string(n["params"]), `"level":"error","message":"Failed to handle event notification: Invalid params`) {
			t.Errorf("notification with %s: %s", params, n.Encode())
		}
	}
	// The SDK ignores a notification without params.
	h.push(t, `{"jsonrpc":"2.0","method":"onEvent"}`)
	h.push(t, `{"jsonrpc":"2.0","method":"onEvent","params":null}`)
	if resp := h.call(t, `{"jsonrpc":"2.0","method":"nothing"}`); resp.Err() == nil {
		t.Errorf("unexpected answer: %s", resp.Encode())
	}
	select {
	case n := <-h.notifs:
		t.Errorf("a notification without params produced %s", n.Encode())
	case <-called:
		t.Errorf("the handler ran for params that are not an event")
	default:
	}
}

func TestWithoutHandleEventOnEventKeepsGoingToTheForwarders(t *testing.T) {
	forwarded := make(chan string, 1)
	h := start(t, func(w *Worker) {
		w.ForwardNotification = func(n rpc.Envelope) { forwarded <- n.Method() }
	})
	h.push(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+eventLine+`}`)
	if got := <-forwarded; got != "onEvent" {
		t.Errorf("forwarded %s", got)
	}
	if resp := h.call(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+eventLine+`}`); resp.Err() == nil || resp.Err().Code != rpc.CodeMethodNotFound {
		t.Errorf("request: %s", resp.Encode())
	}
}

func TestWaitCoversTheEventNotificationsInFlight(t *testing.T) {
	var w *Worker
	done := make(chan struct{})
	h := start(t, func(worker *Worker) {
		w = worker
		w.HandleEvent(func(context.Context, Event) error {
			time.Sleep(50 * time.Millisecond)
			close(done)
			return nil
		})
	})
	h.push(t, `{"jsonrpc":"2.0","method":"onEvent","params":`+eventLine+`}`)
	h.call(t, `{"jsonrpc":"2.0","method":"nothing"}`)
	w.Wait()
	select {
	case <-done:
	default:
		t.Error("Wait returned while an event notification was being handled")
	}
}

func TestInvocationIDOf(t *testing.T) {
	for line, want := range map[string]string{
		`{"jsonrpc":"2.0","method":"m","paperclipInvocation":{"id":"inv-1","scope":{"companyId":"c"}}}`: "inv-1",
		`{"jsonrpc":"2.0","method":"m"}`:                                "",
		`{"jsonrpc":"2.0","method":"m","paperclipInvocation":"inv-1"}`:  "",
		`{"jsonrpc":"2.0","method":"m","paperclipInvocation":{"id":7}}`: "",
	} {
		e, err := rpc.Parse([]byte(line))
		if err != nil {
			t.Fatal(err)
		}
		if got := InvocationIDOf(e); got != want {
			t.Errorf("%s: %q, want %q", line, got, want)
		}
	}
}
