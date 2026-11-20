package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestKindFollowsTheSDKGuards(t *testing.T) {
	cases := map[string]Kind{
		`{"jsonrpc":"2.0","id":1,"method":"runJob"}`:             KindRequest,
		`{"jsonrpc":"2.0","id":"a","method":"runJob"}`:           KindRequest,
		`{"jsonrpc":"2.0","method":"onEvent","params":{}}`:       KindNotification,
		`{"jsonrpc":"2.0","id":1,"result":null}`:                 KindResponse,
		`{"jsonrpc":"2.0","id":null,"error":{"code":-32700}}`:    KindResponse,
		`{"jsonrpc":"2.0","id":1,"method":"x","result":1}`:       KindResponse,
		`{"jsonrpc":"2.0","id":null,"method":"runJob"}`:          KindInvalid,
		`{"jsonrpc":"2.0","id":1}`:                               KindInvalid,
		`{"jsonrpc":"2.0","method":7}`:                           KindInvalid,
		`{"jsonrpc":"2.0","method":""}`:                          KindInvalid,
		`{"jsonrpc":"2.0","id":1,"method":"x","paperclipX":[1]}`: KindRequest,
	}
	for line, want := range cases {
		e, err := Parse([]byte(line))
		if err != nil {
			t.Fatalf("%s: %v", line, err)
		}
		if got := e.Kind(); got != want {
			t.Errorf("%s: kind %d, want %d", line, got, want)
		}
	}
}

func TestParseRefusesWhatIsNotJSONRPC2(t *testing.T) {
	for _, line := range []string{``, `   `, `not json`, `[1]`, `null`, `"x"`, `{"id":1,"method":"x"}`, `{"jsonrpc":"1.0","id":1}`, `{"jsonrpc":2.0}`} {
		if _, err := Parse([]byte(line)); err == nil {
			t.Errorf("%q was accepted", line)
		}
	}
}

func TestEncodeKeepsUnknownMembersAndRawValues(t *testing.T) {
	// Whitespace, escapes, a 2^53+1 integer and <>& must survive byte for byte.
	params := `{"big":9007199254740993, "s":"a<b>&é\n" ,"nested":{"k":[1, 2]}}`
	line := `{"jsonrpc":"2.0","id":7,"method":"handleWebhook","params":` + params + `,"paperclipInvocation":{"id":"inv-1","scope":{"companyId":"c"}},"future":true}`
	e, err := Parse([]byte(line))
	if err != nil {
		t.Fatal(err)
	}
	out := e.With("id", json.RawMessage("42")).Encode()
	if out[len(out)-1] != '\n' || bytes.Count(out, []byte("\n")) != 1 {
		t.Fatalf("not exactly one line: %q", out)
	}
	back, err := Parse(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(back["params"]) != params {
		t.Errorf("params rewritten: %s", back["params"])
	}
	if string(back["id"]) != "42" || string(back["future"]) != "true" || string(back["paperclipInvocation"]) != `{"id":"inv-1","scope":{"companyId":"c"}}` {
		t.Errorf("members lost: %s", out)
	}
	if string(e["id"]) != "7" {
		t.Errorf("With mutated the original")
	}
	if _, has := e.Without("id")["id"]; has {
		t.Errorf("Without kept the member")
	}
}

func TestNewResultAndNewError(t *testing.T) {
	res, err := NewResult(json.RawMessage("3"), map[string]string{"a": "<&>"})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(res.Encode()); got != `{"id":3,"jsonrpc":"2.0","result":{"a":"<&>"}}`+"\n" {
		t.Errorf("result: %s", got)
	}
	null, _ := NewResult(json.RawMessage("3"), nil)
	if string(null["result"]) != "null" {
		t.Errorf("nil result: %s", null["result"])
	}
	e := NewError(nil, &Error{Code: CodeWorkerError, Message: "boom", Data: json.RawMessage(`{"k":1}`)})
	if string(e["id"]) != "null" {
		t.Errorf("id: %s", e["id"])
	}
	if got := e.Err(); got == nil || got.Code != CodeWorkerError || got.Message != "boom" || string(got.Data) != `{"k":1}` {
		t.Errorf("error: %+v", got)
	}
}

// pipePair returns two Conns wired to each other.
func pipePair() (a, b *Conn, closeA func()) {
	ar, bw := io.Pipe()
	br, aw := io.Pipe()
	return NewConn(ar, aw), NewConn(br, bw), func() { _ = aw.Close() }
}

func TestRequestsGetTheirOwnResponseWhateverTheOrder(t *testing.T) {
	a, b, closeA := pipePair()
	defer closeA()
	var mu sync.Mutex
	var held []Envelope
	release := make(chan struct{})
	go func() {
		_ = b.Serve(Handlers{Request: func(req Envelope) {
			mu.Lock()
			held = append(held, req)
			n := len(held)
			mu.Unlock()
			if n == 50 {
				close(release)
			}
		}})
	}()
	go func() { _ = a.Serve(Handlers{}) }()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			req, _ := NewRequest(nil, "echo", i)
			resp, err := a.Call(context.Background(), req)
			if err != nil {
				t.Error(err)
				return
			}
			var got int
			if err := json.Unmarshal(resp["result"], &got); err != nil {
				t.Errorf("call %d: unreadable result: %v", i, err)
			} else if got != i {
				t.Errorf("call %d got the response of %d", i, got)
			}
		}(i)
	}
	<-release
	mu.Lock()
	ids := map[string]bool{}
	for i := len(held) - 1; i >= 0; i-- { // answered in reverse
		req := held[i]
		ids[string(req.ID())] = true
		resp, _ := NewResult(req.ID(), req["params"])
		_ = b.Send(resp)
	}
	mu.Unlock()
	wg.Wait()
	if len(ids) != 50 {
		t.Errorf("%d distinct ids for 50 requests", len(ids))
	}
	if a.Pending() != 0 {
		t.Errorf("%d requests left pending", a.Pending())
	}
}

func TestCallStopsWaitingWithItsContext(t *testing.T) {
	a, b, closeA := pipePair()
	defer closeA()
	go func() { _ = b.Serve(Handlers{}) }()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	req, _ := NewRequest(nil, "never", nil)
	if _, err := a.Call(ctx, req); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if a.Pending() != 0 {
		t.Errorf("the abandoned call is still pending")
	}
}

func TestRequestThatCannotBeWrittenIsAnsweredHere(t *testing.T) {
	r, w := io.Pipe()
	_ = w.Close()
	_ = r.Close()
	c := NewConn(strings.NewReader(""), w)
	got := make(chan Envelope, 1)
	req, _ := NewRequest(nil, "x", nil)
	c.Request(req, func(e Envelope) { got <- e })
	if e := (<-got).Err(); e == nil || e.Code != CodeWorkerUnavailable {
		t.Errorf("reply: %+v", e)
	}
	if c.Pending() != 0 {
		t.Errorf("the failed request is still pending")
	}
}

func TestServeClassifiesAndSurvivesBadLines(t *testing.T) {
	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"a"}`,
		``,
		`garbage`,
		`{"jsonrpc":"2.0","method":"n"}`,
		`{"jsonrpc":"2.0","id":null,"method":"odd"}`,
		`{"jsonrpc":"2.0","id":99,"result":1}`,
		strings.Repeat("x", 300),
		`{"jsonrpc":"2.0","id":2,"method":"last"}`, // no trailing newline
	}, "\n")
	c := NewConn(strings.NewReader(input), io.Discard)
	c.maxLine = 200
	var seen []string
	err := c.Serve(Handlers{
		Request:      func(e Envelope) { seen = append(seen, "req:"+e.Method()) },
		Notification: func(e Envelope) { seen = append(seen, "notif:"+e.Method()) },
		Other:        func(e Envelope) { seen = append(seen, "other:"+e.Method()) },
		Unparseable: func(line []byte, _ error) {
			if line == nil {
				seen = append(seen, "toolong")
			} else {
				seen = append(seen, "bad:"+strings.TrimSpace(string(line)))
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "req:a bad:garbage notif:n other:odd toolong req:last"
	if got := strings.Join(seen, " "); got != want {
		t.Errorf("got  %s\nwant %s", got, want)
	}
}
