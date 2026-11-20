package worker

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/nicodarge/paperclip-plugin-go/rpc"
)

const initializeLine = `{"jsonrpc":"2.0","method":"initialize","params":{"manifest":{"id":"p","capabilities":["events.subscribe"]},"config":{"k":1},"instanceInfo":{"instanceId":"i1","hostVersion":"1.2.3"},"apiVersion":1,"databaseNamespace":null}}`

func TestInitializeReportsTheMethodsTheWorkerServesInTheSDKsOrder(t *testing.T) {
	got := make(chan InitializeParams, 1)
	h := start(t, func(w *Worker) {
		for _, m := range []string{"environmentExecute", "onEvent", "runJob", "getData", "handleWebhook", "configChanged", "validateConfig", "duplexChannelClose"} {
			w.Handle(m, func(context.Context, json.RawMessage) (any, error) { return nil, nil })
		}
		w.HandleLifecycle(Lifecycle{Initialize: func(_ context.Context, p InitializeParams) error {
			got <- p
			return nil
		}})
	})
	resp := h.call(t, initializeLine)
	var result InitializeResult
	if err := json.Unmarshal(resp["result"], &result); err != nil {
		t.Fatalf("%v: %s", err, resp.Encode())
	}
	want := []string{"validateConfig", "configChanged", "health", "shutdown", "environmentExecute", "duplexChannelClose"}
	if !result.OK || !slices.Equal(result.SupportedMethods, want) {
		t.Errorf("result: %+v, want ok and %v", result, want)
	}
	p := <-got
	if string(p.Manifest) != `{"id":"p","capabilities":["events.subscribe"]}` || string(p.Config) != `{"k":1}` ||
		p.InstanceInfo != (InstanceInfo{InstanceID: "i1", HostVersion: "1.2.3"}) || p.APIVersion != 1 || p.DatabaseNamespace != "" {
		t.Errorf("params: %+v", p)
	}
}

func TestInitializeWithNothingRegisteredReportsAnEmptyListNotNull(t *testing.T) {
	h := start(t, func(w *Worker) { w.HandleLifecycle(Lifecycle{}) })
	resp := h.call(t, initializeLine)
	if string(resp["result"]) != `{"ok":true,"supportedMethods":["health","shutdown"]}` {
		t.Errorf("result: %s", resp.Encode())
	}
	bare := NewWorker(nil, nil)
	if got := bare.SupportedMethods(); got == nil || len(got) != 0 {
		t.Errorf("SupportedMethods() = %#v, want an empty non-nil list", got)
	}
}

func TestInitializeRunsOnceAndAFailureLeavesTheWorkerUninitialized(t *testing.T) {
	attempts := 0
	h := start(t, func(w *Worker) {
		w.HandleLifecycle(Lifecycle{Initialize: func(context.Context, InitializeParams) error {
			attempts++
			if attempts == 1 {
				return errors.New("setup failed")
			}
			return nil
		}})
	})
	if e := h.call(t, initializeLine).Err(); e == nil || e.Code != rpc.CodeWorkerError || e.Message != "setup failed" {
		t.Errorf("first: %+v", e)
	}
	if resp := h.call(t, initializeLine); resp.Err() != nil {
		t.Errorf("retry after a failed setup: %s", resp.Encode())
	}
	if e := h.call(t, initializeLine).Err(); e == nil || e.Code != rpc.CodeWorkerError || e.Message != "Worker already initialized" {
		t.Errorf("second: %+v", e)
	}
	if attempts != 2 {
		t.Errorf("Initialize ran %d times", attempts)
	}
}

func TestHealth(t *testing.T) {
	t.Run("is ok by default", func(t *testing.T) {
		h := start(t, func(w *Worker) { w.HandleLifecycle(Lifecycle{}) })
		if resp := h.call(t, `{"jsonrpc":"2.0","method":"health"}`); string(resp["result"]) != `{"status":"ok"}` {
			t.Errorf("result: %s", resp.Encode())
		}
	})
	t.Run("is what the plugin reports", func(t *testing.T) {
		h := start(t, func(w *Worker) {
			w.HandleLifecycle(Lifecycle{Health: func(context.Context) (Health, error) {
				return Health{Status: "degraded", Message: "slow", Details: map[string]any{"queue": 3}}, nil
			}})
		})
		if resp := h.call(t, `{"jsonrpc":"2.0","method":"health"}`); string(resp["result"]) != `{"status":"degraded","message":"slow","details":{"queue":3}}` {
			t.Errorf("result: %s", resp.Encode())
		}
	})
	t.Run("fails when the plugin's check does", func(t *testing.T) {
		h := start(t, func(w *Worker) {
			w.HandleLifecycle(Lifecycle{Health: func(context.Context) (Health, error) { return Health{}, errors.New("no probe") }})
		})
		if e := h.call(t, `{"jsonrpc":"2.0","method":"health"}`).Err(); e == nil || e.Code != rpc.CodeWorkerError {
			t.Errorf("error: %+v", e)
		}
	})
}

func TestShutdown(t *testing.T) {
	t.Run("answers null, then runs AfterShutdown", func(t *testing.T) {
		stopped := make(chan struct{})
		after := make(chan struct{})
		h := start(t, func(w *Worker) {
			w.HandleLifecycle(Lifecycle{
				Shutdown:      func(context.Context) error { close(stopped); return nil },
				AfterShutdown: func() { close(after) },
			})
		})
		resp := h.call(t, `{"jsonrpc":"2.0","method":"shutdown"}`)
		if string(resp["result"]) != "null" {
			t.Errorf("result: %s", resp.Encode())
		}
		<-stopped
		select {
		case <-after:
		case <-time.After(5 * time.Second):
			t.Fatal("AfterShutdown did not run")
		}
	})
	t.Run("answers without either", func(t *testing.T) {
		h := start(t, func(w *Worker) { w.HandleLifecycle(Lifecycle{}) })
		if resp := h.call(t, `{"jsonrpc":"2.0","method":"shutdown"}`); resp.Err() != nil || string(resp["result"]) != "null" {
			t.Errorf("result: %s", resp.Encode())
		}
	})
	t.Run("fails, and does not run AfterShutdown, when the plugin's shutdown does", func(t *testing.T) {
		ran := make(chan struct{}, 1)
		h := start(t, func(w *Worker) {
			w.HandleLifecycle(Lifecycle{
				Shutdown:      func(context.Context) error { return errors.New("stuck") },
				AfterShutdown: func() { ran <- struct{}{} },
			})
		})
		if e := h.call(t, `{"jsonrpc":"2.0","method":"shutdown"}`).Err(); e == nil || e.Message != "stuck" {
			t.Errorf("error: %+v", e)
		}
		h.call(t, `{"jsonrpc":"2.0","method":"nothing"}`)
		select {
		case <-ran:
			t.Error("AfterShutdown ran after a failed shutdown")
		case <-time.After(50 * time.Millisecond):
		}
	})
}

func TestReplyUnservedAnswersLikeTheSDK(t *testing.T) {
	h := start(t, func(w *Worker) {
		w.Handle("environmentProbe", func(context.Context, json.RawMessage) (any, error) { return "served", nil })
		w.Forward = w.ReplyUnserved
	})
	notImplemented := []string{
		"validateConfig", "handleWebhook", "handleApiRequest", "detectExternalObjects", "resolveExternalObject", "refreshExternalObjects",
		"environmentValidateConfig", "environmentAcquireLease", "environmentResumeLease", "environmentReleaseLease", "environmentDestroyLease",
		"environmentRealizeWorkspace", "environmentExecute", "environmentRunnerIngressEndpoint", "environmentSyncIn", "environmentSyncOut",
		"environmentStartInteractiveSetup", "environmentGetInteractiveSetup", "environmentCaptureTemplate", "environmentCancelInteractiveSetup",
		"environmentDeleteTemplate", "loginPtyOpen", "loginPtyInput", "loginPtyStop", "loginPtyClose",
		"duplexChannelOpen", "duplexChannelWrite", "duplexChannelStop", "duplexChannelClose",
	}
	for _, method := range notImplemented {
		e := h.call(t, `{"jsonrpc":"2.0","method":"`+method+`"}`).Err()
		if e == nil || e.Code != rpc.CodeMethodNotImplemented || e.Message != method+" is not implemented by this plugin" {
			t.Errorf("%s: %+v", method, e)
		}
	}
	for _, method := range []string{"getData", "performAction", "executeTool", "runJob", "onEvent", "configChanged", "health", "shutdown", "initialize", "nonsense"} {
		e := h.call(t, `{"jsonrpc":"2.0","method":"`+method+`"}`).Err()
		if e == nil || e.Code != rpc.CodeMethodNotFound || e.Message != "Unknown method: "+method {
			t.Errorf("%s: %+v", method, e)
		}
	}
	if resp := h.call(t, `{"jsonrpc":"2.0","method":"environmentProbe"}`); string(resp["result"]) != `"served"` {
		t.Errorf("a served method: %s", resp.Encode())
	}
}
