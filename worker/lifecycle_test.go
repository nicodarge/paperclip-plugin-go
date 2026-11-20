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

// sdkReportedMethods is the order of the pushes of handleInitialize in
// packages/plugins/sdk/src/worker-rpc-host.ts of Paperclip v2026.1117.0.
var sdkReportedMethods = []string{
	"validateConfig", "configChanged", "health", "shutdown", "prepareIdleSleep", "releaseIdleSleep", "handleApiRequest",
	"detectExternalObjects", "workspaceOperation", "resourceLifecycle", "agentLifecycle", "routeAiConnection",
	"resolveExternalObject", "refreshExternalObjects", "environmentValidateConfig", "environmentTask", "environmentProbe",
	"environmentAcquireLease", "environmentResumeLease", "environmentReleaseLease", "environmentStopLease",
	"environmentDestroyLease", "environmentInspectLease", "environmentRealizeWorkspace", "environmentExecute",
	"environmentRunnerIngressEndpoint", "environmentSyncIn", "environmentSyncOut", "environmentStartInteractiveSetup",
	"environmentGetInteractiveSetup", "environmentCaptureTemplate", "environmentCancelInteractiveSetup",
	"environmentDeleteTemplate", "loginPtyOpen", "loginPtyInput", "loginPtyStop", "loginPtyClose",
	"duplexChannelOpen", "duplexChannelWrite", "duplexChannelStop", "duplexChannelClose",
}

func TestEveryMethodTheSDKReportsIsReportedInItsOrder(t *testing.T) {
	registered := slices.Clone(sdkReportedMethods)
	slices.Reverse(registered)
	w := NewWorker(nil, nil)
	for _, m := range registered {
		w.Handle(m, func(context.Context, json.RawMessage) (any, error) { return nil, nil })
	}
	if got := w.SupportedMethods(); !slices.Equal(got, sdkReportedMethods) {
		t.Errorf("SupportedMethods() = %v, want %v", got, sdkReportedMethods)
	}
}

func TestReplyUnservedAnswersAsTheSDKDoesWithoutTheHook(t *testing.T) {
	h := start(t, func(w *Worker) {
		w.Handle("environmentProbe", func(context.Context, json.RawMessage) (any, error) { return "served", nil })
		w.Forward = w.ReplyUnserved
	})
	notImplemented := []string{
		"validateConfig", "handleWebhook", "handleApiRequest", "detectExternalObjects", "workspaceOperation", "resourceLifecycle",
		"agentLifecycle", "routeAiConnection", "resolveExternalObject", "refreshExternalObjects", "environmentValidateConfig",
		"environmentTask", "environmentAcquireLease", "environmentResumeLease", "environmentReleaseLease", "environmentStopLease",
		"environmentDestroyLease", "environmentInspectLease", "environmentRealizeWorkspace", "environmentExecute",
		"environmentRunnerIngressEndpoint", "environmentSyncIn", "environmentSyncOut", "environmentStartInteractiveSetup",
		"environmentGetInteractiveSetup", "environmentCaptureTemplate", "environmentCancelInteractiveSetup", "environmentDeleteTemplate",
		"loginPtyOpen", "loginPtyInput", "loginPtyStop", "loginPtyClose", "duplexChannelOpen", "duplexChannelWrite",
		"duplexChannelStop", "duplexChannelClose",
	}
	for _, method := range notImplemented {
		e := h.call(t, `{"jsonrpc":"2.0","method":"`+method+`"}`).Err()
		if e == nil || e.Code != rpc.CodeMethodNotImplemented || e.Message != method+" is not implemented by this plugin" {
			t.Errorf("%s: %+v", method, e)
		}
	}

	workerErrors := map[string]string{
		`{"jsonrpc":"2.0","method":"getData","params":{"key":"k1"}}`:           `No data handler registered for key "k1"`,
		`{"jsonrpc":"2.0","method":"getData"}`:                                 `No data handler registered for key "undefined"`,
		`{"jsonrpc":"2.0","method":"performAction","params":{"key":"a1"}}`:     `No action handler registered for key "a1"`,
		`{"jsonrpc":"2.0","method":"executeTool","params":{"toolName":"t1"}}`:  `No tool handler registered for "t1"`,
		`{"jsonrpc":"2.0","method":"runJob","params":{"job":{"jobKey":"j1"}}}`: `No handler registered for job "j1"`,
		`{"jsonrpc":"2.0","method":"runJob","params":{"job":"j1"}}`:            `No handler registered for job "undefined"`,
		`{"jsonrpc":"2.0","method":"runJob","params":{"job":{"jobKey":5}}}`:    `No handler registered for job "undefined"`,
	}
	for line, message := range workerErrors {
		if e := h.call(t, line).Err(); e == nil || e.Code != rpc.CodeWorkerError || e.Message != message {
			t.Errorf("%s: %+v, want WORKER_ERROR %q", line, e, message)
		}
	}

	for method, want := range map[string]string{"onEvent": "null", "shutdown": "null", "health": `{"status":"ok"}`} {
		if resp := h.call(t, `{"jsonrpc":"2.0","method":"`+method+`"}`); resp.Err() != nil || string(resp["result"]) != want {
			t.Errorf("%s: %s, want %s", method, resp.Encode(), want)
		}
	}

	// configChanged is the documented exception: METHOD_NOT_IMPLEMENTED would make the host restart the worker.
	for _, method := range []string{"configChanged", "initialize", "prepareIdleSleep", "releaseIdleSleep", "nonsense"} {
		e := h.call(t, `{"jsonrpc":"2.0","method":"`+method+`"}`).Err()
		if e == nil || e.Code != rpc.CodeMethodNotFound || e.Message != "Unknown method: "+method {
			t.Errorf("%s: %+v", method, e)
		}
	}
	if resp := h.call(t, `{"jsonrpc":"2.0","method":"environmentProbe"}`); string(resp["result"]) != `"served"` {
		t.Errorf("a served method: %s", resp.Encode())
	}
}
