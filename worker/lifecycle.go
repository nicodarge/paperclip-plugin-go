package worker

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/nicodarge/paperclip-plugin-go/rpc"
)

// InitializeParams are the params of the host's initialize request.
type InitializeParams struct {
	Manifest          json.RawMessage `json:"manifest"`
	Config            json.RawMessage `json:"config"`
	InstanceInfo      InstanceInfo    `json:"instanceInfo"`
	APIVersion        int             `json:"apiVersion"`
	DatabaseNamespace string          `json:"databaseNamespace"`
}

// InstanceInfo identifies the Paperclip instance and its version.
type InstanceInfo struct {
	InstanceID  string `json:"instanceId"`
	HostVersion string `json:"hostVersion"`
}

// InitializeResult is what initialize answers.
type InitializeResult struct {
	OK               bool     `json:"ok"`
	SupportedMethods []string `json:"supportedMethods"`
}

// Health is the answer to the host's health request (the SDK's
// PluginHealthDiagnostics); Status is "ok", "degraded" or "error".
type Health struct {
	Status  string         `json:"status"`
	Message string         `json:"message,omitempty"`
	Details map[string]any `json:"details,omitempty"`
}

// Lifecycle is the plugin's side of initialize, health and shutdown, the three
// methods every worker answers. Every member is optional.
type Lifecycle struct {
	// Initialize runs once, before the host sends anything else; an error
	// fails the request and leaves the worker uninitialized.
	Initialize func(ctx context.Context, params InitializeParams) error
	// Health answers the host's health request; without it the worker is
	// reported ok.
	Health func(ctx context.Context) (Health, error)
	// Shutdown runs when the host asks the worker to stop. An error fails the
	// request, and AfterShutdown does not run.
	Shutdown func(ctx context.Context) error
	// AfterShutdown runs once the shutdown response has been written; the SDK
	// exits the process there, so a plugin passes a function that does
	// (os.Exit(0)). It runs on the shutdown handler's goroutine: it must not
	// call Worker.Wait, which would wait for itself.
	AfterShutdown func()
}

// HandleLifecycle registers initialize, health and shutdown as the SDK's
// worker RPC host answers them. initialize answers SupportedMethods, and a
// second one fails with "Worker already initialized". Call it before Serve.
func (w *Worker) HandleLifecycle(l Lifecycle) {
	var mu sync.Mutex
	initialized := false
	w.Handle("initialize", Typed(func(ctx context.Context, params InitializeParams) (InitializeResult, error) {
		mu.Lock()
		defer mu.Unlock()
		if initialized {
			return InitializeResult{}, rpc.Errorf(rpc.CodeWorkerError, "Worker already initialized")
		}
		if l.Initialize != nil {
			if err := l.Initialize(ctx, params); err != nil {
				return InitializeResult{}, err
			}
		}
		initialized = true
		return InitializeResult{OK: true, SupportedMethods: w.SupportedMethods()}, nil
	}))
	w.Handle("health", func(ctx context.Context, _ json.RawMessage) (any, error) {
		if l.Health == nil {
			return Health{Status: "ok"}, nil
		}
		return l.Health(ctx)
	})
	w.Handle("shutdown", func(ctx context.Context, _ json.RawMessage) (any, error) {
		if l.Shutdown != nil {
			if err := l.Shutdown(ctx); err != nil {
				return nil, err
			}
		}
		if l.AfterShutdown != nil {
			AfterReply(ctx, l.AfterShutdown)
		}
		return nil, nil
	})
}

// reportedMethods are the optional methods the SDK's initialize reports when
// the plugin implements them, in the order it lists them. The SDK reports
// health and shutdown only when the plugin defines a hook; HandleLifecycle
// always serves them, so they are always reported here, which is harmless.
// onEvent, runJob, getData, performAction, executeTool and handleWebhook are
// never listed. The SDK reports the two idle-sleep methods only for a plugin
// with an idle-drain hook, and the host calls prepareIdleSleep only when it is
// advertised, so a worker that registers a handler for it opts in.
var reportedMethods = []string{
	"validateConfig",
	"configChanged",
	"health",
	"shutdown",
	"prepareIdleSleep",
	"releaseIdleSleep",
	"handleApiRequest",
	"detectExternalObjects",
	"workspaceOperation",
	"resourceLifecycle",
	"agentLifecycle",
	"routeAiConnection",
	"resolveExternalObject",
	"refreshExternalObjects",
	"environmentValidateConfig",
	"environmentTask",
	"environmentProbe",
	"environmentAcquireLease",
	"environmentResumeLease",
	"environmentReleaseLease",
	"environmentStopLease",
	"environmentDestroyLease",
	"environmentInspectLease",
	"environmentRealizeWorkspace",
	"environmentExecute",
	"environmentRunnerIngressEndpoint",
	"environmentSyncIn",
	"environmentSyncOut",
	"environmentStartInteractiveSetup",
	"environmentGetInteractiveSetup",
	"environmentCaptureTemplate",
	"environmentCancelInteractiveSetup",
	"environmentDeleteTemplate",
	"loginPtyOpen",
	"loginPtyInput",
	"loginPtyStop",
	"loginPtyClose",
	"duplexChannelOpen",
	"duplexChannelWrite",
	"duplexChannelStop",
	"duplexChannelClose",
}

// SupportedMethods lists, in the SDK's order, the optional methods a Handle
// registered, as the supportedMethods member of the initialize result.
func (w *Worker) SupportedMethods() []string {
	out := make([]string, 0, len(reportedMethods))
	for _, m := range reportedMethods {
		if _, ok := w.handlers[m]; ok {
			out = append(out, m)
		}
	}
	return out
}

// unimplementable are the host-to-worker methods the SDK answers
// METHOD_NOT_IMPLEMENTED when the plugin defines no hook for them. The other
// reported methods are never refused (configChanged, health, shutdown and the
// idle-sleep pair are always answered).
var unimplementable = func() map[string]struct{} {
	set := map[string]struct{}{"handleWebhook": {}}
	for _, m := range reportedMethods {
		switch m {
		case "configChanged", "health", "shutdown", "prepareIdleSleep", "releaseIdleSleep":
		default:
			set[m] = struct{}{}
		}
	}
	return set
}()

// ReplyUnserved answers a request no Handler owns, for use as Forward (which,
// when nil, answers every such request METHOD_NOT_FOUND). It answers as the
// SDK's worker RPC host does for a plugin without the hook:
//   - METHOD_NOT_IMPLEMENTED for the methods a plugin may leave out
//     (validateConfig, handleWebhook, handleApiRequest, the environment, login
//     pty, duplex channel, workspace, lifecycle and external object methods);
//   - WORKER_ERROR, "No handler registered for job ...", "No data handler
//     registered for key ...", "No action handler ..." or "No tool handler ...",
//     for runJob, getData, performAction and executeTool;
//   - success for onEvent and shutdown (null), and {"status":"ok"} for health.
//
// It differs from the SDK in three ways: configChanged is answered
// METHOD_NOT_FOUND, because the host restarts the worker when it is answered
// METHOD_NOT_IMPLEMENTED and the SDK would accept the config; shutdown does not
// exit the process; and initialize, prepareIdleSleep and releaseIdleSleep are
// answered METHOD_NOT_FOUND (initialize is HandleLifecycle's, and the host
// calls the idle-sleep method only when it is advertised). Every other method
// is METHOD_NOT_FOUND, "Unknown method: <method>".
func (w *Worker) ReplyUnserved(req rpc.Envelope) {
	method := req.Method()
	var resp rpc.Envelope
	switch method {
	case "onEvent", "shutdown":
		resp, _ = rpc.NewResult(req.ID(), nil)
	case "health":
		resp, _ = rpc.NewResult(req.ID(), Health{Status: "ok"})
	case "runJob":
		resp = rpc.NewError(req.ID(), rpc.Errorf(rpc.CodeWorkerError, "No handler registered for job \"%s\"", jsString(req["params"], "job", "jobKey")))
	case "getData":
		resp = rpc.NewError(req.ID(), rpc.Errorf(rpc.CodeWorkerError, "No data handler registered for key \"%s\"", jsString(req["params"], "key")))
	case "performAction":
		resp = rpc.NewError(req.ID(), rpc.Errorf(rpc.CodeWorkerError, "No action handler registered for key \"%s\"", jsString(req["params"], "key")))
	case "executeTool":
		resp = rpc.NewError(req.ID(), rpc.Errorf(rpc.CodeWorkerError, "No tool handler registered for \"%s\"", jsString(req["params"], "toolName")))
	default:
		failure := rpc.Errorf(rpc.CodeMethodNotFound, "Unknown method: %s", method)
		if _, ok := unimplementable[method]; ok {
			failure = rpc.Errorf(rpc.CodeMethodNotImplemented, "%s is not implemented by this plugin", method)
		}
		resp = rpc.NewError(req.ID(), failure)
	}
	_ = w.host.Send(resp)
}

// jsString reads a string member at path in raw params, as the SDK's template
// strings print it: "undefined" when it is absent or not a string.
func jsString(raw json.RawMessage, path ...string) string {
	for _, key := range path {
		var obj map[string]json.RawMessage
		if json.Unmarshal(raw, &obj) != nil {
			return "undefined"
		}
		raw = obj[key]
	}
	var out string
	if json.Unmarshal(raw, &out) != nil {
		return "undefined"
	}
	return out
}
