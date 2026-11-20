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
	// (os.Exit(0)).
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
// the plugin implements them, in the order it lists them; health and shutdown
// are reported too. Others (onEvent, runJob, getData, performAction,
// executeTool, handleWebhook) are never listed.
var reportedMethods = []string{
	"validateConfig",
	"configChanged",
	"health",
	"shutdown",
	"handleApiRequest",
	"detectExternalObjects",
	"resolveExternalObject",
	"refreshExternalObjects",
	"environmentValidateConfig",
	"environmentProbe",
	"environmentAcquireLease",
	"environmentResumeLease",
	"environmentReleaseLease",
	"environmentDestroyLease",
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
// METHOD_NOT_IMPLEMENTED when the plugin defines no hook for them: it answers
// every other method it does not know METHOD_NOT_FOUND.
var unimplementable = func() map[string]struct{} {
	set := map[string]struct{}{"handleWebhook": {}}
	for _, m := range reportedMethods {
		switch m {
		case "configChanged", "health", "shutdown":
		default:
			set[m] = struct{}{}
		}
	}
	return set
}()

// ReplyUnserved answers a request no Handler owns the way the SDK does:
// METHOD_NOT_IMPLEMENTED for a host method the plugin may leave out,
// METHOD_NOT_FOUND for any other. Set it as Forward to get that behaviour
// where Forward would otherwise be nil, which answers every such request
// METHOD_NOT_FOUND.
func (w *Worker) ReplyUnserved(req rpc.Envelope) {
	method := req.Method()
	failure := rpc.Errorf(rpc.CodeMethodNotFound, "Unknown method: %s", method)
	if _, ok := unimplementable[method]; ok {
		failure = rpc.Errorf(rpc.CodeMethodNotImplemented, "%s is not implemented by this plugin", method)
	}
	_ = w.host.Send(rpc.NewError(req.ID(), failure))
}
