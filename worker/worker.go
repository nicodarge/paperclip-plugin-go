// Package worker is the worker side of the Paperclip plugin protocol: what
// @paperclipai/plugin-sdk's startWorkerRpcHost does for a TypeScript plugin.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nicodarge/paperclip-plugin-go/rpc"
)

// DefaultCallTimeout bounds a worker-to-host call whose context has no
// deadline, like the SDK's DEFAULT_RPC_TIMEOUT_MS.
const DefaultCallTimeout = 30 * time.Second

const runJobMethod = "runJob"

// Handler serves one host-to-worker method. A returned *rpc.Error is sent as
// it is; any other error becomes a WORKER_ERROR carrying its message.
type Handler func(ctx context.Context, params json.RawMessage) (result any, err error)

// Worker serves the host on one Conn.
type Worker struct {
	host *rpc.Conn

	// Handlers are the methods implemented in Go. Each call runs on its own
	// goroutine.
	handlers map[string]Handler
	// jobs are the scheduled jobs implemented in Go, by job key.
	jobs map[string]Handler
	// Forward receives every request no Handler owns, on the reading
	// goroutine and in arrival order; it must send the response itself.
	// Without it such a request is answered METHOD_NOT_FOUND. It must not
	// block: a synchronous CallHost or Conn.Call from it deadlocks, since the
	// response can only be read by the goroutine it is blocking.
	Forward func(req rpc.Envelope)
	// ForwardNotification receives every notification, same ordering and
	// same restriction.
	ForwardNotification func(n rpc.Envelope)
	// Stderr receives handler panics.
	Stderr io.Writer

	// events serves onEvent, in both of its forms, once HandleEvent set it.
	events EventHandler

	active sync.WaitGroup
}

// NewWorker serves the host over r (the host's requests) and w.
func NewWorker(r io.Reader, w io.Writer) *Worker {
	return &Worker{host: rpc.NewConn(r, w), handlers: map[string]Handler{}, jobs: map[string]Handler{}, Stderr: os.Stderr}
}

// Host exposes the connection, for a router that forwards on it.
func (w *Worker) Host() *rpc.Conn { return w.host }

// Handle registers the Go implementation of a method. Call it before Serve.
func (w *Worker) Handle(method string, h Handler) { w.handlers[method] = h }

// HandleJob registers the Go implementation of one scheduled job: a runJob
// call for any other job key goes to Forward. Call it before Serve.
func (w *Worker) HandleJob(jobKey string, h Handler) { w.jobs[jobKey] = h }

func (w *Worker) handlerOf(req rpc.Envelope) (Handler, bool) {
	if h, ok := w.handlers[req.Method()]; ok {
		return h, true
	}
	if req.Method() != runJobMethod || len(w.jobs) == 0 {
		return nil, false
	}
	var params struct {
		Job struct {
			JobKey string `json:"jobKey"`
		} `json:"job"`
	}
	if json.Unmarshal(req["params"], &params) != nil {
		return nil, false
	}
	h, ok := w.jobs[params.Job.JobKey]
	return h, ok
}

// Serve reads the host until it closes the stream, then settles the calls to
// the host still waiting with a WORKER_UNAVAILABLE error. Call it once.
func (w *Worker) Serve() error {
	return w.host.Serve(rpc.Handlers{
		Request: func(req rpc.Envelope) {
			if h, ok := w.handlerOf(req); ok {
				w.active.Add(1)
				go func() {
					defer w.active.Done()
					w.serve(h, req)
				}()
				return
			}
			if w.Forward != nil {
				w.Forward(req)
				return
			}
			_ = w.host.Send(rpc.NewError(req.ID(), rpc.Errorf(rpc.CodeMethodNotFound, "Unknown method: %s", req.Method())))
		},
		Notification: func(n rpc.Envelope) {
			if w.events != nil && n.Method() == onEventMethod {
				w.eventNotification(n)
				return
			}
			if w.ForwardNotification != nil {
				w.ForwardNotification(n)
			}
		},
		Unparseable: func(_ []byte, err error) {
			_ = w.host.Send(rpc.NewError(nil, rpc.Errorf(rpc.CodeParseError, "Parse error: %v", err)))
		},
	})
}

// Wait blocks until every running Handler has returned. Call it after Serve
// has returned: a request read while Wait runs can start a Handler it misses.
func (w *Worker) Wait() { w.active.Wait() }

func (w *Worker) serve(h Handler, req rpc.Envelope) {
	call := &call{request: req}
	ctx := context.WithValue(context.Background(), callKey{}, call)
	if id := InvocationIDOf(req); id != "" {
		ctx = WithInvocation(ctx, id)
	}
	result, err := w.run(ctx, h, req)
	var resp rpc.Envelope
	if err == nil {
		resp, err = rpc.NewResult(req.ID(), result)
	}
	if err != nil {
		var rpcErr *rpc.Error
		if !errors.As(err, &rpcErr) {
			rpcErr = &rpc.Error{Code: rpc.CodeWorkerError, Message: err.Error()}
		}
		resp = rpc.NewError(req.ID(), rpcErr)
	}
	_ = w.host.Send(resp)
	for _, fn := range call.afterReply {
		fn()
	}
}

func (w *Worker) run(ctx context.Context, h Handler, req rpc.Envelope) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			_, _ = fmt.Fprintf(w.Stderr, "paperclip-plugin-go: panic in %s handler: %v\n%s", req.Method(), r, debug.Stack())
			err = fmt.Errorf("panic in %s handler: %v", req.Method(), r)
		}
	}()
	return h(ctx, req["params"])
}

type callKey struct{}

type call struct {
	request    rpc.Envelope
	afterReply []func()
}

// Request returns the envelope the running Handler is serving.
func Request(ctx context.Context) rpc.Envelope {
	if c, ok := ctx.Value(callKey{}).(*call); ok {
		return c.request
	}
	return nil
}

// AfterReply runs fn once the running Handler's response has been written.
// Only the Handler's own goroutine may call it.
func AfterReply(ctx context.Context, fn func()) {
	if c, ok := ctx.Value(callKey{}).(*call); ok {
		c.afterReply = append(c.afterReply, fn)
	}
}

type invocationKey struct{}

// WithInvocation scopes ctx to a host invocation. The host resolves the
// company of a worker-to-host call from the invocation id the call echoes, so a
// call made outside the invocation's context is refused or mis-scoped.
func WithInvocation(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, invocationKey{}, id)
}

// InvocationID returns the invocation ctx is scoped to, or "".
func InvocationID(ctx context.Context) string {
	id, _ := ctx.Value(invocationKey{}).(string)
	return id
}

// InvocationIDOf returns the id of the invocation a host message carries in
// its paperclipInvocation member, or "". A request's Handler gets it in its
// context already; a notification is scoped with WithInvocation.
func InvocationIDOf(e rpc.Envelope) string {
	var inv struct {
		ID string `json:"id"`
	}
	if raw, ok := e["paperclipInvocation"]; ok {
		_ = json.Unmarshal(raw, &inv)
	}
	return inv.ID
}

// CallHost sends a worker-to-host request and decodes its result into out
// (ignored when nil). It echoes ctx's invocation id.
func (w *Worker) CallHost(ctx context.Context, method string, params any, out any) error {
	req, err := w.outbound(ctx, method, params)
	if err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultCallTimeout)
		defer cancel()
	}
	resp, err := w.host.Call(ctx, req)
	if errors.Is(err, context.DeadlineExceeded) {
		return rpc.Errorf(rpc.CodeTimeout, "Worker→host call %q timed out", method)
	}
	if err != nil {
		return err
	}
	if rpcErr := resp.Err(); rpcErr != nil {
		return rpcErr
	}
	if out == nil {
		return nil
	}
	result := resp["result"]
	if len(result) == 0 {
		result = json.RawMessage("null")
	}
	return json.Unmarshal(result, out)
}

// NotifyHost sends a fire-and-forget notification, echoing ctx's invocation id.
func (w *Worker) NotifyHost(ctx context.Context, method string, params any) {
	if n, err := w.outbound(ctx, method, params); err == nil {
		_ = w.host.Send(n)
	}
}

func (w *Worker) outbound(ctx context.Context, method string, params any) (rpc.Envelope, error) {
	e, err := rpc.NewRequest(nil, method, params)
	if err != nil {
		return nil, err
	}
	if id := InvocationID(ctx); id != "" {
		raw, _ := json.Marshal(id)
		e["paperclipInvocationId"] = raw
	}
	return e, nil
}
