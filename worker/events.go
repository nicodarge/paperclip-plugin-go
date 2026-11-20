package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"runtime/debug"

	"github.com/nicodarge/paperclip-plugin-go/rpc"
)

const onEventMethod = "onEvent"

// Event is a domain event the host pushes (the SDK's PluginEvent).
type Event struct {
	EventID    string          `json:"eventId"`
	EventType  string          `json:"eventType"`
	OccurredAt string          `json:"occurredAt"`
	ActorID    string          `json:"actorId"`
	ActorType  string          `json:"actorType"`
	EntityID   string          `json:"entityId"`
	EntityType string          `json:"entityType"`
	CompanyID  string          `json:"companyId"`
	Payload    json.RawMessage `json:"payload"`
}

// EventHandler receives the events the worker subscribed to with
// Events().Subscribe. A returned error is logged to the host, never sent back:
// the SDK answers every onEvent request with success.
type EventHandler func(ctx context.Context, e Event) error

type eventParams struct {
	Event *Event `json:"event"`
}

// HandleEvent registers the one receiver of every event, whether the host
// pushes it as an onEvent request or as an onEvent notification (which the
// host scopes with paperclipInvocation, and the receiver's ctx carries). It
// owns both paths: it registers the onEvent handler itself (do not also
// Handle onEvent), and no onEvent notification reaches ForwardNotification. A notification is handled on a goroutine of its own,
// like a request; one whose params do not decode is logged to the host and
// dropped. Without HandleEvent an onEvent request goes to Handle, and a
// notification to ForwardNotification. Call it before Serve.
func (w *Worker) HandleEvent(h EventHandler) {
	w.events = h
	w.Handle(onEventMethod, Typed(func(ctx context.Context, params eventParams) (any, error) {
		if params.Event == nil {
			return nil, rpc.Errorf(rpc.CodeInvalidParams, "Invalid params: no event")
		}
		w.deliver(ctx, *params.Event)
		return nil, nil
	}))
}

// eventNotification runs on the reading goroutine, which must not block.
func (w *Worker) eventNotification(n rpc.Envelope) {
	if raw := n["params"]; len(raw) == 0 || string(raw) == "null" {
		return
	}
	ctx := context.Background()
	if id := InvocationIDOf(n); id != "" {
		ctx = WithInvocation(ctx, id)
	}
	params, err := DecodeParams[eventParams](n["params"])
	if err == nil && params.Event == nil {
		err = rpc.Errorf(rpc.CodeInvalidParams, "Invalid params: no event")
	}
	if err != nil {
		w.Logger().Error(ctx, fmt.Sprintf("Failed to handle event notification: %v", err), nil)
		return
	}
	event := *params.Event
	w.active.Add(1)
	go func() {
		defer w.active.Done()
		w.deliver(ctx, event)
	}()
}

func (w *Worker) deliver(ctx context.Context, event Event) {
	err := func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				_, _ = fmt.Fprintf(w.Stderr, "paperclip-plugin-go: panic in the %s event handler: %v\n%s", event.EventType, r, debug.Stack())
				err = fmt.Errorf("panic: %v", r)
			}
		}()
		return w.events(ctx, event)
	}()
	if err != nil {
		w.Logger().Error(ctx, fmt.Sprintf("Event handler failed: %v", err), map[string]any{"eventType": event.EventType})
	}
}
