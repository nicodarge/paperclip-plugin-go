package worker

import (
	"context"
	"encoding/json"
	"testing"
)

func TestEventsExecutionAndFilteredIssueCallsSendTheParametersTheSDKSends(t *testing.T) {
	h := start(t, func(w *Worker) {
		w.Handle("runJob", func(ctx context.Context, _ json.RawMessage) (any, error) {
			if err := w.Events().Subscribe(ctx, "agent.run.cancelled", nil); err != nil {
				return nil, err
			}
			if err := w.Events().Subscribe(ctx, "issue.*", map[string]any{"projectId": "p1"}); err != nil {
				return nil, err
			}
			w.Execution().Log(ctx, "stdout", "")
			w.Execution().Log(ctx, "stderr", "boom\n")
			var minimal, full []map[string]string
			if err := w.Issues().ListFiltered(ctx, IssueQuery{CompanyID: "c1", Limit: 50}, &minimal); err != nil {
				return nil, err
			}
			if err := w.Issues().ListFiltered(ctx, IssueQuery{CompanyID: "c1"}, &minimal); err != nil {
				return nil, err
			}
			err := w.Issues().ListFiltered(ctx, IssueQuery{
				CompanyID: "c1", ProjectID: "p1", AssigneeAgentID: "a1", OriginKind: "plugin:x", OriginKindPrefix: "plugin:",
				OriginID: "o1", Status: "todo", IncludePluginOperations: true, Limit: 10, Offset: 20,
			}, &full)
			return map[string]any{"minimal": minimal, "full": full}, err
		})
	})
	h.answers["events.subscribe"] = `null`
	h.answers["issues.list"] = `[{"id":"i1"}]`

	resp := h.call(t, `{"jsonrpc":"2.0","method":"runJob","params":{},"paperclipInvocation":{"id":"inv-5"}}`)
	if got := string(resp["result"]); got != `{"full":[{"id":"i1"}],"minimal":[{"id":"i1"}]}` {
		t.Fatalf("result: %s (%v)", got, resp.Err())
	}
	want := []string{
		`events.subscribe {"eventPattern":"agent.run.cancelled","filter":null}`,
		`events.subscribe {"eventPattern":"issue.*","filter":{"projectId":"p1"}}`,
		`issues.list {"companyId":"c1","limit":50}`,
		`issues.list {"companyId":"c1"}`,
		`issues.list {"companyId":"c1","projectId":"p1","assigneeAgentId":"a1","originKind":"plugin:x","originKindPrefix":"plugin:","originId":"o1","status":"todo","includePluginOperations":true,"limit":10,"offset":20}`,
	}
	if len(h.calls) != len(want) {
		t.Fatalf("%d host calls, want %d", len(h.calls), len(want))
	}
	for i, c := range h.calls {
		if got := c.Method() + " " + string(c["params"]); got != want[i] {
			t.Errorf("call %d: %s, want %s", i, got, want[i])
		}
		if string(c["paperclipInvocationId"]) != `"inv-5"` {
			t.Errorf("call %d did not echo the invocation: %s", i, c.Encode())
		}
	}

	// The empty chunk was dropped: the first execute.log is the stderr one.
	n := h.nextNotification(t)
	if n.Method() != "execute.log" || string(n["params"]) != `{"stream":"stderr","chunk":"boom\n"}` || string(n["paperclipInvocationId"]) != `"inv-5"` {
		t.Errorf("execute.log: %s", n.Encode())
	}
}
