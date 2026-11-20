package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/nicodarge/paperclip-plugin-go/rpc"
)

func TestTypedDecodesParamsAndRefusesWhatDoesNotFit(t *testing.T) {
	type params struct {
		N int `json:"n"`
	}
	h := Typed(func(_ context.Context, p params) (map[string]int, error) { return map[string]int{"n": p.N}, nil })
	for raw, want := range map[string]string{`{"n":3}`: `{"n":3}`, `{"n":3,"extra":true}`: `{"n":3}`, ``: `{"n":0}`, `null`: `{"n":0}`} {
		got, err := h(context.Background(), json.RawMessage(raw))
		if err != nil {
			t.Fatalf("%q: %v", raw, err)
		}
		if encoded, _ := json.Marshal(got); string(encoded) != want {
			t.Errorf("%q: %s, want %s", raw, encoded, want)
		}
	}
	for _, raw := range []string{`{"n":"3"}`, `[1]`, `"text"`, `{`} {
		_, err := h(context.Background(), json.RawMessage(raw))
		var rpcErr *rpc.Error
		if !errors.As(err, &rpcErr) || rpcErr.Code != rpc.CodeInvalidParams {
			t.Errorf("%q: %v, want INVALID_PARAMS", raw, err)
		}
	}
}
