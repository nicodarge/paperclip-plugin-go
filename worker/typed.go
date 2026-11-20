package worker

import (
	"context"
	"encoding/json"

	"github.com/nicodarge/paperclip-plugin-go/rpc"
)

// DecodeParams decodes the params of a host request into P; absent or null
// params give P's zero value, and anything that does not fit is an
// INVALID_PARAMS error.
func DecodeParams[P any](raw json.RawMessage) (P, error) {
	var params P
	if len(raw) == 0 || string(raw) == "null" {
		return params, nil
	}
	if err := json.Unmarshal(raw, &params); err != nil {
		return params, rpc.Errorf(rpc.CodeInvalidParams, "Invalid params: %v", err)
	}
	return params, nil
}

// Typed adapts a function of decoded params to a Handler, answering
// INVALID_PARAMS when the params do not decode into P.
func Typed[P, R any](fn func(ctx context.Context, params P) (R, error)) Handler {
	return func(ctx context.Context, raw json.RawMessage) (any, error) {
		params, err := DecodeParams[P](raw)
		if err != nil {
			return nil, err
		}
		return fn(ctx, params)
	}
}
