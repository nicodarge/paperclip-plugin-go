// Package rpc implements the Paperclip plugin wire protocol: JSON-RPC 2.0
// messages, one per line (NDJSON), in both directions over a byte stream.
package rpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// Error codes of the plugin protocol (JSONRPC_ERROR_CODES and
// PLUGIN_RPC_ERROR_CODES in the SDK's protocol.ts).
const (
	CodeParseError           = -32700
	CodeInvalidRequest       = -32600
	CodeMethodNotFound       = -32601
	CodeInvalidParams        = -32602
	CodeInternalError        = -32603
	CodeWorkerUnavailable    = -32000
	CodeCapabilityDenied     = -32001
	CodeWorkerError          = -32002
	CodeTimeout              = -32003
	CodeMethodNotImplemented = -32004
	CodeInvocationScope      = -32005
	CodeCrossTenantConfig    = -32006
)

// Error is a JSON-RPC error object.
type Error struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *Error) Error() string { return e.Message }

// Errorf builds an Error with the given code.
func Errorf(code int, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Kind classifies an envelope the way the SDK's type guards do.
type Kind int

const (
	KindInvalid Kind = iota
	KindResponse
	KindRequest
	KindNotification
)

// Envelope is one JSON-RPC message with every top-level member kept as raw
// JSON, so a forwarded message carries members this code does not know about
// (paperclipInvocation, paperclipInvocationId, whatever a later host adds).
type Envelope map[string]json.RawMessage

var (
	versionJSON = json.RawMessage(`"2.0"`)
	nullJSON    = json.RawMessage(`null`)
)

// Parse decodes one line. It fails on anything that is not a JSON object whose
// "jsonrpc" member is "2.0", like the SDK's parseMessage.
func Parse(line []byte) (Envelope, error) {
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return nil, errors.New("empty message")
	}
	var e Envelope
	if err := json.Unmarshal(line, &e); err != nil || e == nil {
		return nil, errors.New("invalid JSON")
	}
	if !bytes.Equal(e["jsonrpc"], versionJSON) {
		return nil, errors.New(`invalid or missing jsonrpc version (expected "2.0")`)
	}
	return e, nil
}

func isNull(raw json.RawMessage) bool { return len(raw) == 0 || bytes.Equal(raw, nullJSON) }

// Kind applies the SDK's guards in the SDK's order: a response is tested first.
func (e Envelope) Kind() Kind {
	_, hasID := e["id"]
	_, hasResult := e["result"]
	_, hasError := e["error"]
	if hasID && (hasResult || hasError) {
		return KindResponse
	}
	if e.Method() == "" {
		return KindInvalid
	}
	if !hasID {
		return KindNotification
	}
	if isNull(e["id"]) {
		return KindInvalid
	}
	return KindRequest
}

// Method returns the "method" member when it is a non-empty string.
func (e Envelope) Method() string {
	raw, ok := e["method"]
	if !ok || len(raw) == 0 || raw[0] != '"' {
		return ""
	}
	var m string
	if json.Unmarshal(raw, &m) != nil {
		return ""
	}
	return m
}

// ID returns the raw "id" member.
func (e Envelope) ID() json.RawMessage { return e["id"] }

// Err returns the error object of an error response, nil otherwise.
func (e Envelope) Err() *Error {
	raw, ok := e["error"]
	if !ok || isNull(raw) {
		return nil
	}
	var out Error
	if err := json.Unmarshal(raw, &out); err != nil {
		return &Error{Code: CodeInternalError, Message: "malformed error object in response"}
	}
	return &out
}

// With returns a copy with one member replaced.
func (e Envelope) With(key string, value json.RawMessage) Envelope {
	out := make(Envelope, len(e)+1)
	for k, v := range e {
		out[k] = v
	}
	out[key] = value
	return out
}

// Without returns a copy with one member removed.
func (e Envelope) Without(key string) Envelope {
	out := make(Envelope, len(e))
	for k, v := range e {
		if k != key {
			out[k] = v
		}
	}
	return out
}

// Encode serialises the envelope as one newline-terminated line. Member values
// are written byte for byte as they were received.
func (e Envelope) Encode() []byte {
	keys := make([]string, 0, len(e))
	size := 2
	for k, v := range e {
		keys = append(keys, k)
		size += len(k) + len(v) + 4
	}
	sort.Strings(keys)
	buf := bytes.NewBuffer(make([]byte, 0, size+1))
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		name, _ := json.Marshal(k)
		buf.Write(name)
		buf.WriteByte(':')
		if v := e[k]; len(v) == 0 {
			buf.Write(nullJSON)
		} else {
			buf.Write(v)
		}
	}
	buf.WriteString("}\n")
	return buf.Bytes()
}

// NewRequest builds a request or, with a nil id, a notification.
func NewRequest(id json.RawMessage, method string, params any) (Envelope, error) {
	e := Envelope{"jsonrpc": versionJSON}
	name, _ := json.Marshal(method)
	e["method"] = name
	if id != nil {
		e["id"] = id
	}
	if params != nil {
		raw, err := marshal(params)
		if err != nil {
			return nil, err
		}
		e["params"] = raw
	}
	return e, nil
}

// NewResult builds a success response; a nil result is sent as null.
func NewResult(id json.RawMessage, result any) (Envelope, error) {
	raw := nullJSON
	if result != nil {
		var err error
		if raw, err = marshal(result); err != nil {
			return nil, err
		}
	}
	return Envelope{"jsonrpc": versionJSON, "id": orNull(id), "result": raw}, nil
}

// NewError builds an error response.
func NewError(id json.RawMessage, e *Error) Envelope {
	raw, _ := marshal(e)
	return Envelope{"jsonrpc": versionJSON, "id": orNull(id), "error": raw}
}

func orNull(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nullJSON
	}
	return raw
}

// marshal leaves <, > and & unescaped, as JSON.stringify does. A
// json.RawMessage is compacted onto one line, since a line break inside it
// would split the NDJSON frame, and an invalid one is an error.
func marshal(v any) (json.RawMessage, error) {
	if raw, ok := v.(json.RawMessage); ok {
		if len(raw) == 0 {
			return nullJSON, nil
		}
		var compact bytes.Buffer
		if err := json.Compact(&compact, raw); err != nil {
			return nil, fmt.Errorf("invalid raw JSON: %w", err)
		}
		return compact.Bytes(), nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
