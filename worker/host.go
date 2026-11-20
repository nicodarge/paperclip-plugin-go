package worker

import (
	"context"
	"encoding/json"
)

// ScopeKey addresses one plugin state entry (the SDK's ScopeKey).
type ScopeKey struct {
	ScopeKind string `json:"scopeKind"`
	ScopeID   string `json:"scopeId,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	StateKey  string `json:"stateKey"`
}

// State is ctx.state.
type State struct{ w *Worker }

// State returns the plugin state client.
func (w *Worker) State() State { return State{w} }

// Get decodes the stored value into out and reports whether one exists.
func (s State) Get(ctx context.Context, key ScopeKey, out any) (found bool, err error) {
	var raw json.RawMessage
	if err := s.w.CallHost(ctx, "state.get", key, &raw); err != nil {
		return false, err
	}
	if len(raw) == 0 || string(raw) == "null" {
		return false, nil
	}
	return true, json.Unmarshal(raw, out)
}

// Set upserts a JSON-serialisable value.
func (s State) Set(ctx context.Context, key ScopeKey, value any) error {
	params := struct {
		ScopeKey
		Value any `json:"value"`
	}{key, value}
	return s.w.CallHost(ctx, "state.set", params, nil)
}

// Delete removes an entry; a missing one is not an error.
func (s State) Delete(ctx context.Context, key ScopeKey) error {
	return s.w.CallHost(ctx, "state.delete", key, nil)
}

// Config is ctx.config.
type Config struct{ w *Worker }

// Config returns the plugin config client.
func (w *Worker) Config() Config { return Config{w} }

// Get decodes the plugin config of a company into out. The host refuses the
// call when neither companyID nor ctx's invocation resolves a company.
func (c Config) Get(ctx context.Context, companyID string, out any) error {
	params := struct {
		CompanyID string `json:"companyId,omitempty"`
	}{companyID}
	return c.w.CallHost(ctx, "config.get", params, out)
}

// Secrets is ctx.secrets.
type Secrets struct{ w *Worker }

// Secrets returns the secret resolver.
func (w *Worker) Secrets() Secrets { return Secrets{w} }

// SecretOptions are the optional members of a secrets.resolve call.
type SecretOptions struct {
	CompanyID  string
	ConfigPath string
}

// Resolve returns the value behind a secret reference. Callers must not cache
// it: a rotated secret has to take effect on the next call.
func (s Secrets) Resolve(ctx context.Context, secretRef string, opts SecretOptions) (string, error) {
	params := struct {
		SecretRef  string `json:"secretRef"`
		CompanyID  string `json:"companyId,omitempty"`
		ConfigPath string `json:"configPath,omitempty"`
	}{secretRef, opts.CompanyID, opts.ConfigPath}
	var value string
	err := s.w.CallHost(ctx, "secrets.resolve", params, &value)
	return value, err
}

// Agents is ctx.agents.
type Agents struct{ w *Worker }

// Agents returns the agent client.
func (w *Worker) Agents() Agents { return Agents{w} }

// List decodes up to limit agents of a company into out.
func (a Agents) List(ctx context.Context, companyID string, limit int, out any) error {
	params := struct {
		CompanyID string `json:"companyId"`
		Limit     int    `json:"limit"`
	}{companyID, limit}
	return a.w.CallHost(ctx, "agents.list", params, out)
}

// ListPage decodes one page of the agents of a company into out, starting at
// offset; the offset is always sent, as ctx.agents.list sends it.
func (a Agents) ListPage(ctx context.Context, companyID string, limit, offset int, out any) error {
	params := struct {
		CompanyID string `json:"companyId"`
		Limit     int    `json:"limit"`
		Offset    int    `json:"offset"`
	}{companyID, limit, offset}
	return a.w.CallHost(ctx, "agents.list", params, out)
}

// Issues is ctx.issues.
type Issues struct{ w *Worker }

// Issues returns the issue client.
func (w *Worker) Issues() Issues { return Issues{w} }

// List decodes one page of the issues of a company into out.
func (i Issues) List(ctx context.Context, companyID string, limit, offset int, out any) error {
	params := struct {
		CompanyID string `json:"companyId"`
		Limit     int    `json:"limit"`
		Offset    int    `json:"offset"`
	}{companyID, limit, offset}
	return i.w.CallHost(ctx, "issues.list", params, out)
}

// Get decodes one issue into out; an unknown issue decodes as JSON null.
func (i Issues) Get(ctx context.Context, companyID, issueID string, out any) error {
	params := struct {
		IssueID   string `json:"issueId"`
		CompanyID string `json:"companyId"`
	}{issueID, companyID}
	return i.w.CallHost(ctx, "issues.get", params, out)
}

// ListComments decodes every comment of an issue into out.
func (i Issues) ListComments(ctx context.Context, companyID, issueID string, out any) error {
	params := struct {
		IssueID   string `json:"issueId"`
		CompanyID string `json:"companyId"`
	}{issueID, companyID}
	return i.w.CallHost(ctx, "issues.listComments", params, out)
}

// CreateComment adds a comment to an issue, written by the plugin itself.
func (i Issues) CreateComment(ctx context.Context, companyID, issueID, body string) error {
	params := struct {
		IssueID   string `json:"issueId"`
		Body      string `json:"body"`
		CompanyID string `json:"companyId"`
	}{issueID, body, companyID}
	return i.w.CallHost(ctx, "issues.createComment", params, nil)
}

// Update patches an issue; patch is a JSON object holding only the members to
// change.
func (i Issues) Update(ctx context.Context, companyID, issueID string, patch any) error {
	params := struct {
		IssueID   string `json:"issueId"`
		Patch     any    `json:"patch"`
		CompanyID string `json:"companyId"`
	}{issueID, patch, companyID}
	return i.w.CallHost(ctx, "issues.update", params, nil)
}

// DB is ctx.db: read-only SQL on the tables the manifest lists
// (database.coreReadTables), each written as public.<table>.
type DB struct{ w *Worker }

// DB returns the database client.
func (w *Worker) DB() DB { return DB{w} }

// Query runs one SELECT and decodes its rows into out.
func (d DB) Query(ctx context.Context, sql string, args []any, out any) error {
	params := struct {
		SQL    string `json:"sql"`
		Params []any  `json:"params"`
	}{sql, args}
	return d.w.CallHost(ctx, "db.query", params, out)
}

// Logger is ctx.logger: each call is a `log` notification to the host.
type Logger struct{ w *Worker }

// Logger returns the host logger.
func (w *Worker) Logger() Logger { return Logger{w} }

func (l Logger) log(ctx context.Context, level, message string, meta map[string]any) {
	params := struct {
		Level   string         `json:"level"`
		Message string         `json:"message"`
		Meta    map[string]any `json:"meta,omitempty"`
	}{level, message, meta}
	l.w.NotifyHost(ctx, "log", params)
}

// Debug sends a debug log notification.
func (l Logger) Debug(ctx context.Context, message string, meta map[string]any) {
	l.log(ctx, "debug", message, meta)
}

// Info sends a info log notification.
func (l Logger) Info(ctx context.Context, message string, meta map[string]any) {
	l.log(ctx, "info", message, meta)
}

// Warn sends a warn log notification.
func (l Logger) Warn(ctx context.Context, message string, meta map[string]any) {
	l.log(ctx, "warn", message, meta)
}

// Error sends a error log notification.
func (l Logger) Error(ctx context.Context, message string, meta map[string]any) {
	l.log(ctx, "error", message, meta)
}
