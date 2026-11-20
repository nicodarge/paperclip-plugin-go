# paperclip-plugin-go

A Go implementation of the worker side of the [Paperclip](https://github.com/paperclipai/paperclip) plugin protocol. Paperclip starts a plugin worker as a child process and speaks newline-delimited JSON-RPC 2.0 with it over stdin and stdout; this module is the transport and routing of that worker, in Go, with the standard library only. It is not a port of `startWorkerRpcHost` of `@paperclipai/plugin-sdk`: the worker routes the host's requests to registered handlers or forwards them, and the plugin implements its methods itself, with helpers for the lifecycle (`initialize`, `health`, `shutdown`) and for events.

Paperclip itself publishes no Go SDK.

## Layout

| Path | Content |
| --- | --- |
| `rpc/` | The wire protocol: `Envelope` (a JSON-RPC 2.0 message that keeps every top-level member as raw JSON, so a forwarded message carries members this code does not know), the protocol's error codes, and `Conn`, one NDJSON stream with its own outbound request ids |
| `worker/` | `Worker`: serves the host's requests with registered handlers, forwards the rest, and calls back into the host (state, config, secrets, agents, issues, events, execution log, database, logger) |
| `tools/<name>/` | One Go module per CI tool (`golangci-lint`, `svu`), pinned in its `go.mod` and verified by its `go.sum` |
| `scripts/install-tools.sh` | Builds the pinned CI tools from `tools/` |

## Protocol version

Written against the Paperclip release `v2026.1117.0` (commit `c0f132898d2a2677312d8c72096187b27a3d9a38`), the host deployed on 2026-11-20: the error codes, method names and message shapes follow `packages/plugins/sdk/src/` of that commit, and the helpers for events, the execution log, the lifecycle and `issues.list` were checked against its `worker-rpc-host.ts` and `server/src/services/`. The conformance run that drives the real Paperclip worker manager against a plugin lives in `gro-bridge`, not here; this repository's tests exercise the worker against an in-process host.

## Usage

```go
import (
    "context"
    "encoding/json"
    "os"

    "github.com/nicodarge/paperclip-plugin-go/worker"
)

func main() {
    w := worker.NewWorker(os.Stdin, os.Stdout)
    w.HandleLifecycle(worker.Lifecycle{
        Initialize: func(ctx context.Context, _ worker.InitializeParams) error {
            return w.Events().Subscribe(ctx, "agent.run.cancelled", nil)
        },
        AfterShutdown: func() { os.Exit(0) },
    })
    w.HandleEvent(func(ctx context.Context, e worker.Event) error {
        w.Logger().Info(ctx, "event "+e.EventType, nil)
        return nil
    })
    w.HandleJob("sync", func(ctx context.Context, _ json.RawMessage) (any, error) {
        var n int
        _, err := w.State().Get(ctx, worker.ScopeKey{ScopeKind: "instance", StateKey: "count"}, &n)
        return n, err
    })
    if err := w.Serve(); err != nil {
        os.Exit(1)
    }
    w.Wait()
}
```

- `HandleLifecycle` answers `initialize` (with the `supportedMethods` the registered handlers add up to, `Worker.SupportedMethods`, and an error on a second call), `health` (ok unless `Lifecycle.Health` says otherwise) and `shutdown`; `Lifecycle.AfterShutdown` runs once the shutdown response is written, and is where a plugin exits, as the SDK does (it must not call `Worker.Wait`).
- `HandleEvent` receives every event the host pushes, as an `onEvent` request or as an `onEvent` notification (handled on its own goroutine, scoped to the invocation the notification carries); `Events().Subscribe` is `ctx.events.on`'s `events.subscribe`. A handler's error is logged to the host, never returned, as in the SDK. Without `HandleEvent`, `onEvent` goes to `Handle` and `ForwardNotification` as before.
- `Worker.ReplyUnserved`, set as `Forward`, answers a request no handler owns as the SDK does for a plugin without the hook: `METHOD_NOT_IMPLEMENTED` for the methods a plugin may leave out, `WORKER_ERROR` ("No data handler registered for key ..." and the like) for `runJob`, `getData`, `performAction` and `executeTool`, success for `onEvent`, `shutdown` and `health`, and `METHOD_NOT_FOUND` for the rest. It differs in three ways: `configChanged` is answered `METHOD_NOT_FOUND`, because the host restarts the worker on `METHOD_NOT_IMPLEMENTED`; `shutdown` does not exit; `initialize` and the idle-sleep methods are `METHOD_NOT_FOUND`, the host calling `prepareIdleSleep` only when `initialize` advertised it.
- `Typed` adapts a function of decoded params to a `Handler` (`INVALID_PARAMS` when the params do not decode, `DecodeParams` is its decoder). `InvocationIDOf` reads the invocation a host message carries, to scope a notification with `WithInvocation`.
- `Execution().Log` is `ctx.execution.log`, an `execute.log` notification the host drops without the invocation `ctx` carries. It sends a chunk as it is, and the host drops one over 1,000,000 characters. `Issues().ListFiltered` is `issues.list` with the SDK's filters (`IssueQuery`; a zero `Limit` or `Offset` is not sent, and no limit means no window); `List` sends only the company, limit and offset.
- `Handle` registers a host-to-worker method and `HandleJob` one scheduled job (a `runJob` call is routed by its `job.jobKey`); each call runs on its own goroutine. A returned `*rpc.Error` is sent as it is, any other error becomes a `WORKER_ERROR` response, and a panic is recovered into one.
- A request no handler owns goes to `Worker.Forward`, and every notification to `Worker.ForwardNotification`, on the reading goroutine and in arrival order; without `Forward` the request is answered `METHOD_NOT_FOUND`. They exist for a plugin that fronts another worker: it owns some methods in Go and relays the others to a child process. `Worker.Host`, `rpc.Conn.WriteRaw` and `rpc.Envelope.With`/`Without` exist for the same router (to relay a line, or one with its id replaced, without re-encoding it).
- Everything that runs on the reading goroutine must not block: `Forward`, `ForwardNotification`, the `Handlers` of `rpc.Conn.Serve` and the reply callback of `rpc.Conn.Request`. Calling `CallHost` or `Conn.Call` synchronously from one deadlocks, because the response can only be read by the goroutine that is waiting. Hand the work to a goroutine.
- `Serve` runs once. When the host closes the stream it settles the calls still waiting with a `WORKER_UNAVAILABLE` error, and refuses later ones the same way. `Worker.Wait` is for after `Serve` has returned.
- `rpc.Conn.KeepPendingOnClose`, set before `Serve`, opts a `Conn` out of that settling: when the stream ends or fails to read, `Serve` returns without settling the pending requests, which stay pending, and later requests are written as before (answered `WORKER_UNAVAILABLE` only if the write fails).
- A worker-to-host call must be made with the `context.Context` the handler received: `CallHost` echoes the invocation id in it, which is how the host resolves the company of the call. A call from a detached context loses that scope.
- `AfterReply` runs a function once the handler's response has been written (a `shutdown` handler that exits needs it).

## Exported API

`go doc github.com/nicodarge/paperclip-plugin-go/rpc` and `go doc github.com/nicodarge/paperclip-plugin-go/worker` list it.

## Releases

A push to `production` runs `release.yaml`:

1. `svu next --v0` derives the version from the conventional commits since the last tag: `fix:` releases a patch, `feat:` a minor, `feat!:` or a `BREAKING CHANGE:` footer a major, except that while the module is `v0` a breaking change bumps the minor, so a tag never reaches `v1.0.0` by a commit message. Other types (`chore:`, `docs:`, `ci:`, `test:`, `refactor:`) release nothing.
2. The job tags the commit `vX.Y.Z`, pushes the tag and creates the GitHub release with generated notes.

Consumers import the module at a tag: `go get github.com/nicodarge/paperclip-plugin-go@vX.Y.Z`. The workflow uses the job's `GITHUB_TOKEN` only and needs no secret. Never create or move a tag by hand.

## CI

Every workflow runs on GitHub-hosted `ubuntu-latest` runners and holds no secret.

| Workflow | Runs | What |
| --- | --- | --- |
| `ci.yaml` | pull requests | `go vet`, `go test -race ./...` (job `test`) and golangci-lint (job `golangci-lint`) |
| `lint.yaml` | pull requests | The pre-commit linters and guards (`manual` stage) over the whole tree, as a backstop for the commit-stage hooks (job `lint`) |
| `release.yaml` | push to `production` | The release described above |

Renovate (`renovate.json`) bumps GitHub Actions, the CI tool modules and the pre-commit hooks every Monday and merges patch and minor updates once CI is green. A major, a `0.x` or a prerelease update stays open for a person.

## Development

Install the commit hooks once: `pre-commit install`. They run `gofmt -s -w`, actionlint, yamllint, shellcheck and markdownlint, `golangci-guard` (from `nicodarge/go-tools`) on the golangci-lint configuration and on Go comments, plus whitespace and secret guards. Tests, `go vet` and golangci-lint run in CI only.
