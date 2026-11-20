# paperclip-plugin-go

A Go implementation of the worker side of the [Paperclip](https://github.com/paperclipai/paperclip) plugin protocol. Paperclip starts a plugin worker as a child process and speaks newline-delimited JSON-RPC 2.0 with it over stdin and stdout; this module is the transport and routing of that worker, in Go, with the standard library only. It is not a port of `startWorkerRpcHost` of `@paperclipai/plugin-sdk`: the worker routes the host's requests to registered handlers or forwards them, and the plugin implements `initialize`, `health`, `shutdown`, `onEvent` and `runJob` itself.

Paperclip itself publishes no Go SDK.

## Layout

| Path | Content |
| --- | --- |
| `rpc/` | The wire protocol: `Envelope` (a JSON-RPC 2.0 message that keeps every top-level member as raw JSON, so a forwarded message carries members this code does not know), the protocol's error codes, and `Conn`, one NDJSON stream with its own outbound request ids |
| `worker/` | `Worker`: serves the host's requests with registered handlers, forwards the rest, and calls back into the host (state, config, secrets, agents, issues, database, logger) |
| `tools/<name>/` | One Go module per CI tool (`golangci-lint`, `svu`), pinned in its `go.mod` and verified by its `go.sum` |
| `scripts/install-tools.sh` | Builds the pinned CI tools from `tools/` |

## Protocol version

Written against the Paperclip release `v2026.1023.0` (commit `1b4c803816e82b0265c8422e1a804327a7ed8c54`), the one `conformance/pin.json` of `nicodarge/gro-bridge` pins on 2026-11-20 (from the `nicodarge/paperclip` fork): the error codes, method names and message shapes follow `packages/plugins/sdk/src/` of that commit. The conformance run that drives the real Paperclip worker manager against a plugin lives in `gro-bridge`, not here; this repository's tests exercise the worker against an in-process host.

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
    w.Handle("initialize", func(ctx context.Context, params json.RawMessage) (any, error) {
        return map[string]bool{"ok": true}, nil
    })
    w.Handle("health", func(ctx context.Context, _ json.RawMessage) (any, error) {
        return map[string]string{"status": "ok"}, nil
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
