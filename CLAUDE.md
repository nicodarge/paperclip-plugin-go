# CLAUDE.md

The README describes the layout, the API and the release flow; this file is what to keep true when changing it. Its conventions are those of `nicodarge/go-tools`, from which the CI, lint and release setup is copied.

## Rules

- Public repository: no secret, no self-hosted runner label, no reference to internal hosts, addresses or credentials. Every job runs on `ubuntu-latest`.
- Go standard library only, no cgo, no Node.js: the module is imported by `gro-bridge` and `gro-sandbox` and must add no dependency to them. The `markdownlint` pre-commit hook is the one third-party Node tool.
- The module is the public surface of the Paperclip worker protocol: a change to an exported name or signature is `feat!:`, and a new one is `feat:`. Keep the surface minimal; a helper only one consumer needs stays in that consumer.
- Nothing in this repository names a consumer (`gro-bridge`, `gro-sandbox`) in code or in a test: a job key, a method or a state key in a test is a placeholder.
- A change follows the protocol of the Paperclip release the README names, read from `packages/plugins/sdk/src/` of that commit; when the host moves, re-read it and update the README line with the date. Verify a wire detail (member name, error code, echo rule) in that source, never from memory.
- Releases are `release.yaml`'s: never create, move or delete a `v*` tag by hand, and never edit a version. The pull request title (squash subject) is the conventional commit `svu` reads: `fix:` patch, `feat:` minor, `feat!:` major; while the module is `v0`, `release.yaml` runs `svu next --v0` and a `feat!:` or `BREAKING CHANGE:` bumps the minor, not the major (a tag is irreversible: the tag ruleset forbids updating or deleting one, so `v1.0.0` is cut only by a deliberate change to `release.yaml`); `chore:`, `docs:`, `ci:`, `test:` and `refactor:` release nothing.
- CI tools live in `tools/<name>/` as one Go module each, installed by `scripts/install-tools.sh`. Do not merge them into one `go.mod`: their dependency graphs conflict. Never download a CI tool's binary in a workflow; add it as a module.
- Pin every GitHub Action by commit SHA with the tag in a trailing comment.
- The golangci-lint config is `.golangci.yaml`. Every golangci-lint finding fails CI and is fixed at source: no `//nolint`, no `#nosec`, no exclusions. The `golangci-config-guard` and `go-no-suppression` hooks (`nicodarge/go-tools`, pinned by `rev`, bumped by Renovate) enforce it; their rules are in the README of that repository. An exported name needs its doc comment (revive `exported`).

## CI

- `ci.yaml` and `lint.yaml` run on pull requests and produce the checks `test`, `golangci-lint` and `lint`; a new workflow that puts a check on a pull request needs no change elsewhere except the `production` ruleset's required checks.
- Renovate merges green patch and minor updates itself. There is no `pr-review`, `bump-review` or `auto-merge` workflow here: the shared workflows live in a private repository a public one cannot call.
- No AI review runs in this repository's CI: run the `pr-reviewer` subagent locally on the pushed branch before `gh pr create`.
