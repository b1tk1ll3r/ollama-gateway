# Checkpoint 17 — production update verification

Validation date: 2026-09-08  
Validation toolchain: Go 1.23.2 on Linux/amd64

## Release scope

Checkpoint 17 completes sustained HA-readiness resource sampling. It adds `cmd/ha-sampler`, shared read-only resource collection under `internal/haresource`, sweep integration, report integration, tests and operator documentation.

The normal gateway request, scheduler, routing and persistence code is unchanged relative to checkpoint 16. Rebuilding with the same Go toolchain and release flags produced byte-identical gateway executables for all four packaged gateway targets.

## Required verification results

The following checks passed on the final source tree:

- `go test ./...`
- `go test ./... -count=3` during the release cycle
- `go vet ./...`
- `go test -race ./...`
- `gofmt -l .` returned no files
- `sh -n scripts/ha-readiness.sh scripts/ollama-env.sh`
- all shipped JSON example configurations parse with `jq`
- all files in `dist/SHA256SUMS` verify successfully

## Runtime smoke validation

The packaged Linux/amd64 gateway was tested against the deterministic mock Ollama backend.

1. **Persistent restart smoke:** two complete start/readiness/request/shutdown cycles reused the same `storage.data_dir`. Each cycle served a non-streaming and a streaming OpenAI-compatible request, logged `gateway stopped`, and exited with status 0.
2. **Authenticated readiness smoke:** private `/metrics` plus a Bearer API key completed the readiness sweep. Client counts reconciled with gateway request/queue/service counters, endpoint resource snapshots were complete, and sustained resource traces were complete.
3. **Secret-leak check:** the Bearer API key used by the authenticated sweep was not present in any generated readiness result file.
4. **Final sustained-sampler E2E:** concurrency 1 and 4 both completed with zero request errors, exact counter reconciliation, and `resource_samples.stop_reason == "stop-file"`. A sampler that exits because of `max-duration`, process exit, or interruption is deliberately not accepted as complete sustained evidence.

## Gateway binary identity vs checkpoint 16

| Target | Checkpoint 17 SHA-256 | Compared with checkpoint 16 |
|---|---|---|
| Linux amd64 | `01efc1b236faa9b7b912e6bee6af8266f6a24722af419a6dd43b6f294c955f47` | byte-identical |
| Linux arm64 | `a364f03e67fe377f140936c2c0cdd2b480166e2c5d1fe519878d730268e74248` | byte-identical |
| macOS arm64 | `8f9185f561752656902ef8cf44f5f1c8f65b660f40fb5caf790c2d23773c14d8` | byte-identical |
| Windows amd64 | `5a5bbbf962f1a9fea88a76fc0ee1e28669170496800e9ea39d9ed11caf14b053` | byte-identical |

This identity is the strongest update-safety property of this checkpoint for systems already on checkpoint 16: replacing the gateway executable with the checkpoint 17 gateway executable does not change the executable bytes.

## Operator note

For systems older than checkpoint 16, use `docs/PRODUCTION-UPDATE.md` and take a configuration/data-directory backup first. Earlier checkpoints contain real gateway runtime and persistence features, so the byte-identity statement above does not apply to an upgrade directly from an older production build.
