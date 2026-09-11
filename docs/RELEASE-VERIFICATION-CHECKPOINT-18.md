# Checkpoint 18 — Admin UI placement hotfix

Validation date: 2026-09-08

## Release scope

Checkpoint 18 is a production hotfix on top of checkpoint 17. It restores the complete Model Placement UI helper block that was accidentally removed during the runtime model-alias UI work introduced after checkpoint 13.

The regression manifested after successful admin authentication as a browser error such as:

`renderPlacement is not defined`

The missing block also contained the generic `lines()` helper used by alias, placement and tenant model-access forms, so leaving only `renderPlacement` patched would have exposed follow-on UI failures when saving those forms.

No gateway request, scheduler, routing, quota, persistence, batch, conversation, HA-sampling or worker-runtime behavior was intentionally changed by this hotfix. The gateway executables do change because the Admin UI JavaScript is embedded into the binary.

## Fix

Restored these UI helpers:

- `placementRule`
- `placementRows`
- `placementCell`
- `placementSourceLabel`
- `renderPlacement`
- `placementCellHTML`
- `placementWorkerCard`
- `loadPlacement`
- `openPlacementDialog`
- `lines`
- `installedForWorker`
- `savePlacementRule`
- `placementExactAction`

The existing placement matrix, worker-rule editor, exact allow/deny overrides, presets and reset controls now have their implementation functions again.

## Regression protection

`internal/webui/webui_test.go` now verifies that every `renderX()` function called by the main `render()` dispatcher is actually defined in the embedded Admin UI bundle. A second test verifies the complete Placement UI helper set.

This specifically prevents the original `renderPlacement is not defined` class of regression from passing `go test ./...` again.

## Required verification results

The final source tree passed:

- `go test ./...`
- `go vet ./...`
- `go test -race ./...`
- `node --check internal/webui/assets/app.js`
- release cross-builds for Linux amd64, Linux arm64, macOS arm64 and Windows amd64
- `dist/SHA256SUMS` verification

## Update guidance

For Docker/Compose deployments, rebuild or replace the gateway image/binary. Copying only the source `app.js` beside an already-built binary does not fix the problem because `internal/webui/assets/app.js` is embedded at build time.

The production configuration and persistent `/data` volume do not require a migration from checkpoint 17 to checkpoint 18.

## Gateway binary SHA-256

| Target | SHA-256 |
|---|---|
| Linux amd64 | `550f481313e5869d884280dfc3248d70b6eabf51b4b3bb44083c36f622065cf2` |
| Linux arm64 | `27f4ab0b7a1dc9a2379e7e99471e177f9da9c715445b05b089d028011b8101aa` |
| macOS arm64 | `d72dd623844050b1b7396ae364e49a30ab76d2bee21a2ced22dad308c0e839bb` |
| Windows amd64 | `dedf06c3d42fdc8c2fe3de19cd9c00fcdb6eb6fab91442df3b18ac5e8a416072` |
