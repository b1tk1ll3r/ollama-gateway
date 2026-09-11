# Checkpoint 26 release verification

Scope: embedded Admin WebUI only (dialog semantics + regression tests).

Verified:

- `go test ./...`
- `go vet ./...`
- `node --check internal/webui/assets/app.js`
- race tests across all packages (full run completed in two segments due execution timeout)
- rebuilt Gateway binaries for Linux amd64/arm64, macOS arm64, Windows amd64
- `dist/SHA256SUMS` verified
- real Linux-amd64 release-binary smoke confirmed embedded policy/pull close controls and clean SIGTERM shutdown

Regression covered: required HTML form controls must not prevent modal cancellation/close.
