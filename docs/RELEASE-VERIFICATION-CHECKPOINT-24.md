# Release verification — Checkpoint 24

Checkpoint 24 is an admin-WebUI-only visual refresh based on the production-hardened Checkpoint 23 runtime. It changes the embedded HTML/CSS/JavaScript presentation layer and the four gateway binaries that embed those assets. Scheduler, quota, routing, authentication, persistence, telemetry and inference semantics are unchanged.

## Scope

- Professional light control-plane theme inspired by dense technical/mission-database UIs.
- Offline-safe font stack: preferred local IBM Plex/JetBrains/Cascadia fonts with system fallbacks; no external font or CDN dependency.
- Explicit light color scheme for browser-native controls.
- Light sidebar, panels, KPI cards, tables, forms, dialogs, callouts, code/config editor and login screen.
- Light Live Flow and Infrastructure canvases, including canvas-drawn labels/nodes/routes rather than CSS-only restyling.
- Accessibility-oriented color tokens. Primary text, muted text, accent text, primary buttons and semantic status colors meet approximately WCAG AA 4.5:1 contrast against white for normal text.
- Regression tests assert that the light theme, light metadata and light canvas colors remain embedded.

## Verification performed

```sh
node --check internal/webui/assets/app.js
python -c 'import tinycss2; ... parse internal/webui/assets/app.css ...'
go test ./...
go vet ./...
go test -race ./...
```

All commands completed successfully.

Four gateway release targets were rebuilt because WebUI assets are embedded in the gateway binary:

- linux/amd64
- linux/arm64
- darwin/arm64
- windows/amd64

`dist/SHA256SUMS` was regenerated and `sha256sum -c dist/SHA256SUMS` succeeded for all shipped binaries.

## Release-binary smoke

The rebuilt linux/amd64 binary was started with an isolated writable data directory and deterministic mock Ollama backend. The following checks passed:

- `-check-config`
- `/readyz`
- embedded `/admin/`
- embedded `/admin/app.css` contains the Checkpoint 24 light-theme marker
- embedded `/admin/app.js` passes `node --check`
- `/gateway/ui-api/session` via direct loopback admin IP bypass returns an admin identity
- `/v1/chat/completions` returns a successful mock response
- SIGTERM shutdown exits with code 0

A headless Chromium screenshot was attempted, but the container's Chromium/DBus environment did not terminate reliably. This was not counted as a passed test. Asset parsing, embedding and runtime serving were verified independently as described above.

## Deployment

No configuration or persistent-state migration is required from Checkpoint 23. Rebuild/redeploy the gateway image because the WebUI is compiled into the binary. After update, hard-refresh the browser once to discard any previously cached `app.css` or `app.js`.
