# Security notes

## OIDC

The gateway validates bearer JWTs against OIDC discovery and JWKS. It rejects `alg=none`, restricts accepted signature algorithms, validates issuer, audience, expiry and `nbf`, and refreshes JWKS when a `kid` is unknown. API bearer-token validation is also the source of truth for the optional browser Authorization Code + PKCE login flow.

## Trusted-IP bypass

IP bypass is evaluated before bearer authentication. Starting with checkpoint 22, the **TCP peer address** is the authentication source for `auth.ip_bypass` by default, even when `X-Forwarded-For` is accepted for logging and usage attribution. This prevents a trusted-proxy chain from turning a spoofed forwarded address into a credential-free identity.

Set `auth.ip_bypass_use_forwarded_ip=true` only when a deployment intentionally needs an original client address behind a reverse proxy to satisfy IP bypass. That mode requires a tightly restricted `auth.trusted_proxies` list plus network ACLs that prevent clients from reaching the gateway directly. Prefer narrow `/32` or `/128` bypass/proxy entries. Never configure a broad Internet-wide bypass for an exposed gateway.

## API keys

API keys are represented in the authenticator by their SHA-256 digest; plaintext runtime secrets are not retained. Clients may send either `X-API-Key` or `Authorization: Bearer ...`.

There are two sources:

- **Configuration keys** come from `auth.api_keys`. Put their secrets in environment variables and reference them with `${NAME}` in JSON. They survive restarts and are read-only in the web UI.
- **Runtime keys** are created by an administrator in the web UI or through `POST /gateway/ui-api/api-keys`. A cryptographically random `ofg_...` secret is returned once, then only the digest and metadata are persisted in the local key store. UI-created keys can be revoked immediately and survive process restarts until revoked.

Only identities with `gateway:admin` may list, create or revoke API keys. Cookie-authenticated mutations are covered by the same CSRF checks as the rest of the control plane. Key secrets are never included in list responses, the redacted configuration endpoint or admin logs.

## Ollama management

With `native.management_requires_admin=true`, model-management operations require `gateway:admin`. Do not expose Ollama port 11434 directly to untrusted clients; expose only the gateway.

## Web control plane

Browser OIDC uses Authorization Code + PKCE. The transient verifier/state is HMAC-protected with `ui.session_secret`. After exchange, the browser receives only an opaque session identifier; the OIDC access token remains in the gateway's in-memory session store.

A process restart invalidates all browser sessions. Cookie-authenticated state-changing requests require a same-origin CSRF token. The embedded UI uses a restrictive Content Security Policy and has no CDN, analytics or third-party JavaScript dependency.

Policy overrides are persisted locally and take effect immediately for new requests.

## In-memory security boundary

Removing external state storage reduces the network attack surface: there is no coordination service, state-store credential or message-bus endpoint to protect. Sensitive runtime material such as OIDC session tokens and quota state exists only in process memory.

A crash or restart clears browser sessions and active transient inference jobs, but API keys, policy overrides, quota state, usage history, metric counters and durable batch definitions are restored from local durable state.

## Optional content-bearing conversations

The default gateway privacy posture remains content-free: prompts and generated responses are not persisted unless `conversations.enabled=true` is explicitly configured. The optional Responses conversation store is isolated from usage retention and uses AES-256-GCM encryption at rest, a dedicated retention window, an entry cap, and a per-context byte cap.

`previous_response_id` lookup is bound to the authenticated tenant and scheduler actor. A response ID from another identity is reported exactly like a nonexistent ID to avoid enumeration. `store:false` prevents persistence of the new response. The Admin UI exposes store status but not decrypted conversation content. See `docs/CONVERSATIONS.md`.

The conversation encryption key is a deployment secret. Losing or changing it makes the existing ciphertext unreadable; exposing it together with the state file defeats at-rest confidentiality. Backups should therefore be protected as secrets.

## Optional content-bearing batch jobs

Durable batch jobs are also disabled by default. Enabling them does not persist content by itself, but every submitted job stores its request body and later its response body under `storage.batch_jobs_dir`. These spool payloads are **not encrypted by the gateway**; they rely on mode-0600 files, a mode-0750 spool directory and the deployment filesystem/disk security boundary. Use filesystem or full-disk encryption if at-rest content encryption is required.

Batch metadata stores an authenticated identity/authorization snapshot but never the bearer token or API-key secret. Owner-facing `/gateway/v1/batches` endpoints are tenant+actor scoped; cross-identity IDs are hidden as not found. Global batch inspection/control and output download are restricted to the admin UI/API and therefore require `gateway:admin`.

An accepted job can execute after the original credential has been revoked because the credential secret is intentionally not retained. Current tenant model policy, worker placement/maintenance/circuit state, quotas and scheduling are still evaluated when each attempt runs. Operationally, revoke the credential **and cancel its accepted durable jobs** when retroactive cancellation is required.

Batch input/output files and backups can contain prompts, tool arguments, retrieved data and model output. Configure a short `batch_jobs.retention`, protect `storage.data_dir`, and treat backup archives as content-bearing secrets. See `docs/BATCH-JOBS.md`.

## Live telemetry

Live telemetry contains tenant/actor identifiers, request IDs, model names, worker names, timings, token counts and credit estimates, but never prompt text or generated model output. UI telemetry endpoints require an authenticated administrator.

`workers[].telemetry_url` is administrator-controlled and fetched by the gateway. Point it only to a trusted exporter because it is an SSRF-capable configured URL, just like an Ollama worker URL. The optional shipped worker-telemetry agent should be bound to a trusted management interface and restricted with `-allow-cidrs`; it intentionally does not create a separate application-secret store.

## Model access control

Model access control is independent from worker placement. Tenant defaults are configured under `model_access`; API keys can carry a narrower or different allow/deny list. The gateway applies the identity ACL before scheduling and filters `/api/tags` and `/v1/models` accordingly.

Patterns are exact model/alias names, `*`, or one trailing wildcard such as `qwen3:*`. The most specific matching rule wins; deny wins a specificity tie. API-key ACL metadata is persisted together with the key hash and is editable at creation time in **Admin -> Sicherheit -> API Keys**.

## Reverse-proxy network boundary

Prefer publishing the gateway only on loopback when the reverse proxy is local to the Docker host. A proxy should be the only network component able to reach the gateway's published port. Avoid trusting whole RFC1918 ranges merely because the deployment uses private addressing; list only concrete proxy/NAT peer addresses in `auth.trusted_proxies`.

Checkpoint 23's Compose defaults additionally run with a read-only root filesystem, all Linux capabilities dropped and `no-new-privileges`. Persistent writes are constrained to `/data`; `/tmp` is ephemeral. These controls reduce the impact of a process compromise but do not replace API-key/OIDC authentication or host firewalling.
