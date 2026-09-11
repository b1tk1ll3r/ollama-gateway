# Checkpoint 22 — IP-bypass / trusted-proxy hardening

Date: 2026-09-08

## Security issue addressed

Before checkpoint 22, `Authenticator.Authenticate` evaluated `auth.ip_bypass` against the fully resolved client IP. If the immediate peer was in `auth.trusted_proxies`, that address could originate from `X-Forwarded-For`. A deployment that trusted a broad reachable subnet could therefore allow a direct client in that subnet to present a bypass address such as `127.0.0.1`.

## New default

- `ClientIP()` still resolves trusted forwarding chains for observability.
- `auth.ip_bypass` is evaluated against the direct TCP peer by default.
- `Identity.ClientIP` remains the resolved client address for usage/logging.
- Legacy forwarded-IP bypass is available only with explicit `auth.ip_bypass_use_forwarded_ip=true`.
- Config preflight warns when forwarded bypass is enabled and when `auth.trusted_proxies` contains broad non-loopback CIDRs.

API-key and OIDC authentication semantics are unchanged. No persistent-state schema change is introduced.

## Regression coverage

Tests prove that:

1. a trusted peer with `X-Forwarded-For: 127.0.0.1` cannot satisfy loopback IP bypass by default,
2. forwarded client-IP resolution still reports `127.0.0.1` for observability in that test,
3. explicit compatibility mode restores forwarded-IP bypass,
4. a direct loopback peer still satisfies the normal loopback bypass,
5. preflight warns on compatibility mode and a broad `10.0.0.0/8` trusted-proxy range while not flagging loopback-only proxy ranges.

## Built-binary spoof E2E

A release Linux-amd64 gateway was started with a trusted loopback proxy peer and an IP-bypass rule for `192.0.2.123/32`.

With the new default (`ip_bypass_use_forwarded_ip=false`), a request from the trusted TCP peer carrying `X-Forwarded-For: 192.0.2.123` returned **401 Unauthorized**.

The same test with the explicit compatibility flag set to `true` returned **200 OK** with an `ip-bypass` admin identity and resolved `client_ip=192.0.2.123`. This proves both the secure default and the intentional compatibility escape hatch in the release binary.

## Production-config preflight

The user's two-worker configuration validates successfully with the checkpoint-22 candidate. Preflight reports broad trusted-proxy warnings for `10.0.0.0/8`, `172.16.0.0/12` and `192.168.0.0/16`; these no longer permit forwarded headers to satisfy IP bypass under the default, but should still be narrowed to improve client-IP attribution integrity.
