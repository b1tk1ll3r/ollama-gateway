# Deployment hardening and preflight

Checkpoint 19 adds deployment checks intended to catch configuration/mount mistakes before production traffic reaches the gateway. These checks do not participate in the inference hot path.

## Effective configuration preflight

Run the gateway binary with `-check-config` before starting the service:

```sh
./ollama-gateway -config ./gateway-config.json -check-config
```

The command validates the same strict bootstrap schema as normal startup, loads the persistent UI override from `storage.data_dir` when present, enforces the bootstrap-only storage rule, verifies that the effective state directory is writable, and prints a secret-free JSON summary. A non-zero exit code means the gateway should not be started with that configuration.

The summary includes:

- bootstrap config path,
- whether a persistent override was loaded,
- persistent override path,
- effective worker count,
- effective/absolute state directory,
- storage writability,
- non-fatal deployment warnings.

Warnings currently cover common production foot-guns such as `local_system_stats=true` on a remote worker URL, an unknown `native.control_worker`, public metrics, a relative state directory and UI authentication/cookie combinations that deserve review.

For Compose deployments, run the check in the exact image/user/mount context that will be used in production:

```sh
GATEWAY_CONFIG=./gateway-config.json docker compose run --rm gateway \
  -config /etc/ollama-gateway/config.json -check-config
```

This is preferable to validating only on the Docker host because it also proves that the container user can read the bootstrap file and write the mounted state volume.

## Built-in HTTP probe

The scratch image contains no shell, curl or wget. The gateway binary therefore has a minimal HTTP probe mode:

```sh
/ollama-gateway -probe http://127.0.0.1:8080/healthz -probe-timeout 2s
```

It exits 0 for any 2xx response and non-zero for connection errors, timeouts or non-2xx responses. Probe mode does not load gateway configuration or state.

The image/Compose liveness healthcheck uses `/healthz`. Load balancers and rollout automation should additionally require `/readyz`; readiness checks scheduler, workers and persistent runtime stores and can return 503 while the process itself remains healthy.

## Compose entrypoint rule

The image already defines:

```text
ENTRYPOINT ["/ollama-gateway"]
```

Therefore Compose `command` must contain only arguments:

```yaml
command: ["-config", "/etc/ollama-gateway/config.json"]
```

Do **not** repeat `/ollama-gateway` in `command`. Repeating the executable turns it into the first positional argument passed to Go's flag parser and can prevent following flags from being interpreted as intended.

The supplied Compose file supports selecting a bootstrap configuration without editing the file:

```sh
GATEWAY_CONFIG=./gateway-config.json docker compose up -d --build
```

If `GATEWAY_CONFIG` is omitted, the development `config.example.json` is mounted. Do not mistake that default example for a production configuration; `-check-config` reports the effective worker count and makes that error easier to detect before rollout.

## Recommended staged update

1. Back up the bootstrap config and `storage.data_dir`.
2. Build/pull the candidate image.
3. Run `-check-config` in the candidate container with the production mounts.
4. Stop the old gateway gracefully.
5. Start the candidate and wait for the Docker liveness check.
6. Require `/readyz` before restoring traffic.
7. Verify authenticated `/gateway/ui-api/session`, one non-streaming request and one streaming request if used.
8. Keep the previous image/binary and state backup available until the observation window is complete.

## Checkpoint 23: reverse-proxy boundary and container sandbox

The supplied Compose file now publishes the gateway on host loopback by default:

```text
127.0.0.1:9080 -> container :8080
```

This is the preferred topology when the TLS/reverse proxy runs on the same Docker host. It prevents LAN clients from bypassing the proxy and reaching the gateway's published port directly. Override `GATEWAY_PUBLISH_ADDRESS` only when the real proxy is on another host. In that case bind to the specific host interface where possible and enforce a host firewall/ACL that permits only the real proxy source address.

The Compose runtime also uses a read-only root filesystem, drops all Linux capabilities and enables `no-new-privileges`. `/data` remains the only persistent writable application state mount and `/tmp` is an ephemeral tmpfs.

For the deployment observed during the checkpoint-22 rollout, browser traffic arrived at the container through Docker with TCP peer `172.30.3.1`. The hardened production configuration therefore replaces broad RFC1918 `trusted_proxies` ranges with the exact `172.30.3.1/32` peer. `ip_bypass_use_forwarded_ip` remains false. If the network path changes, re-observe the gateway's `remote_addr` and trust only the actual proxy/NAT peer that is expected to supply forwarded headers.

A host-side preflight catches the common rollout mistakes before Compose starts production traffic:

```sh
cp .env.production.example .env
GATEWAY_CONFIG=./gateway-config.json ./scripts/production-preflight.sh
```

It fails if the production config points to `config.example.json`, if the gateway is published outside loopback without explicit acknowledgement, if Compose rendering fails, or if the gateway's own `-check-config` rejects the effective configuration/state mounts.

For a remote reverse proxy, use an explicit interface and firewall policy, for example:

```sh
GATEWAY_PUBLISH_ADDRESS=10.2.10.20 \
ALLOW_NON_LOOPBACK_BIND=1 \
GATEWAY_CONFIG=./gateway-config.json \
./scripts/production-preflight.sh
```

The explicit opt-in is not a substitute for a firewall. The published port should be reachable only from the reverse proxy.

For production, prefer the stricter `docker-compose.production.yml`. Unlike the development Compose file, it requires `GATEWAY_CONFIG` to be set and never falls back to `config.example.json`.
