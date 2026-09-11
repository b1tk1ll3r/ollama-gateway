# OpenWebUI integration

## Recommended connection

Use OpenWebUI's Ollama provider connection with the gateway base URL (no `/api` suffix) and a dedicated gateway API key. You can either configure a durable key in `auth.api_keys` or create a persistent key under **Admin -> Sicherheit -> API Keys**.

Gateway configuration:

```json
"auth": {
  "oidc": { "enabled": false },
  "api_keys": [
    {
      "name": "openwebui",
      "key": "${OPENWEBUI_GATEWAY_KEY}",
      "tenant": "interactive",
      "subject": "openwebui",
      "application": "openwebui",
      "scopes": []
    }
  ],
  "ip_bypass": [],
  "trusted_proxies": []
}
```

### Create the key in the web interface

Open **Admin -> Sicherheit -> API Keys -> API-Key erstellen** and use, for example:

```text
Name:        openwebui
Tenant:      interactive
Application: openwebui
Scopes:      (empty)
```

Copy the generated `ofg_...` secret immediately and paste it into OpenWebUI. The gateway does not retain the plaintext value. The plaintext is shown once; only its SHA-256 hash and metadata are persisted, so the same OpenWebUI credential remains valid across gateway restarts.

OpenWebUI connection:

```text
URL:     http://host.docker.internal:8080
API Key: <same value as OPENWEBUI_GATEWAY_KEY>
```

OpenWebUI's backend appends native Ollama paths itself. Do not configure the URL as `...:8080/api`.

## Diagnostics

From a machine that can reach the gateway:

```bash
curl -i -H "Authorization: Bearer $OPENWEBUI_GATEWAY_KEY" http://GATEWAY:8080/api/version
curl -i -H "Authorization: Bearer $OPENWEBUI_GATEWAY_KEY" http://GATEWAY:8080/api/tags
```

Expected status for both is HTTP 200. `/api/tags` must contain a `models` array and every returned model is normalized to contain both `name` and `model`.

OpenAI-compatible discovery is also available:

```bash
curl -i -H "Authorization: Bearer $OPENWEBUI_GATEWAY_KEY" http://GATEWAY:8080/v1/models
```

## Docker networking

The default example IP bypass only trusts loopback. An OpenWebUI Docker container normally reaches the gateway from a Docker/host network address, so loopback bypass does not apply. Prefer a dedicated API key over widening the bypass CIDR.

On Docker Desktop for macOS, `host.docker.internal` normally resolves to the host. On Linux, use an address/service name reachable from the OpenWebUI backend or configure Docker's host-gateway mapping.

## Gateway compatibility behavior

The gateway owns model discovery instead of forwarding it to a single control worker:

- `GET /api/tags`: parallel inventory across all workers, deduplicated by model ID.
- `GET /api/ps`: aggregated loaded-model view.
- `GET /v1/models`: OpenAI model list generated from the same aggregate inventory.
- `POST /api/show`: routed to a worker that owns the requested installed model when known.
- Compute requests are constrained to workers known to own the model; a loaded copy receives additional affinity preference.

If one worker fails during discovery but another returns models, the gateway returns the available model set and sets `X-Gateway-Partial-Errors`. If no worker can provide tags, discovery returns HTTP 503 rather than an empty, misleading list.

## OpenWebUI with "Authentication: None" and IP bypass

If the OpenWebUI connection is configured with **Authentication: None**, the gateway must authenticate the OpenWebUI backend through `auth.ip_bypass`. The bypass is evaluated against the **source IP observed by the gateway**, not against the gateway URL.

For example, if the gateway log shows:

```text
authentication rejected client_ip=10.10.11.42 ... path=/api/version ...
```

add only that OpenWebUI host address when possible:

```json
"ip_bypass": [
  {
    "cidrs": ["10.10.11.42/32"],
    "tenant": "interactive",
    "subject": "openwebui",
    "application": "openwebui",
    "scopes": []
  },
  {
    "cidrs": ["127.0.0.1/32", "::1/128"],
    "tenant": "local",
    "subject": "localhost",
    "application": "local-tools",
    "scopes": ["gateway:admin"]
  }
]
```

Then OpenWebUI may remain configured with:

```text
URL:              http://10.10.11.123:8080
Authentication:   None
```

Do **not** blindly add `10.10.11.123/32` just because that is the gateway URL. That is the destination address. Use the `client_ip` value printed by the gateway when OpenWebUI performs `/api/version` or `/api/tags`.

For Docker, the observed source may be a Docker bridge/VM address rather than the LAN address of the host. A dedicated static API key is more stable than allowing a broad Docker subnet.

### Authentication diagnostics

Authentication failures now log the resolved client address and include it in the `X-Gateway-Client-IP` response header. Native Ollama endpoints also return Ollama-compatible errors such as:

```json
{"error":"authentication required"}
```

rather than an OpenAI-shaped nested object. This prevents OpenWebUI from rendering the gateway error as `[object Object]`.
