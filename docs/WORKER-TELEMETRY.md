# Remote worker telemetry

Ollama Fair Gateway can consume optional out-of-band resource telemetry through `workers[].telemetry_url`. Checkpoint 20 ships a small `worker-telemetry` agent so remote Ollama hosts can report their own RAM/GPU state instead of accidentally reporting the gateway host through `local_system_stats`.

The agent is **not** part of the inference request path. Gateway workers fetch it during the normal health/telemetry refresh cycle.

## Why this matters for remote workers

`local_system_stats: true` runs the memory collector inside the gateway process. It is correct only when the Ollama worker and gateway share the same host. For a worker such as `http://10.2.10.48:11434`, it otherwise reports the gateway machine's RAM as if it belonged to `10.2.10.48`.

For remote workers, use:

```json
{
  "name": "M75q - Gen5 - 1048",
  "url": "http://10.2.10.48:11434",
  "local_system_stats": false,
  "telemetry_url": "http://10.2.10.48:11500/telemetry"
}
```

and run the agent on that Ollama host.

## Validate locally first

Host memory only:

```sh
./ollama-gateway-worker-telemetry-linux-amd64 -once
```

AMD GPU through the Linux amdgpu sysfs interface:

```sh
./ollama-gateway-worker-telemetry-linux-amd64 -once -amd-sysfs
```

If auto-detection picks the wrong GPU, specify the device explicitly:

```sh
./ollama-gateway-worker-telemetry-linux-amd64 \
  -once \
  -amd-sysfs \
  -amd-device /sys/class/drm/card1/device
```

NVIDIA:

```sh
./ollama-gateway-worker-telemetry-linux-amd64 -once -nvidia-smi
```

The JSON schema matches the existing gateway `telemetry_url` contract:

```json
{
  "memory_used_bytes": 123,
  "memory_total_bytes": 456,
  "vram_used_bytes": 789,
  "vram_total_bytes": 1024,
  "gpu_utilization_percent": 42,
  "gpu_temperature_c": 63.5,
  "gpu_power_watts": 88,
  "source": "host-memory+amdgpu-sysfs",
  "updated_at": "2026-09-08T18:00:00Z"
}
```

A field may be absent when the operating system/driver does not expose it. The agent still returns the remaining usable metrics and puts collector failures in the `error` field.

## Run as a service

The listener defaults to loopback for safety. To expose it to the gateway, bind a worker interface and restrict clients by CIDR. Prefer an exact gateway host address when possible:

```sh
./ollama-gateway-worker-telemetry-linux-amd64 \
  -listen 0.0.0.0:11500 \
  -allow-cidrs 10.2.19.1/32 \
  -amd-sysfs
```

Example systemd unit:

```ini
[Unit]
Description=Ollama Gateway Worker Telemetry
After=network-online.target

[Service]
ExecStart=/usr/local/bin/ollama-gateway-worker-telemetry-linux-amd64 -listen 0.0.0.0:11500 -allow-cidrs 10.2.19.1/32 -amd-sysfs
Restart=on-failure
User=nobody
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true

[Install]
WantedBy=multi-user.target
```

Adjust the CIDR to the source IP the worker actually sees from the gateway. Do not use `0.0.0.0/0` merely to make the endpoint reachable.

## AMD telemetry details

On Linux the agent auto-detects the first `/sys/class/drm/card*/device` whose PCI vendor is `0x1002`. It reads standard amdgpu attributes when available:

- `mem_info_vram_total`
- `mem_info_vram_used`
- `gpu_busy_percent`
- `hwmon/*/temp1_input`
- `hwmon/*/power1_average`

No ROCm library, `rocm-smi`, NVML or CGO is required for the AMD path. Integrated/shared-memory GPUs may expose different or incomplete VRAM semantics; keep the statically configured `memory_capacity_bytes`/`vram_capacity_bytes` values as hard capacity hints and treat runtime telemetry as routing pressure information.

## Security

The agent intentionally has no application credential store. It relies on a narrow listener/firewall/CIDR allowlist so it remains dependency-free and does not introduce a second secret lifecycle. Resource telemetry can still reveal infrastructure details, so expose it only on a trusted management network, VPN or host firewall. For untrusted network segments, put the endpoint behind a TLS-authenticated reverse proxy rather than exposing it directly.

`workers[].telemetry_url` remains administrator-controlled and is an SSRF-capable URL. Point it only to trusted worker exporters.

## Freshness and caching

Checkpoint 21 validates `updated_at` when the external exporter supplies it. The shipped agent always sends the field and also returns `Cache-Control: no-store`.

A sample is rejected when it is older than `max(30s, 6 x workers[].health_interval)` or more than 30 seconds in the future. Rejected values are not merged into the routing telemetry; the reason is exposed through the worker telemetry error instead. This prevents a caching proxy or badly skewed exporter clock from silently presenting old resource pressure as current data.

For compatibility, third-party exporters that omit `updated_at` are still accepted. Their freshness cannot be verified, so new integrations should always provide an RFC3339 timestamp.
