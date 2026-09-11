# Verbesserungen und Prioritäten (September 2026)

Dieses Dokument beschreibt die nach einem Abgleich mit Ollama, OpenWebUI und etablierten LLM-Gateways priorisierten Verbesserungen. Ziel bleibt ein einzelner, sehr schneller Go-Prozess ohne Redis oder Datenbank im Inference-Hot-Path.

## Priorität A – in diesem Release umgesetzt

### 1. Capability-aware Model Preflight

Ollama liefert über `POST /api/show` Modell-Capabilities und Modellinformationen. Das Gateway cached diese Metadaten pro Worker/Modell und erkennt aktuell:

- `completion`
- `tools`
- `vision`
- `thinking`
- `embedding`

Ein Request wird vor Admission gegen seine benötigten Capabilities geprüft. Das verhindert z. B., dass ein OpenWebUI-Tool-Request erst tief im Ollama-Backend mit `does not support tools` scheitert.

Konfiguration:

```json
"model_capabilities": {
  "mode": "enforce",
  "cache_ttl": "10m",
  "context_guard": "reject",
  "context": {
    "max_requested_tokens": 32768,
    "default_worker_tokens": 4096,
    "estimation_margin_percent": 15,
    "vision_reserve_tokens_per_image": 2048
  }
}
```

`mode`:

- `enforce`: bekannte Capability-Konflikte mit HTTP 400 ablehnen.
- `observe`: nur Warnheader/Logs setzen.
- `off`: Metadaten-Preflight deaktivieren.

Wenn `/api/show` temporär nicht erreichbar ist, lässt das Gateway den Request bewusst durch. Verfügbarkeit hat bei unbekannten Metadaten Vorrang; nur *bekannt inkompatible* Requests werden geblockt.

### 2. Context Guard

Der Gateway trennt das **theoretische Modellmaximum** von dem Kontext, den ein konkreter Worker tatsächlich bereitstellt. Für OpenAI-/Responses-/Anthropic-kompatible Requests ohne natives `num_ctx` gilt als effektive Quelle in dieser Reihenfolge:

1. `context_length` des bereits geladenen Modells aus `/api/ps`;
2. `num_ctx` aus den Modelfile-Parametern von `/api/show`;
3. `workers[].default_context_tokens`;
4. `model_capabilities.context.default_worker_tokens`.

Anschließend greifen `workers[].context_limits` und der Gateway-Cap. Ein natives `options.num_ctx` wird nur auf `/api/*` akzeptiert, muss ein positiver Integer sein und wird gegen Gateway-Cap, Modellmaximum und Worker-Cap geprüft. Ein kleinerer aktuell geladener Kontext darf für ein explizit größeres `num_ctx` neu allokiert werden, erhält dann aber keinen Loaded-Routingbonus.

Die Admission-Schätzung zählt neben Prompt/Messages/Tool-Schemas jetzt auch `instructions`, `suffix`, `max_output_tokens`, eine konfigurierbare Vision-Reserve pro Bild und eine Sicherheitsmarge. Das ist bewusst eine konservative Schätzung und kein Tokenizer-Ersatz.

### 3. Per-model Concurrency

Ein 24-GB-Worker sollte kleine 8B-Modelle anders behandeln können als 27B-Modelle. Jeder Worker kann deshalb zusätzlich zu `max_concurrent` Modellgrenzen definieren:

```json
"model_concurrency": {
  "qwen3:8b": 2,
  "gemma4:*": 1,
  "*": 1
}
```

Matching-Reihenfolge:

1. exakter Modellname;
2. längster passender Prefix mit abschließendem `*`;
3. `*`;
4. `max_concurrent`.

Der Slot wird im selben in-memory Worker-State geführt und bei Cancel/Fehler/Completion wieder freigegeben.

### 4. Adaptive Worker Routing

Worker-Routing berücksichtigt jetzt zusätzlich zu Health und aktiven Slots:

- bereits geladenes Modell;
- auf dem Worker installiertes Modell;
- per-model Slot-Auslastung;
- gelernte Output-Tokenrate je Worker/Modell (EWMA), persistent über Neustarts;
- tatsächlichen VRAM-Druck, wenn Telemetrie verfügbar ist;
- GPU-Auslastung;
- eine harte Zusatzstrafe für das Laden eines neuen Modells bei sehr hohem VRAM-Druck.

Die Gewichte sind konfigurierbar:

```json
"routing": {
  "loaded_bonus": 60,
  "installed_bonus": 30,
  "throughput_bonus": 20,
  "vram_pressure_penalty": 35,
  "gpu_utilization_penalty": 10,
  "avoid_vram_percent": 97
}
```

Durchsatzwerte werden ausschließlich aus echten abgeschlossenen Inferenzrequests gelernt und unter `storage.worker_performance_file` persistiert. Nach einem Neustart startet das Routing deshalb mit den zuletzt bekannten EWMA-Werten und lernt sie anschließend weiter.

### 5. NVIDIA-Telemetrie ohne zusätzlichen Agenten

Auf NVIDIA-Hosts kann ein Worker direkt `nvidia-smi` verwenden:

```json
"nvidia_smi": true,
"nvidia_gpu": "0"
```

Erfasst werden:

- GPU utilization
- VRAM used / total
- Temperatur
- Power Draw

Die Abfrage läuft außerhalb des Request-Hot-Paths im normalen Worker-Health-Zyklus. Das Gateway bleibt `CGO_ENABLED=0` und benötigt keine NVML-Bibliothek. Ein vorhandenes `telemetry_url` kann weiterhin verwendet werden und externe Werte überschreiben.

### 6. Operations UI und Metrics

Die Modellansicht zeigt jetzt erkannte Capabilities und Context Length. Worker-Karten zeigen – soweit vorhanden – GPU, VRAM, Temperatur, Leistung und gelernte tok/s.

Prometheus exportiert zusätzlich:

```text
ollama_gateway_worker_memory_used_bytes
ollama_gateway_worker_memory_total_bytes
ollama_gateway_worker_vram_used_bytes
ollama_gateway_worker_vram_total_bytes
ollama_gateway_worker_gpu_utilization_percent
ollama_gateway_worker_gpu_temperature_celsius
ollama_gateway_worker_gpu_power_watts
ollama_gateway_worker_model_active
ollama_gateway_worker_model_prompt_tokens_per_second
ollama_gateway_worker_model_output_tokens_per_second
ollama_gateway_worker_model_performance_samples
```

Modelle sind eine begrenzte operative Dimension. Tenant-/User-/Request-IDs werden weiterhin nicht als Prometheus-Labels ausgegeben.

### 7. Unbegrenzte Credits verständlicher im UI

`0` bedeutet weiterhin unbegrenzt. Die Policy-Oberfläche zeigt dafür jetzt `∞` und bietet explizite Checkboxen für unbegrenzte Actor- bzw. Tenant-Credits.

## RTX-4090-Startprofil

`config.rtx4090.example.json` enthält einen Startpunkt für eine RTX 4090 mit 24 GiB VRAM und beispielhaft 64 GiB System-RAM. Den RAM-Wert bitte an den tatsächlichen Host anpassen. Das Profil funktioniert unter Linux und Windows; beide Plattformen erfassen Host-RAM nativ, und `nvidia-smi` liefert die GPU-Telemetrie, sofern es im `PATH` verfügbar ist.

Für 24 GiB VRAM ist der wichtigste Unterschied zum 256-GB-Unified-Memory-Mac die Modell-spezifische Concurrency. Große ~17-GB-Modelle starten im Beispiel bei 1, kleine Modelle dürfen 2 parallele Slots erhalten.

## Priorität B – als nächste Ausbaustufe empfohlen

### Upstream Circuit Breaker + begrenzte Retries

Retries sind bei LLM-Streaming gefährlich, weil eine bereits begonnene Generierung nicht transparent wiederholt werden darf. Sinnvoll wäre deshalb:

- Retry nur vor dem ersten Response-Byte und nur bei eindeutig transienten Transportfehlern;
- Circuit Breaker pro Worker;
- Exponential Backoff bei Health-/Load-Fehlern;
- kein blindes Request Hedging, da es GPU-Arbeit dupliziert.

### API-Key Model ACLs

Pro API-Key/Tenant sollten erlaubte/verbotene Modell-Patterns möglich sein, z. B. `allowed_models: ["qwen3:*", "embeddinggemma:*"]`. Das ist besonders für teure oder unzensierte Modelle sinnvoll.

### Capability Routing / expliziter Fallback

Ein optionaler, explizit konfigurierter Fallback könnte Tool-Requests auf ein toolfähiges Modell umleiten. Default sollte `reject` bleiben, da ein stiller Modellwechsel semantisch überraschend ist.

### Persistentes Audit-Log als optionales Modul

Control-Plane-, Accounting- und Routing-Zustand ist inzwischen lokal persistent, während aktive Scheduling-/Socket-Zustände in-memory bleiben. Für Compliance wäre zusätzlich ein optionaler asynchroner Audit-Sink sinnvoll, der nur Admin-Aktionen und Metadaten schreibt und niemals Prompts/Antworten.

### OpenTelemetry

Prometheus deckt Aggregationen ab. Für verteilte Client-/Gateway-/Ollama-Latenz wäre optionales OTel Tracing sinnvoll, weiterhin ohne Prompt-Inhalte.

## Referenzen

- Ollama Show model details: https://docs.ollama.com/api-reference/show-model-details
- Ollama Tool calling: https://docs.ollama.com/capabilities/tool-calling
- Ollama Thinking: https://docs.ollama.com/capabilities/thinking
- Ollama Vision: https://docs.ollama.com/capabilities/vision
- Ollama Embeddings: https://docs.ollama.com/capabilities/embeddings
- OpenWebUI Ollama connection/context notes: https://docs.openwebui.com/getting-started/quick-start/connect-a-provider/starting-with-ollama/
- NVIDIA `nvidia-smi` selective query: https://docs.nvidia.com/deploy/nvidia-smi/


## Checkpoint 27 — Context / `num_ctx` Hardening

Der Context Guard unterscheidet jetzt zwischen dem theoretischen Modellmaximum (`/api/show model_info.*.context_length`) und dem tatsächlich nutzbaren Kontext eines Workers. Für Requests ohne explizites natives `options.num_ctx` gilt in dieser Reihenfolge: geladenes `/api/ps context_length`, Modelfile-`num_ctx`, `workers[].default_context_tokens`, dann der globale `model_capabilities.context.default_worker_tokens`. `workers[].context_limits` kann Modelle pro Worker zusätzlich begrenzen.

Native `options.num_ctx` muss ein positiver Integer sein. Es wird gegen Gateway-Cap, Modellmaximum und Worker-Cap geprüft. Ein bereits kleiner geladenes Modell bleibt für einen explizit größeren `num_ctx` grundsätzlich routbar, verliert aber den Loaded-Bonus, weil Ollama den KV-Cache neu allozieren muss.

Die Preflight-Schätzung berücksichtigt nun außerdem `max_output_tokens`, `instructions`, `suffix`, einen konfigurierbaren Vision-Reservewert pro Bild sowie eine Sicherheitsmarge. Die Modellansicht zeigt Modell-Maximum, Modelfile-Kontext und geladenen Kontext getrennt.
