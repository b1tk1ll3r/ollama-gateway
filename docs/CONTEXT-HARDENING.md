# Context / `num_ctx` hardening

Checkpoint 27 treats context size as a runtime resource rather than only a model capability.

## Effective context

For requests that cannot set native Ollama `options.num_ctx` (OpenAI `/v1/*`, Responses and Anthropic-compatible routes), each worker gets an effective context window from the strongest available evidence:

1. loaded `/api/ps` `context_length`;
2. Modelfile `num_ctx` returned by `/api/show` `parameters`;
3. `workers[].default_context_tokens`;
4. `model_capabilities.context.default_worker_tokens`.

The result is clamped by the theoretical model maximum and `workers[].context_limits`. The request is routed only to workers that can hold the estimated input + output budget. If no worker can hold it and `context_guard` is `reject`, admission fails before queueing or inference.

## Native `options.num_ctx`

`options.num_ctx` is accepted only on native Ollama `/api/*` requests. It must be a positive JSON integer. The gateway checks it against:

- `model_capabilities.context.max_requested_tokens` (default 32768; `-1` removes the gateway cap);
- the model maximum discovered from `/api/show`;
- a matching `workers[].context_limits` entry.

A model already loaded with a smaller context may still receive a larger explicit `num_ctx` because Ollama can resize/reload it, but that worker loses the loaded-model routing bonus for that request.

## Conservative request estimate

The context guard now includes:

- `prompt`, messages and `system`;
- Responses `instructions` and `input`;
- native generate `suffix`;
- tool schema JSON;
- `max_tokens`, `max_completion_tokens`, `max_output_tokens` and native `num_predict`;
- a configurable reserve per image (`vision_reserve_tokens_per_image`);
- a configurable input safety margin (`estimation_margin_percent`).

The estimator intentionally remains provider-independent and is not an exact tokenizer. The margin and image reserve compensate for that uncertainty.

## Recommended configuration

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

For a worker with a known different Ollama default or a stricter memory envelope:

```json
{
  "name": "worker-a",
  "default_context_tokens": 4096,
  "context_limits": {
    "large-*": 16384,
    "*": 32768
  }
}
```

Use a Modelfile `num_ctx` when OpenAI-compatible clients need a larger stable context, because those APIs do not provide native per-request `options.num_ctx` semantics.
