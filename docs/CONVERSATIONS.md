# Optional encrypted Responses conversations

`conversations` implements the P2.3 stateful layer for clients that use the OpenAI Responses API with `previous_response_id`.

The feature is **disabled by default**. When disabled, the gateway does not parse, capture, persist, or reinterpret response content for conversation purposes; `/v1/responses` keeps its normal passthrough behavior.

## Configuration

```json
"conversations": {
  "enabled": false,
  "encryption_key": "${GATEWAY_CONVERSATION_KEY}",
  "retention": "24h",
  "max_entries": 1000,
  "max_content_bytes": 2097152
},
"storage": {
  "conversations_file": "conversations.enc.json"
}
```

When enabled:

- `encryption_key` must contain at least 32 characters. Use an environment variable or another deployment secret source; do not commit it.
- `retention` is independent from usage-journal retention because this store contains prompt/output content.
- `max_entries` bounds the number of stored response contexts; oldest entries are evicted first.
- `max_content_bytes` bounds one flattened conversation context and also bounds the response capture used to construct it.

The configured key is SHA-256-derived into an AES-256 key. The state snapshot is encrypted with AES-256-GCM and written mode `0600` through the same fsync + atomic-replace mechanism used by the other local state files. The on-disk JSON envelope contains only version/algorithm metadata, nonce, ciphertext, and update time; conversation content is not plaintext on disk.

Changing or losing the encryption key makes the existing store unreadable. Startup fails closed if an enabled store cannot be decrypted.

## `previous_response_id` behavior

For an enabled store, a successful `/v1/responses` request is stored when the request does not set `"store": false`.

For a later request containing `previous_response_id`:

1. the ID is looked up only inside the authenticated tenant + actor boundary;
2. the prior flattened input/output item context is loaded;
3. the current `input` is appended;
4. `previous_response_id` is removed before forwarding upstream;
5. the resulting complete `input` is sent through the normal ACL, preflight, quota, queue, placement, routing and proxy path.

A missing ID and an ID owned by another tenant/actor are intentionally indistinguishable to the caller. Both fail as an invalid previous response reference, preventing cross-identity enumeration.

String `input` values are normalized to a user message item when history must be flattened. Existing array/object inputs are retained as items. Previous `instructions` are not copied into the stored conversation context; only input/output items are chained.

## Streaming

The proxy normally does not retain response bodies. Conversation capture is activated only for `/v1/responses` when conversations are enabled and the request is eligible for storage.

For streaming Responses, the gateway reconstructs persisted output from `response.completed` (or completed output-item events when necessary). Client streaming remains unchanged. If the capture exceeds `max_content_bytes`, the inference response still succeeds but that response is not stored for future chaining.

## Retention and deletion

Expired entries are pruned on reads/writes and by a background cleanup loop. The cleanup interval is derived from the configured retention and capped at 15 minutes, so content does not depend on future traffic to expire.

`store: false` is the per-request opt-out. It still allows a request to consume a valid prior response context, but the new response is not persisted as the next link.

The store is intentionally not exposed as a content browser in the Admin UI. The persistence page reports only operational metadata such as whether it is enabled and the number of retained entries.

## Backups and threat model

The Admin storage backup includes the encrypted conversation state file when present. Treat backups as sensitive anyway: gateway configuration backups can contain deployment secrets, and possession of both the conversation ciphertext and its encryption key allows decryption.

This feature protects content at rest from casual filesystem disclosure. It does not protect content from a compromised running gateway process, from an administrator who possesses the configured key, or from the Ollama worker that receives the expanded prompt context.
