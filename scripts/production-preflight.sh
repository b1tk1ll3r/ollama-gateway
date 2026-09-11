#!/bin/sh
set -eu

CONFIG=${GATEWAY_CONFIG:-./gateway-config.json}
COMPOSE_FILE=${COMPOSE_FILE:-docker-compose.production.yml}
PUBLISH_ADDRESS=${GATEWAY_PUBLISH_ADDRESS:-127.0.0.1}
PUBLISH_PORT=${GATEWAY_PUBLISH_PORT:-9080}
ALLOW_NON_LOOPBACK_BIND=${ALLOW_NON_LOOPBACK_BIND:-0}

fail() {
  printf 'production preflight: ERROR: %s\n' "$*" >&2
  exit 1
}

warn() {
  printf 'production preflight: WARN: %s\n' "$*" >&2
}

command -v docker >/dev/null 2>&1 || fail 'docker is not available'
docker compose version >/dev/null 2>&1 || fail 'docker compose is not available'
[ -r "$CONFIG" ] || fail "bootstrap config is not readable: $CONFIG"

case "$CONFIG" in
  *config.example.json|*/config.example.json)
    fail 'GATEWAY_CONFIG points at config.example.json; select the production bootstrap config explicitly'
    ;;
esac

case "$PUBLISH_ADDRESS" in
  127.0.0.1|::1|localhost)
    ;;
  0.0.0.0|::|'[::]')
    [ "$ALLOW_NON_LOOPBACK_BIND" = 1 ] || fail "gateway would be published on all interfaces ($PUBLISH_ADDRESS:$PUBLISH_PORT); bind to loopback or set ALLOW_NON_LOOPBACK_BIND=1 only with an external firewall/ACL"
    warn "non-loopback wildcard publishing explicitly allowed: $PUBLISH_ADDRESS:$PUBLISH_PORT"
    ;;
  *)
    [ "$ALLOW_NON_LOOPBACK_BIND" = 1 ] || fail "gateway would be published on non-loopback address $PUBLISH_ADDRESS:$PUBLISH_PORT; use loopback for a host-local reverse proxy, or set ALLOW_NON_LOOPBACK_BIND=1 after restricting the host firewall to the real proxy"
    warn "non-loopback publishing explicitly allowed: $PUBLISH_ADDRESS:$PUBLISH_PORT"
    ;;
esac

export GATEWAY_CONFIG="$CONFIG" GATEWAY_PUBLISH_ADDRESS="$PUBLISH_ADDRESS" GATEWAY_PUBLISH_PORT="$PUBLISH_PORT"

docker compose -f "$COMPOSE_FILE" config >/dev/null

printf 'production preflight: compose syntax OK\n'
printf 'production preflight: published endpoint %s:%s -> container :8080\n' "$PUBLISH_ADDRESS" "$PUBLISH_PORT"

# Runs the candidate image with the exact bootstrap/state mounts and runtime user.
# The gateway preflight output is secret-redacted by design.
docker compose -f "$COMPOSE_FILE" run --rm --no-deps gateway \
  -config /etc/ollama-gateway/config.json -check-config

printf 'production preflight: OK\n'
