#!/bin/sh
set -eu

ROOT=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT HUP INT TERM
mkdir -p "$TMP/bin"

cat > "$TMP/bin/docker" <<'EOF'
#!/bin/sh
set -eu
[ "$1" = compose ] || exit 64
shift
# Accept optional -f <file>.
if [ "${1:-}" = -f ]; then shift 2; fi
case "${1:-}" in
  version) exit 0 ;;
  config) exit 0 ;;
  run)
    cat <<JSON
{"status":"ok","workers":2,"storage_writable":true}
JSON
    exit 0
    ;;
  *) exit 65 ;;
esac
EOF
chmod +x "$TMP/bin/docker"

cat > "$TMP/gateway-config.json" <<'EOF'
{}
EOF
cat > "$TMP/config.example.json" <<'EOF'
{}
EOF

PATH="$TMP/bin:$PATH" \
GATEWAY_CONFIG="$TMP/gateway-config.json" \
GATEWAY_PUBLISH_ADDRESS=127.0.0.1 \
COMPOSE_FILE="$ROOT/docker-compose.production.yml" \
"$ROOT/scripts/production-preflight.sh" > "$TMP/ok.out" 2> "$TMP/ok.err"
grep -q 'production preflight: OK' "$TMP/ok.out"

if PATH="$TMP/bin:$PATH" \
  GATEWAY_CONFIG="$TMP/config.example.json" \
  COMPOSE_FILE="$ROOT/docker-compose.production.yml" \
  "$ROOT/scripts/production-preflight.sh" >/dev/null 2>&1; then
  echo 'expected config.example.json rejection' >&2
  exit 1
fi

if PATH="$TMP/bin:$PATH" \
  GATEWAY_CONFIG="$TMP/gateway-config.json" \
  GATEWAY_PUBLISH_ADDRESS=0.0.0.0 \
  COMPOSE_FILE="$ROOT/docker-compose.production.yml" \
  "$ROOT/scripts/production-preflight.sh" >/dev/null 2>&1; then
  echo 'expected wildcard bind rejection' >&2
  exit 1
fi

PATH="$TMP/bin:$PATH" \
GATEWAY_CONFIG="$TMP/gateway-config.json" \
GATEWAY_PUBLISH_ADDRESS=10.2.10.20 \
ALLOW_NON_LOOPBACK_BIND=1 \
COMPOSE_FILE="$ROOT/docker-compose.production.yml" \
"$ROOT/scripts/production-preflight.sh" > "$TMP/remote.out" 2> "$TMP/remote.err"
grep -q 'production preflight: OK' "$TMP/remote.out"
grep -q 'non-loopback publishing explicitly allowed' "$TMP/remote.err"

echo 'production preflight tests: PASS'
