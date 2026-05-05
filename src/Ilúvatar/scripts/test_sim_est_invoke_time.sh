#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-8079}"
WORKER_CONFIG="${WORKER_CONFIG:-$ROOT_DIR/iluvatar_worker/src/worker.json}"
STARTUP_TIMEOUT_SEC="${STARTUP_TIMEOUT_SEC:-45}"
FQDN="${FQDN:-dummy-fqdn}"

if [[ ! -f "$WORKER_CONFIG" ]]; then
  echo "Worker config not found: $WORKER_CONFIG" >&2
  exit 1
fi

TMP_DIR="$(mktemp -d -t iluvatar-sim-est-XXXXXX)"
LOG_DIR="$TMP_DIR/logs"
TMP_CONFIG="$TMP_DIR/worker.sim.json"
WORKER_STDOUT="$TMP_DIR/worker.stdout.log"
mkdir -p "$LOG_DIR"

cleanup() {
  if [[ -n "${WORKER_PID:-}" ]] && kill -0 "$WORKER_PID" 2>/dev/null; then
    kill -INT "$WORKER_PID" 2>/dev/null || true
    sleep 1
    kill -TERM "$WORKER_PID" 2>/dev/null || true
  fi
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

# Ensure worker writes logs to a writable location for this test run.
sed -E 's#("directory"[[:space:]]*:[[:space:]]*")[^"]*(")#\1'"$LOG_DIR"'\2#' "$WORKER_CONFIG" > "$TMP_CONFIG"

(
  cd "$ROOT_DIR"
  cargo run --bin iluvatar_worker -- --sim --config "$TMP_CONFIG"
) >"$WORKER_STDOUT" 2>&1 &
WORKER_PID=$!

deadline=$((SECONDS + STARTUP_TIMEOUT_SEC))
until (echo >/dev/tcp/"$HOST"/"$PORT") >/dev/null 2>&1; do
  if ! kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "Sim worker exited before becoming ready. Log follows:" >&2
    cat "$WORKER_STDOUT" >&2
    exit 1
  fi
  if (( SECONDS >= deadline )); then
    echo "Timed out waiting for worker at $HOST:$PORT after ${STARTUP_TIMEOUT_SEC}s." >&2
    echo "Worker log follows:" >&2
    cat "$WORKER_STDOUT" >&2
    exit 1
  fi
  sleep 0.2
done

(
  cd "$ROOT_DIR"
  cargo run --bin iluvatar_worker_cli -- --host "$HOST" --port "$PORT" est-invoke-time --fqdn "$FQDN"
)
