#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_LOG="$ROOT_DIR/tests/perf_server.log"
CLIENTS="${CLIENTS:-8}"
OPS="${OPS:-1000}"
PIPELINE="${PIPELINE:-64}"
SERVER_PID=""

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
}

wait_for_server() {
    for _ in {1..50}; do
        if ! kill -0 "$SERVER_PID" 2>/dev/null; then
            echo "server exited during startup" >&2
            cat "$SERVER_LOG" >&2 || true
            return 1
        fi
        if bash -c 'exec 9<>/dev/tcp/127.0.0.1/8080' 2>/dev/null; then
            return 0
        fi
        sleep 0.1
    done

    echo "server did not start" >&2
    cat "$SERVER_LOG" >&2 || true
    return 1
}

cd "$ROOT_DIR"
if bash -c 'exec 9<>/dev/tcp/127.0.0.1/8080' 2>/dev/null; then
    echo "port 8080 is already in use; stop the existing server before perf-test" >&2
    exit 1
fi

./server >"$SERVER_LOG" 2>&1 &
SERVER_PID="$!"
trap cleanup EXIT
wait_for_server

tests/perf_test "$CLIENTS" "$OPS" "$PIPELINE"
