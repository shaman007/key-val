#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SERVER_LOG="$ROOT_DIR/tests/server_smoke.log"
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

assert_eq() {
    local expected="$1"
    local actual="$2"
    if [[ "$actual" != "$expected" ]]; then
        echo "expected: $expected" >&2
        echo "actual:   $actual" >&2
        cat "$SERVER_LOG" >&2 || true
        exit 1
    fi
}

cd "$ROOT_DIR"
if bash -c 'exec 9<>/dev/tcp/127.0.0.1/8080' 2>/dev/null; then
    echo "port 8080 is already in use; stop the existing server before smoke-test" >&2
    exit 1
fi

./server >"$SERVER_LOG" 2>&1 &
SERVER_PID="$!"
trap cleanup EXIT
wait_for_server

exec 3<>/dev/tcp/127.0.0.1/8080
printf 'wipe\n' >&3
IFS= read -r line <&3
assert_eq "All clean!" "$line"

printf 'write batched one\nsearch batched\n' >&3
IFS= read -r line <&3
assert_eq "OK" "$line"
IFS= read -r line <&3
[[ "$line" == Found:\ one,* ]] || {
    echo "expected batched search hit, got: $line" >&2
    exit 1
}

printf 'write split ' >&3
sleep 0.1
printf 'two 10\nsearch split\n' >&3
IFS= read -r line <&3
assert_eq "OK" "$line"
IFS= read -r line <&3
[[ "$line" == Found:\ two,* ]] || {
    echo "expected split search hit, got: $line" >&2
    exit 1
}
exec 3>&-

exec 4<>/dev/tcp/127.0.0.1/8080
printf '%*s\n' 5000 '' | tr ' ' x >&4
IFS= read -r line <&4
assert_eq "Error: command too long" "$line"
exec 4>&-

echo "server smoke tests passed"
