#!/usr/bin/env bash
# Tests for scripts/check-compose.sh (offline, --no-manifest). Run: bash scripts/check-compose.test.sh
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECK="$ROOT/scripts/check-compose.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
failures=0

write_compose() { # <file> <port-entry> <with-healthcheck: yes|no>
  {
    echo "name: check-compose-fixture"
    echo "services:"
    echo "  web:"
    echo "    image: busybox:1.37"
    echo "    ports:"
    echo "      - \"$2\""
    if [ "$3" = "yes" ]; then
      echo "    healthcheck:"
      echo "      test: [\"CMD\", \"true\"]"
    fi
  } >"$1"
}

expect() { # <name> <want-exit> <substring> <file>
  local out got
  out="$(bash "$CHECK" --no-manifest "$4" 2>&1)"
  got=$?
  if [ "$got" -ne "$2" ]; then
    echo "FAIL: $1 (exit $got, want $2)"
    echo "$out" | sed 's/^/  | /'
    failures=$((failures + 1))
  elif [ -n "$3" ] && ! grep -qF -- "$3" <<<"$out"; then
    echo "FAIL: $1 (output missing '$3')"
    echo "$out" | sed 's/^/  | /'
    failures=$((failures + 1))
  else
    echo "PASS: $1"
  fi
}

write_compose "$TMP/ok.yml" "127.0.0.1:8080:80" yes
expect "loopback port with healthcheck passes" 0 "" "$TMP/ok.yml"

write_compose "$TMP/open.yml" "8080:80" yes
expect "port without 127.0.0.1 prefix fails" 1 "not bound to 127.0.0.1" "$TMP/open.yml"

write_compose "$TMP/wide.yml" "0.0.0.0:8080:80" yes
expect "port bound to 0.0.0.0 fails" 1 "not bound to 127.0.0.1" "$TMP/wide.yml"

write_compose "$TMP/nohc.yml" "127.0.0.1:8080:80" no
expect "service without healthcheck fails" 1 "no healthcheck" "$TMP/nohc.yml"

expect "real infra compose passes offline checks" 0 "" "$ROOT/infra/docker-compose.yml"

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed"
  exit 1
fi
echo "all check-compose tests passed"
