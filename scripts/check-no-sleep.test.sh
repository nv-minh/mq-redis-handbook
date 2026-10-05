#!/usr/bin/env bash
# Tests for scripts/check-no-sleep.sh using throwaway fixtures. Run: bash scripts/check-no-sleep.test.sh
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECK="$ROOT/scripts/check-no-sleep.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
failures=0

# fixture <name> <relative-file> <content> : creates $TMP/<name>/<relative-file>
fixture() {
  mkdir -p "$TMP/$1/$(dirname "$2")"
  printf '%s\n' "$3" >"$TMP/$1/$2"
}

expect() { # <name> <want-exit> <substring> <root>
  local out got
  out="$(bash "$CHECK" "$4" 2>&1)"
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

LAB="01-x/lab-01-x"

fixture clean "$LAB/ts/lab.test.ts" 'const v = await eventually(async () => ready(), { timeoutMs: 5000 });'
fixture clean "$LAB/go/lab_test.go" 'v := testkit.Eventually(t, 5*time.Second, func() (int, bool) { return 1, true })'
expect "clean TS and Go tests pass" 0 "" "$TMP/clean"

fixture ts-settimeout "$LAB/ts/lab.test.ts" 'await new Promise((r) => setTimeout(r, 500));'
expect "setTimeout in TS test fails and names file" 1 "lab.test.ts:1" "$TMP/ts-settimeout"

fixture ts-sleep "$LAB/ts/lab.test.ts" 'await sleep(500);'
expect "sleep() in TS test fails" 1 "lab.test.ts:1" "$TMP/ts-sleep"

fixture ts-promises "$LAB/ts/lab.test.ts" 'import { setTimeout as wait } from "node:timers/promises";'
expect "timers/promises import in TS test fails" 1 "lab.test.ts:1" "$TMP/ts-promises"

fixture go-sleep "$LAB/go/lab_test.go" '	time.Sleep(500 * time.Millisecond)'
expect "time.Sleep in Go test fails and names file" 1 "lab_test.go:1" "$TMP/go-sleep"

fixture allowed "$LAB/ts/lab.test.ts" 'await sleep(5); // allow-sleep: simulating slow consumer on purpose'
expect "allow-sleep marker is honored" 0 "" "$TMP/allowed"

# Non-test files and non-lab directories are out of scope (demo.ts may simulate latency).
fixture scope "$LAB/ts/demo.ts" 'await sleep(500);'
fixture scope "packages/testkit-ts/src/index.test.ts" 'setTimeout(() => {}, 100);'
fixture scope "node_modules/x/a.test.ts" 'setTimeout(() => {}, 100);'
expect "demo files, packages and node_modules are out of scope" 0 "" "$TMP/scope"

expect "real repo has no fixed sleeps in lab tests" 0 "" "$ROOT"

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed"
  exit 1
fi
echo "all check-no-sleep tests passed"
