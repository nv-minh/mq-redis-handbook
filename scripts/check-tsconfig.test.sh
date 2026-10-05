#!/usr/bin/env bash
# Pins that tsconfig.json really covers lab files (NN-ten/lab-NN-ten/ts/*.ts).
# TypeScript include globs have no [0-9] classes, so a bad pattern silently skips every lab.
# Works in a temp dir; leaves nothing behind in the repo. Run: bash scripts/check-tsconfig.test.sh
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
failures=0

cp "$ROOT/tsconfig.json" "$TMP/tsconfig.json"
ln -s "$ROOT/node_modules" "$TMP/node_modules"
mkdir -p "$TMP/99-fixture/lab-01-x/ts"

tsc() { (cd "$TMP" && "$ROOT/node_modules/.bin/tsc" --noEmit -p tsconfig.json 2>&1); }

printf 'export const ok: number = 1;\n' >"$TMP/99-fixture/lab-01-x/ts/lab.ts"
if out="$(tsc)"; then
  echo "PASS: well-typed lab file compiles"
else
  echo "FAIL: well-typed lab file should compile"
  echo "$out" | sed 's/^/  | /'
  failures=$((failures + 1))
fi

printf 'export const bad: number = "not a number";\n' >"$TMP/99-fixture/lab-01-x/ts/bad.ts"
out="$(tsc)"
if [ $? -ne 0 ] && grep -q '99-fixture/lab-01-x/ts/bad.ts' <<<"$out"; then
  echo "PASS: type error in NN-ten/lab-NN-ten/ts/*.ts is reported by tsc"
else
  echo "FAIL: tsconfig.json does not cover lab files, tsc ignored a deliberate type error"
  echo "$out" | sed 's/^/  | /'
  failures=$((failures + 1))
fi

# Lab test files must be covered too.
rm "$TMP/99-fixture/lab-01-x/ts/bad.ts"
printf 'export const bad2: number = "x";\n' >"$TMP/99-fixture/lab-01-x/ts/lab.test.ts"
out="$(tsc)"
if [ $? -ne 0 ] && grep -q 'lab.test.ts' <<<"$out"; then
  echo "PASS: type error in lab.test.ts is reported by tsc"
else
  echo "FAIL: tsconfig.json does not cover lab test files"
  echo "$out" | sed 's/^/  | /'
  failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed"
  exit 1
fi
echo "all check-tsconfig tests passed"
