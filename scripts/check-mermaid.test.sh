#!/usr/bin/env bash
# Tests for scripts/check-mermaid.sh using throwaway fixtures. Run: bash scripts/check-mermaid.test.sh
# Renders with the real mmdc (Chromium), so it needs `pnpm install` first. Set MERMAID_PUPPETEER_CONFIG on CI.
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECK="$ROOT/scripts/check-mermaid.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
failures=0

# expect <name> <want-exit> <substring-or-empty> <args...>
expect() {
  local name="$1" want="$2" substr="$3"
  shift 3
  local out got
  out="$(bash "$CHECK" "$@" 2>&1)"
  got=$?
  if [ "$got" -ne "$want" ]; then
    echo "FAIL: $name (exit $got, want $want)"
    echo "$out" | sed 's/^/  | /'
    failures=$((failures + 1))
  elif [ -n "$substr" ] && ! grep -qF -- "$substr" <<<"$out"; then
    echo "FAIL: $name (output missing '$substr')"
    echo "$out" | sed 's/^/  | /'
    failures=$((failures + 1))
  else
    echo "PASS: $name"
  fi
}

# (a) a file with only valid blocks passes
mkdir -p "$TMP/valid"
cat >"$TMP/valid/ok.md" <<'MD'
# Valid

```mermaid
flowchart LR
  A --> B
```

```mermaid
sequenceDiagram
  A->>B: ok
  B--xA: failure
```
MD
expect "valid blocks exit 0" 0 "" "$TMP/valid"

# (b) one valid and one invalid block: exit 1, and the invalid block is named as file:line (line of the opening fence)
mkdir -p "$TMP/mixed"
cat >"$TMP/mixed/bad.md" <<'MD'
# Mixed

```mermaid
flowchart LR
  A --> B
```

Text between blocks.

```mermaid
flowchart LR
  A --> [[[ not valid
  ??? -->
```
MD
expect "invalid block exits 1 and names file:line" 1 "bad.md:10" "$TMP/mixed"

# (c) a Markdown file without mermaid blocks passes, and other fenced languages are ignored
mkdir -p "$TMP/none"
cat >"$TMP/none/plain.md" <<'MD'
# No diagrams

```bash
echo "flowchart LR [[[ not mermaid"
```
MD
expect "markdown without mermaid blocks exits 0" 0 "" "$TMP/none"

# (d) a single file argument works, node_modules is skipped when walking a directory
mkdir -p "$TMP/walk/node_modules/x"
printf '```mermaid\nnot a diagram at all ???\n```\n' >"$TMP/walk/node_modules/x/bad.md"
cp "$TMP/valid/ok.md" "$TMP/walk/ok.md"
expect "node_modules is skipped" 0 "" "$TMP/walk"
expect "single file argument fails when its block is invalid" 1 "bad.md:1" "$TMP/walk/node_modules/x/bad.md"

# (e) no temp files are left behind
leftover="$(find "${TMPDIR:-/tmp}" -maxdepth 1 -name 'check-mermaid.*' 2>/dev/null | head -1)"
if [ -n "$leftover" ]; then
  echo "FAIL: temp files left behind: $leftover"
  failures=$((failures + 1))
else
  echo "PASS: no temp files left behind"
fi

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed"
  exit 1
fi
echo "all check-mermaid tests passed"
