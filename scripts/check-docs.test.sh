#!/usr/bin/env bash
# Tests for scripts/check-docs.sh using throwaway fixtures. Run: bash scripts/check-docs.test.sh
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHECK="$ROOT/scripts/check-docs.sh"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

EM_DASH="$(printf '\xe2\x80\x94')"
failures=0

# make_topic <dir> : valid topic with theory.md and one lab README
make_topic() {
  local dir="$1"
  mkdir -p "$dir/lab-01-demo"
  cat >"$dir/theory.md" <<'MD'
# Theory

```mermaid
flowchart LR
  A --> B
```

```mermaid
sequenceDiagram
  A->>B: ok
  B--xA: failure
```

## Nguồn tham khảo

- https://example.com (v1.0)
MD
  cat >"$dir/lab-01-demo/README.md" <<'MD'
# Lab

```mermaid
flowchart LR
  X --> Y
```
MD
}

# expect <name> <expected-exit> <expected-output-substring-or-empty> -- <args...>
expect() {
  local name="$1" want="$2" substr="$3"
  shift 4
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

# (a) complete theory passes
make_topic "$TMP/ok/01-ok"
expect "complete topic exits 0" 0 "" -- "$TMP/ok/01-ok"

# (b) missing sequenceDiagram fails and names the file
make_topic "$TMP/b/02-noseq"
grep -v -e 'sequenceDiagram' -e 'A->>B' -e 'B--xA' "$TMP/b/02-noseq/theory.md" >"$TMP/b/t.md"
mv "$TMP/b/t.md" "$TMP/b/02-noseq/theory.md"
expect "missing sequenceDiagram exits 1 and names file" 1 "02-noseq/theory.md" -- "$TMP/b/02-noseq"

# (c) em dash fails
make_topic "$TMP/c/03-emdash"
printf 'Bad %s dash\n' "$EM_DASH" >>"$TMP/c/03-emdash/theory.md"
expect "em dash exits 1" 1 "03-emdash/theory.md" -- "$TMP/c/03-emdash"

# (d) missing sources section fails
make_topic "$TMP/d/04-nosrc"
grep -v 'Nguồn tham khảo' "$TMP/d/04-nosrc/theory.md" >"$TMP/d/t.md"
mv "$TMP/d/t.md" "$TMP/d/04-nosrc/theory.md"
expect "missing sources exits 1" 1 "04-nosrc/theory.md" -- "$TMP/d/04-nosrc"

# (e) lab README without a mermaid block fails
make_topic "$TMP/e/05-nolabdiagram"
printf '# Lab without diagram\n' >"$TMP/e/05-nolabdiagram/lab-01-demo/README.md"
expect "lab README without mermaid exits 1" 1 "lab-01-demo/README.md" -- "$TMP/e/05-nolabdiagram"

# (f) no arguments is not an error
expect "no topics exits 0" 0 "" --

# (g) use case README without a mermaid block fails and names the file
make_topic "$TMP/g/06-nousecasediagram"
mkdir -p "$TMP/g/06-nousecasediagram/usecase-01-demo"
printf '# Use case without diagram\n' >"$TMP/g/06-nousecasediagram/usecase-01-demo/README.md"
expect "usecase README without mermaid exits 1 and names file" 1 "usecase-01-demo/README.md" -- "$TMP/g/06-nousecasediagram"

# (h) use case README with a mermaid block passes
make_topic "$TMP/h/07-usecaseok"
mkdir -p "$TMP/h/07-usecaseok/usecase-01-demo"
cat >"$TMP/h/07-usecaseok/usecase-01-demo/README.md" <<'MD'
# Use case

```mermaid
flowchart LR
  X --> Y
```
MD
expect "usecase README with mermaid exits 0" 0 "" -- "$TMP/h/07-usecaseok"

# (i) a topic with no usecase directories is unaffected (case (a) already covers it, assert explicitly)
make_topic "$TMP/i/08-nousecase"
expect "topic without usecase directories exits 0" 0 "" -- "$TMP/i/08-nousecase"

if [ "$failures" -ne 0 ]; then
  echo "$failures test(s) failed"
  exit 1
fi
echo "all check-docs tests passed"
