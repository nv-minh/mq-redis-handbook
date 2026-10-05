#!/usr/bin/env bash
# Fail if a lab test uses a fixed sleep. Usage: scripts/check-no-sleep.sh [repo-root]
# Scope: <root>/NN-*/**/*.test.ts and <root>/NN-*/**/*_test.go (not demo files, not packages/testkit).
# Waiting must be polling with a timeout: testkit `eventually` / `Eventually`.
# A line carrying the marker `allow-sleep` is exempt (a deliberate delay that is the scenario itself).
set -u

ROOT="${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
status=0

# time.Sleep(, setTimeout(, sleep( (also covers `await sleep(`), and the timers/promises module.
PATTERN='(time\.Sleep|setTimeout|(^|[^[:alnum:]_])sleep)[[:space:]]*\(|timers/promises'

for topic in "$ROOT"/[0-9][0-9]-*/; do
  [ -d "$topic" ] || continue
  while IFS= read -r file; do
    hits="$(grep -nE "$PATTERN" "$file" | grep -v 'allow-sleep' || true)"
    if [ -n "$hits" ]; then
      while IFS= read -r hit; do
        echo "check-no-sleep: ${file#"$ROOT"/}:${hit}" >&2
      done <<<"$hits"
      status=1
    fi
  done < <(find "$topic" -type f \( -name '*.test.ts' -o -name '*_test.go' \) -not -path '*/node_modules/*')
done

if [ "$status" -ne 0 ]; then
  echo "check-no-sleep: use eventually()/Eventually() from the testkit instead of fixed sleeps" >&2
fi
exit "$status"
