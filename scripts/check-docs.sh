#!/usr/bin/env bash
# Validate handbook docs. Usage: scripts/check-docs.sh <topic-dir>...
# Per topic dir:
#   - every lab-*/README.md has a ```mermaid block
#   - theory.md has a flowchart, a sequenceDiagram and a "## Nguồn tham khảo" section
#   - no .md file contains an em dash
# Exits 1 and prints the offending file for every violation. No arguments is not an error.
set -u

EM_DASH="$(printf '\xe2\x80\x94')"
status=0

fail() {
  echo "check-docs: $1: $2" >&2
  status=1
}

for dir in "$@"; do
  dir="${dir%/}"
  if [ ! -d "$dir" ]; then
    fail "$dir" "not a directory"
    continue
  fi

  # Lab READMEs need at least one mermaid diagram.
  for readme in "$dir"/lab-*/README.md; do
    [ -e "$readme" ] || continue
    grep -q '^```mermaid' "$readme" || fail "$readme" "missing a \`\`\`mermaid block"
  done

  theory="$dir/theory.md"
  if [ ! -f "$theory" ]; then
    fail "$theory" "file not found"
  else
    grep -Eq '^[[:space:]]*flowchart([[:space:]]|$)' "$theory" || fail "$theory" "missing flowchart diagram"
    grep -Eq '^[[:space:]]*sequenceDiagram([[:space:]]|$)' "$theory" || fail "$theory" "missing sequenceDiagram"
    grep -q '^## Nguồn tham khảo' "$theory" || fail "$theory" "missing '## Nguồn tham khảo' section"
  fi

  while IFS= read -r md; do
    if grep -qF -- "$EM_DASH" "$md"; then
      fail "$md" "contains an em dash, use '-' instead"
    fi
  done < <(find "$dir" -type f -name '*.md')
done

exit "$status"
