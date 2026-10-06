#!/usr/bin/env bash
# Check that every ```mermaid block in Markdown files parses and renders with mermaid-cli (mmdc).
# Usage: scripts/check-mermaid.sh [file-or-dir...]
#   Without arguments: every tracked *.md file, except docs/superpowers/ (plans and specs: they quote
#   diagrams as examples and are not part of the handbook). docs/research/ is included (it has no
#   diagrams today, so it costs nothing, and a diagram added there later is checked automatically).
#   A directory argument is searched recursively for *.md (node_modules is skipped).
# Exit 1 and print `file:line` (line of the opening fence) for every block that fails.
# Env:
#   MERMAID_PUPPETEER_CONFIG  path of a puppeteer config JSON passed to mmdc with -p.
#                             CI sets it to a file with {"args": ["--no-sandbox"]} (ubuntu runners need it).
#                             Locally it is unset and mmdc uses the Chromium that `pnpm install` downloaded.
#   MMDC                      mmdc command to use (default: node_modules/.bin/mmdc, else `mmdc` from PATH).
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -n "${MMDC:-}" ]; then
  mmdc_cmd="$MMDC"
elif [ -x "$ROOT/node_modules/.bin/mmdc" ]; then
  mmdc_cmd="$ROOT/node_modules/.bin/mmdc"
elif command -v mmdc >/dev/null 2>&1; then
  mmdc_cmd="mmdc"
else
  echo "check-mermaid: không tìm thấy mmdc, hãy chạy 'pnpm install' trước" >&2
  exit 1
fi

work="$(mktemp -d "${TMPDIR:-/tmp}/check-mermaid.XXXXXX")"
trap 'rm -rf "$work"' EXIT

# Collect the Markdown files to check.
files=()
if [ "$#" -eq 0 ]; then
  while IFS= read -r f; do
    case "$f" in docs/superpowers/*) continue ;; esac
    [ -f "$ROOT/$f" ] && files+=("$ROOT/$f")
  done < <(git -C "$ROOT" ls-files '*.md')
else
  for arg in "$@"; do
    if [ -d "$arg" ]; then
      while IFS= read -r f; do files+=("$f"); done < <(find "$arg" -name node_modules -prune -o -type f -name '*.md' -print | sort)
    elif [ -f "$arg" ]; then
      files+=("$arg")
    else
      echo "check-mermaid: không tìm thấy $arg" >&2
      exit 1
    fi
  done
fi

status=0
checked=0
fileno=0
for file in "${files[@]+"${files[@]}"}"; do
  fileno=$((fileno + 1))
  # Extract each mermaid block to $work/<fileno>.<n>.mmd and list "<n> <opening-fence-line>" in $work/<fileno>.index.
  # Fences of other languages are tracked too, so a "```mermaid" quoted inside another block is not extracted.
  awk -v dir="$work" -v idx="$fileno" '
    function close_block() { close(out); inblock = 0 }
    !infence && /^[ \t]*```mermaid[ \t]*$/ {
      infence = 1; inblock = 1; n++
      out = sprintf("%s/%s.%d.mmd", dir, idx, n)
      printf "" > out
      print n, NR > (dir "/" idx ".index")
      next
    }
    !infence && /^[ \t]*```/ { infence = 1; next }
    infence && /^[ \t]*```[ \t]*$/ { if (inblock) close_block(); infence = 0; next }
    inblock { print > out }
    END { if (inblock) { close_block(); print "unterminated" > (dir "/" idx ".unterminated") } }
  ' "$file"

  if [ -f "$work/$fileno.unterminated" ]; then
    echo "check-mermaid: $file: khối mermaid không có dấu đóng \`\`\`" >&2
    status=1
  fi
  [ -f "$work/$fileno.index" ] || continue

  while read -r n line; do
    checked=$((checked + 1))
    block="$work/$fileno.$n.mmd"
    args=(-q -i "$block" -o "$block.svg")
    [ -n "${MERMAID_PUPPETEER_CONFIG:-}" ] && args+=(-p "$MERMAID_PUPPETEER_CONFIG")
    if ! out="$($mmdc_cmd "${args[@]}" 2>&1)"; then
      echo "check-mermaid: $file:$line: khối mermaid không parse được" >&2
      # The first lines carry the parser error; the rest is a puppeteer stack trace.
      echo "$out" | head -n 6 | sed 's/^/  | /' >&2
      status=1
    fi
  done <"$work/$fileno.index"
done

if [ "$status" -eq 0 ]; then
  echo "check-mermaid: ok ($checked khối trong ${#files[@]} file)"
fi
exit "$status"
