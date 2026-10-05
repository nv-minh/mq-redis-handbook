#!/usr/bin/env bash
# Validate infra/docker-compose.yml (all profiles). Usage: scripts/check-compose.sh [--no-manifest] [compose-file]
#   (a) every published port is bound to 127.0.0.1 (broker default credentials must not leak to the network)
#   (b) every service has a healthcheck (`up --wait` relies on it)
#   (c) every image has a manifest for linux/arm64 and linux/amd64 (Apple Silicon and CI)
# --no-manifest skips (c), which needs network access to the registry.
# A Docker Hub rate limit only warns, unless CHECK_COMPOSE_STRICT=1 (use that in CI after docker login).
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
check_manifests=1
if [ "${1:-}" = "--no-manifest" ]; then
  check_manifests=0
  shift
fi
compose_file="${1:-$ROOT/infra/docker-compose.yml}"
status=0

fail() {
  echo "check-compose: $1" >&2
  status=1
}

config_json="$(docker compose -f "$compose_file" --profile sentinel --profile cluster config --format json)" || {
  echo "check-compose: 'docker compose config' failed for $compose_file" >&2
  exit 1
}

# (a) + (b): validated on the rendered config, so YAML anchors and variable defaults are resolved.
problems="$(node -e '
  const cfg = JSON.parse(require("fs").readFileSync(0, "utf8"));
  for (const [name, svc] of Object.entries(cfg.services)) {
    for (const p of svc.ports ?? []) {
      if (p.host_ip !== "127.0.0.1") {
        console.log(`service ${name}: port ${p.published ?? "(random)"}:${p.target} is not bound to 127.0.0.1 (host_ip=${p.host_ip ?? "unset"})`);
      }
    }
    if (!svc.healthcheck || !svc.healthcheck.test) {
      console.log(`service ${name}: no healthcheck`);
    }
  }
' <<<"$config_json")"
if [ -n "$problems" ]; then
  while IFS= read -r line; do fail "$line"; done <<<"$problems"
fi

# (c) multi-arch manifests
if [ "$check_manifests" -eq 1 ]; then
  images="$(node -e '
    const cfg = JSON.parse(require("fs").readFileSync(0, "utf8"));
    console.log([...new Set(Object.values(cfg.services).map((s) => s.image))].join("\n"));
  ' <<<"$config_json")"
  # `docker manifest inspect` is slow (network round trip per image), so inspect in parallel.
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  n=0
  while IFS= read -r image; do
    [ -n "$image" ] || continue
    n=$((n + 1))
    printf '%s' "$image" >"$tmp/$n.image"
    (docker manifest inspect "$image" >"$tmp/$n.out" 2>&1; echo $? >"$tmp/$n.rc") &
  done <<<"$images"
  wait
  for ((i = 1; i <= n; i++)); do
    image="$(cat "$tmp/$i.image")"
    if [ "$(cat "$tmp/$i.rc")" != "0" ]; then
      if grep -q 'toomanyrequests' "$tmp/$i.out" && [ "${CHECK_COMPOSE_STRICT:-0}" != "1" ]; then
        # Docker Hub rate limit is not a defect of the compose file. CI sets CHECK_COMPOSE_STRICT=1.
        echo "warn: image $image: Docker Hub rate limit hit, multi-arch not verified (run 'docker login' or retry later)" >&2
      else
        fail "image $image: docker manifest inspect failed: $(cat "$tmp/$i.out")"
      fi
      continue
    fi
    missing="$(node -e '
      const m = JSON.parse(require("fs").readFileSync(0, "utf8"));
      const have = new Set((m.manifests ?? []).map((x) => `${x.platform.os}/${x.platform.architecture}`));
      console.log(["linux/arm64", "linux/amd64"].filter((p) => !have.has(p)).join(" "));
    ' <"$tmp/$i.out")"
    if [ -n "$missing" ]; then
      fail "image $image: no manifest for $missing"
    else
      echo "ok: $image has linux/arm64 and linux/amd64"
    fi
  done
fi

if [ "$status" -eq 0 ]; then
  echo "check-compose: ok"
fi
exit "$status"
