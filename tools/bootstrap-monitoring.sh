#!/usr/bin/env bash
# Installs the monitoring test tools into ./.tools/bin (no sudo): vmalert-tool (from vmutils, same
# version as VictoriaMetrics) and promtool (Prometheus release). Used by `make alerts-test`.
# Every download is verified against the checksum file published with the release.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=versions.env
source "$ROOT/tools/versions.env"
TOOLS="$ROOT/.tools"
BIN="$TOOLS/bin"
DL="$TOOLS/downloads"
mkdir -p "$BIN" "$DL"

log() { printf '\033[36m[bootstrap-monitoring]\033[0m %s\n' "$*"; }

verify() { # file expected_sha256
  local actual
  actual="$(sha256sum "$1" | awk '{print $1}')"
  if [[ -z "$2" || "$actual" != "$2" ]]; then
    echo "checksum mismatch for $1: expected '$2', got $actual" >&2
    rm -f "$1"
    exit 1
  fi
}

fetch() { # url dest
  [[ -s "$2" ]] || curl -fsSL --retry 3 -o "$2" "$1"
}

install_vmalert_tool() {
  if [[ -x "$BIN/vmalert-tool" ]] && "$BIN/vmalert-tool" --version 2>/dev/null | grep -q "$VICTORIAMETRICS_VERSION"; then return; fi
  log "vmalert-tool $VICTORIAMETRICS_VERSION"
  local file="vmutils-linux-amd64-$VICTORIAMETRICS_VERSION.tar.gz"
  local base="https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/$VICTORIAMETRICS_VERSION"
  fetch "$base/$file" "$DL/$file"
  verify "$DL/$file" "$(curl -fsSL "$base/vmutils-linux-amd64-${VICTORIAMETRICS_VERSION}_checksums.txt" | awk -v f="$file" '$2==f {print $1}')"
  local tmp
  tmp="$(mktemp -d)"
  tar -C "$tmp" -xzf "$DL/$file" vmalert-tool-prod
  install -m 0755 "$tmp/vmalert-tool-prod" "$BIN/vmalert-tool"
  rm -rf "$tmp"
}

install_promtool() {
  if [[ -x "$BIN/promtool" ]] && "$BIN/promtool" --version 2>/dev/null | grep -q "version $PROMETHEUS_VERSION"; then return; fi
  log "promtool $PROMETHEUS_VERSION"
  local dir="prometheus-$PROMETHEUS_VERSION.linux-amd64"
  local file="$dir.tar.gz"
  local base="https://github.com/prometheus/prometheus/releases/download/v$PROMETHEUS_VERSION"
  fetch "$base/$file" "$DL/$file"
  verify "$DL/$file" "$(curl -fsSL "$base/sha256sums.txt" | awk -v f="$file" '$2==f {print $1}')"
  local tmp
  tmp="$(mktemp -d)"
  tar -C "$tmp" -xzf "$DL/$file" "$dir/promtool"
  install -m 0755 "$tmp/$dir/promtool" "$BIN/promtool"
  rm -rf "$tmp"
}

install_vmalert_tool
install_promtool
