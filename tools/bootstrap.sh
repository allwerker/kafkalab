#!/usr/bin/env bash
# Installs the pinned toolchain into ./.tools (no sudo, nothing system-wide).
# Every download is verified against the checksum published by the project.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=versions.env
source "$ROOT/tools/versions.env"
TOOLS="$ROOT/.tools"
BIN="$TOOLS/bin"
DL="$TOOLS/downloads"
mkdir -p "$BIN" "$DL"

log() { printf '\033[36m[bootstrap]\033[0m %s\n' "$*"; }

verify() { # file expected_sha256
  local actual
  actual="$(sha256sum "$1" | awk '{print $1}')"
  if [[ "$actual" != "$2" ]]; then
    echo "checksum mismatch for $1: expected $2, got $actual" >&2
    rm -f "$1"
    exit 1
  fi
}

fetch() { # url dest
  [[ -s "$2" ]] || curl -fsSL --retry 3 -o "$2" "$1"
}

install_go() {
  if [[ -x "$TOOLS/go/bin/go" ]] && [[ "$("$TOOLS/go/bin/go" env GOVERSION)" == "go$GO_VERSION" ]]; then return; fi
  log "go $GO_VERSION"
  local file="go$GO_VERSION.linux-amd64.tar.gz" sha
  sha="$(curl -fsSL 'https://go.dev/dl/?mode=json&include=all' \
    | jq -r --arg f "$file" '.[].files[] | select(.filename==$f) | .sha256')"
  fetch "https://go.dev/dl/$file" "$DL/$file"
  verify "$DL/$file" "$sha"
  rm -rf "$TOOLS/go" && tar -C "$TOOLS" -xzf "$DL/$file"
}

install_kubectl() {
  if [[ -x "$BIN/kubectl" ]] && "$BIN/kubectl" version --client 2>/dev/null | grep -q "$KUBECTL_VERSION"; then return; fi
  log "kubectl $KUBECTL_VERSION"
  local base="https://dl.k8s.io/release/$KUBECTL_VERSION/bin/linux/amd64/kubectl"
  rm -f "$DL/kubectl"
  fetch "$base" "$DL/kubectl"
  verify "$DL/kubectl" "$(curl -fsSL "$base.sha256")"
  install -m 0755 "$DL/kubectl" "$BIN/kubectl"
}

install_helm() {
  if [[ -x "$BIN/helm" ]] && "$BIN/helm" version --short 2>/dev/null | grep -q "$HELM_VERSION"; then return; fi
  log "helm $HELM_VERSION"
  local file="helm-$HELM_VERSION-linux-amd64.tar.gz"
  fetch "https://get.helm.sh/$file" "$DL/$file"
  verify "$DL/$file" "$(curl -fsSL "https://get.helm.sh/$file.sha256sum" | awk '{print $1}')"
  tar -C "$DL" -xzf "$DL/$file" linux-amd64/helm
  install -m 0755 "$DL/linux-amd64/helm" "$BIN/helm"
}

install_helmfile() {
  if [[ -x "$BIN/helmfile" ]] && "$BIN/helmfile" --version 2>/dev/null | grep -q "$HELMFILE_VERSION"; then return; fi
  log "helmfile $HELMFILE_VERSION"
  local file="helmfile_${HELMFILE_VERSION}_linux_amd64.tar.gz"
  local base="https://github.com/helmfile/helmfile/releases/download/v$HELMFILE_VERSION"
  fetch "$base/$file" "$DL/$file"
  verify "$DL/$file" "$(curl -fsSL "$base/helmfile_${HELMFILE_VERSION}_checksums.txt" | awk -v f="$file" '$2==f {print $1}')"
  tar -C "$DL" -xzf "$DL/$file" helmfile
  install -m 0755 "$DL/helmfile" "$BIN/helmfile"
}

install_k3d() {
  if [[ -x "$BIN/k3d" ]] && "$BIN/k3d" version 2>/dev/null | grep -q "$K3D_VERSION"; then return; fi
  log "k3d $K3D_VERSION"
  local base="https://github.com/k3d-io/k3d/releases/download/$K3D_VERSION"
  rm -f "$DL/k3d-linux-amd64"
  fetch "$base/k3d-linux-amd64" "$DL/k3d-linux-amd64"
  verify "$DL/k3d-linux-amd64" "$(curl -fsSL "$base/checksums.txt" | awk '$2 ~ /k3d-linux-amd64$/ {print $1}')"
  install -m 0755 "$DL/k3d-linux-amd64" "$BIN/k3d"
}

install_avro_tools() {
  local jar="$TOOLS/avro-tools-$AVRO_TOOLS_VERSION.jar"
  [[ -s "$jar" ]] && return
  log "avro-tools $AVRO_TOOLS_VERSION"
  local url="https://repo1.maven.org/maven2/org/apache/avro/avro-tools/$AVRO_TOOLS_VERSION/avro-tools-$AVRO_TOOLS_VERSION.jar"
  fetch "$url" "$jar.tmp"
  # Maven Central publishes SHA-1 for every artifact; SHA-256 is not guaranteed.
  local expected actual
  expected="$(curl -fsSL "$url.sha1" | awk '{print $1}')"
  actual="$(sha1sum "$jar.tmp" | awk '{print $1}')"
  [[ "$expected" == "$actual" ]] || { echo "checksum mismatch for avro-tools" >&2; rm -f "$jar.tmp"; exit 1; }
  mv "$jar.tmp" "$jar"
}

install_go_tools() {
  export GOBIN="$BIN" GOPATH="$TOOLS/gopath" GOTOOLCHAIN=local PATH="$TOOLS/go/bin:$PATH"
  local -a pkgs=(
    "github.com/hamba/avro/v2/cmd/avrogen@v2.31.0"
    "github.com/vladopajic/go-test-coverage/v2@v2.23.0"
    "honnef.co/go/tools/cmd/staticcheck@v0.8.2"
  )
  for p in "${pkgs[@]}"; do
    local name="${p%@*}"; name="${name##*/}"; [[ "$name" == v2 ]] && name="go-test-coverage"
    [[ -x "$BIN/$name" ]] && continue
    log "go install $p"
    go install "$p"
  done
}

install_go
install_kubectl
install_helm
install_helmfile
install_k3d
install_avro_tools
install_go_tools
log "done. Tools in $BIN"
