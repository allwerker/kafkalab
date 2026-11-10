#!/usr/bin/env bash
# Checks host prerequisites before the cluster is created. Exit code 1 on hard failures.
set -uo pipefail

fail=0
ok()   { printf '  \033[32m✔\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m!\033[0m %s\n' "$*"; }
bad()  { printf '  \033[31m✘\033[0m %s\n' "$*"; fail=1; }

echo "preflight:"
if docker info >/dev/null 2>&1; then ok "docker daemon reachable"; else bad "docker daemon not reachable (is your user in the docker group?)"; fi

inst="$(cat /proc/sys/fs/inotify/max_user_instances)"
if (( inst >= 512 )); then ok "fs.inotify.max_user_instances=$inst"
else warn "fs.inotify.max_user_instances=$inst (<512); run: sudo sysctl -w fs.inotify.max_user_instances=1024"; fi

watches="$(cat /proc/sys/fs/inotify/max_user_watches)"
if (( watches >= 65536 )); then ok "fs.inotify.max_user_watches=$watches"; else warn "fs.inotify.max_user_watches=$watches is low"; fi

mem_gb="$(awk '/MemAvailable/ {printf "%d", $2/1024/1024}' /proc/meminfo)"
if (( mem_gb >= 12 )); then ok "available memory ${mem_gb} GB"; else warn "available memory ${mem_gb} GB (<12 GB; consider PROFILE=lite)"; fi

disk_gb="$(df -BG --output=avail "${HOME}" | tail -1 | tr -dc '0-9')"
if (( disk_gb >= 40 )); then ok "free disk ${disk_gb} GB"; else bad "free disk ${disk_gb} GB (<40 GB)"; fi

port="${LB_PORT:-8080}"
if ss -ltn | awk '{print $4}' | grep -qE "[:.]${port}\$"; then
  if docker ps --format '{{.Names}} {{.Ports}}' | grep -q "k3d-.*${port}->"; then ok "port ${port} held by k3d"; else bad "port ${port} is in use by another process"; fi
else ok "port ${port} free"; fi

exit "$fail"
