#!/usr/bin/env bash
# Stops the Kafka Lab.
#
#   ./stop_stack.sh          stop the cluster, keep all data (start again: ./start_stack.sh --fast)
#   ./stop_stack.sh --nuke   delete cluster, registry and persistent data (asks for confirmation)
set -euo pipefail

ROOT="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
cd "$ROOT"

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

case "${1:-}" in
  "")
    step "Stopping the cluster (data is kept)"
    make --no-print-directory down
    printf '\n\033[1;32mKafka Lab stopped.\033[0m Start again with: ./start_stack.sh --fast\n'
    ;;
  --nuke)
    read -r -p "Delete the cluster and ALL lab data? Type 'nuke' to confirm: " answer
    [[ "$answer" == "nuke" ]] || { echo "aborted"; exit 1; }
    step "Deleting cluster, registry and persistent data"
    make --no-print-directory nuke
    printf '\n\033[1;32mKafka Lab deleted.\033[0m Rebuild with: ./start_stack.sh\n'
    ;;
  *)
    echo "usage: $0 [--nuke]" >&2
    exit 2
    ;;
esac
