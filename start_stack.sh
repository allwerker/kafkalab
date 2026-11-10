#!/usr/bin/env bash
# Starts the Kafka Lab and prints a short tutorial.
#
#   ./start_stack.sh          full start: toolchain, cluster, images, all Helm releases
#   ./start_stack.sh --fast   only start an existing (stopped) cluster, skip build and deploy
#   PROFILE=lite ./start_stack.sh   single broker, no logs/traces (~5 GB RAM)
set -euo pipefail

ROOT="$(cd "$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")" && pwd)"
cd "$ROOT"
PORT="${LB_PORT:-8080}"
FAST=false
[[ "${1:-}" == "--fast" ]] && FAST=true

step() { printf '\n\033[1;36m==> %s\033[0m\n' "$*"; }

step "Toolchain (pinned, project-local in .tools/)"
make --no-print-directory bootstrap

if $FAST; then
  step "Starting the existing cluster"
  make --no-print-directory cluster
else
  step "Cluster, images and all releases (first run ≈ 10 min)"
  make --no-print-directory up
fi

step "Waiting for the controlboard"
for _ in $(seq 1 90); do
  if curl -sf -o /dev/null -H 'Host: board.localhost' "http://127.0.0.1:${PORT}/healthz"; then break; fi
  sleep 2
done
curl -sf -o /dev/null -H 'Host: board.localhost' "http://127.0.0.1:${PORT}/healthz" \
  && echo "controlboard is up" || echo "controlboard not reachable yet — check with: make status"

cat <<EOF

$(printf '\033[1;32m')Kafka Lab is running.$(printf '\033[0m')

  Controlboard   http://board.localhost:${PORT}
  Grafana        http://grafana.localhost:${PORT}   (admin/admin, anonymous viewer works)
  AKHQ           http://akhq.localhost:${PORT}
  VictoriaMetrics http://vm.localhost:${PORT}/vmui
  VictoriaLogs   http://logs.localhost:${PORT}/select/vmui
  (all bound to 127.0.0.1; the Launchpad tab in the controlboard links everything)

Short tutorial (≈ 15 min, full version: docs/guided-tour.md)

  1. Look around   Open the controlboard. Orders flow from the generator through payment and
                   inventory to shipping; each lane is a topic partition, piles mean lag.
                   In Grafana open "Kafka (bundle) / Kafka Cluster Overview".

  2. Black Friday  Scenarios tab → Black Friday → Start (1,000 orders/s).
                   Watch parcels pile up, then Grafana "Consumer Groups & Lag":
                   lag per partition, estimated time behind, consume vs produce rate.
                   The board reverts automatically and shows a report.

  3. Outage        Start "Payment Provider Down" (90 s). See "requested vs observed" on the
                   Experiments tab, the alert in the alert panel, and lost revenue in
                   Grafana "Kafka Lab / Commerce Business".

  4. Broker chaos  Arm chaos in the top bar, start "Kafka Broker Failure", type the scenario
                   id to confirm. A broker rack goes dark; check "KRaft Controller & Quorum"
                   and under-replicated partitions. PANIC reverts everything at once.

  5. Play          Tuning Lab (acks, linger, compression), Incident Drill (find the hidden
                   failure), click a parcel for its trace and logs.

Useful commands
  ./kctl get pods -A     kubectl for this cluster, from any directory
  ./kctl lab status      pods and URLs        ./kctl lab groups   consumer groups
  make footprint         memory per namespace
  make down              stop (keeps data)    ./start_stack.sh --fast   start again
  make nuke              delete everything
EOF
