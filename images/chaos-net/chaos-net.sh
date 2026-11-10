#!/bin/sh
# chaos-net — network fault helper for the Kafka Lab controlboard (see docs/adr/0003-broker-chaos.md).
#
# Runs as an ephemeral container inside the target pod's network namespace (needs NET_ADMIN).
# Applies one fault for DURATION seconds, then undoes it. It always cleans up:
#   - when DURATION is over,
#   - on SIGTERM/SIGINT/SIGHUP,
#   - when the controlboard health URL fails HEALTH_FAILURES times in a row (dead-man switch:
#     a crashed controlboard must never leave a broker partitioned).
# Ephemeral containers cannot be stopped from the API, so an early stop injects a second
# container with MODE=undo: it removes every lab rule in the namespace. The first one notices
# that its marker chain is gone and exits without touching anything (it never undoes the
# rules of a newer experiment).
#
# Environment:
#   ID              experiment id, [a-z0-9], first 12 chars are used (required except MODE=undo)
#   MODE            partition | loss | latency | undo     (default partition)
#   PORTS           tcp port range, iptables syntax       (default 9090:9093)
#   LOSS_PERCENT    1..100 for MODE=loss                  (default 30)
#   LATENCY_MS      delay for MODE=latency                (default 200)
#   DURATION        seconds                               (default 60, max 900)
#   HEALTH_URL      dead-man switch URL, empty = off      (default http://controlboard.control:8080/healthz)
#   HEALTH_EVERY    seconds between checks                (default 5)
#   HEALTH_FAILURES consecutive failures before self-revert (default 3)
#   IFACE           interface for tc netem                (default eth0)
set -u

MODE="${MODE:-partition}"
ID=$(echo "${ID:-manual}" | tr -cd 'a-z0-9' | cut -c1-12)
PORTS="${PORTS:-9090:9093}"
LOSS_PERCENT="${LOSS_PERCENT:-30}"
LATENCY_MS="${LATENCY_MS:-200}"
DURATION="${DURATION:-60}"
HEALTH_URL="${HEALTH_URL-http://controlboard.control:8080/healthz}"
HEALTH_EVERY="${HEALTH_EVERY:-5}"
HEALTH_FAILURES="${HEALTH_FAILURES:-3}"
IFACE="${IFACE:-eth0}"
CHAIN_IN="LCI$ID"
CHAIN_OUT="LCO$ID"

[ "$DURATION" -gt 900 ] && DURATION=900

log() { echo "chaos-net: $*"; }

# Prefer the nftables backend (the k3s host uses nf_tables); fall back to legacy.
IPT=iptables
if command -v iptables-nft >/dev/null 2>&1 && iptables-nft -L -n >/dev/null 2>&1; then
  IPT=iptables-nft
elif command -v iptables-legacy >/dev/null 2>&1 && iptables-legacy -L -n >/dev/null 2>&1; then
  IPT=iptables-legacy
fi

drop_chain() { # $1 parent, $2 chain
  while $IPT -D "$1" -j "$2" 2>/dev/null; do :; done
  $IPT -F "$2" 2>/dev/null
  $IPT -X "$2" 2>/dev/null
}

own() { $IPT -L "$CHAIN_IN" -n >/dev/null 2>&1; }

undo_own() {
  drop_chain INPUT "$CHAIN_IN"
  drop_chain OUTPUT "$CHAIN_OUT"
  [ "$MODE" = latency ] && tc qdisc del dev "$IFACE" root 2>/dev/null
  log "reverted"
}

undo_all() {
  for c in $($IPT -S 2>/dev/null | awk '$1 == "-N" && $2 ~ /^LC[IO]/ { print $2 }'); do
    case "$c" in
      LCI*) drop_chain INPUT "$c" ;;
      LCO*) drop_chain OUTPUT "$c" ;;
    esac
  done
  if tc qdisc show dev "$IFACE" 2>/dev/null | grep -q netem; then
    tc qdisc del dev "$IFACE" root 2>/dev/null
  fi
  log "all lab network faults removed"
}

if [ "$MODE" = undo ]; then
  undo_all
  exit 0
fi

finish() {
  if own; then undo_own; fi
  exit 0
}
trap finish TERM INT HUP

# Rules match traffic to and from the given ports on every interface except loopback, so the
# pod's own health checks (exec probes over localhost) keep working.
rules() { # $1 = extra match (e.g. statistic), may be empty
  for dir in dport sport; do
    $IPT -A "$CHAIN_IN" ! -i lo -p tcp --"$dir" "$PORTS" $1 -j DROP || return 1
    $IPT -A "$CHAIN_OUT" ! -o lo -p tcp --"$dir" "$PORTS" $1 -j DROP || return 1
  done
}

apply() {
  # The (possibly empty) chains double as the ownership marker of this experiment.
  $IPT -N "$CHAIN_IN" && $IPT -N "$CHAIN_OUT" || return 1
  $IPT -I INPUT 1 -j "$CHAIN_IN" && $IPT -I OUTPUT 1 -j "$CHAIN_OUT" || return 1
  case "$MODE" in
    partition)
      rules "" ;;
    loss)
      p=$(awk "BEGIN { printf \"%.4f\", $LOSS_PERCENT / 100 }")
      rules "-m statistic --mode random --probability $p" ;;
    latency)
      # tc netem needs sch_netem; the kernel loads it on demand. If that is impossible this
      # fails and the experiment reports it (services use the client-side fault dialer).
      tc qdisc add dev "$IFACE" root netem delay "${LATENCY_MS}ms" ;;
    *)
      log "unknown MODE $MODE"; return 1 ;;
  esac
}

if ! apply; then
  log "applying $MODE failed"
  undo_own
  exit 2
fi
log "applied id=$ID mode=$MODE ports=$PORTS duration=${DURATION}s backend=$IPT"

start=$(date +%s)
failures=0
while :; do
  [ $(($(date +%s) - start)) -ge "$DURATION" ] && break
  sleep "$HEALTH_EVERY" &
  wait $!
  if ! own; then
    log "undone externally"
    exit 0
  fi
  if [ -n "$HEALTH_URL" ]; then
    if wget -q -T 2 -O /dev/null "$HEALTH_URL" 2>/dev/null; then
      failures=0
    else
      failures=$((failures + 1))
      if [ "$failures" -ge "$HEALTH_FAILURES" ]; then
        log "controlboard unreachable ${failures}x, self-reverting"
        break
      fi
    fi
  fi
done
finish
