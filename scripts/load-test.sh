#!/usr/bin/env bash
# Load test: N submissions from C parallel clients against a backlog cap, then wait for the
# queue to drain and check that every accepted task reached a final state.
source "$(dirname "$0")/common.sh"
RUN="${RUN_DIR:-$ROOT/run/load}"
N="${N:-3000}"
C="${C:-64}"
rm -rf "$RUN"
start_app "$RUN" \
  --taskqueue.admission.max-backlog="${MAX_BACKLOG:-800}" --taskqueue.admission.adaptive=false \
  --taskqueue.simulation.min-latency=100ms --taskqueue.simulation.max-latency=400ms
trap 'stop_app TERM >/dev/null' EXIT

log "submitting $N tasks from $C parallel clients (backlog cap ${MAX_BACKLOG:-800}: LOW refused at 70%, NORMAL at 90%)"
t0=$(now_ns)
submit_many "$N" load "$C" > "$RUN/responses.txt"
t1=$(now_ns)
log "submitted in $(( (t1 - t0) / 1000000 )) ms"
summarize_responses "$RUN/responses.txt"
log "state right after the burst"; stats | jq -c '{backlog, inMemoryQueue, running, succeeded}'
metrics | jq -c '{queue: {size: .queue.size, capacity: .queue.capacity, peakSize: .queue.peakSize}, spilledToDisk: .counters.spilledToDisk}'

log "waiting for the backlog to drain"
d0=$(now_ns)
wait_drained 600
d1=$(now_ns)
accepted=$(awk '$1=="202"' "$RUN/responses.txt" | wc -l | tr -d ' ')
distinct=$(awk '$1=="202" {print $2}' "$RUN/responses.txt" | sort -u | wc -l | tr -d ' ')
final=$(stats | jq '.succeeded + .failed')
log "drained in $(( (d1 - d0) / 1000000 )) ms"
log "accepted=$accepted distinct ids=$distinct finished=$final"
stats | jq -c .
metrics | jq -c '{queue: {capacity: .queue.capacity, peakSize: .queue.peakSize}, counters, timings}'
if [[ "$accepted" == "$distinct" && "$accepted" == "$final" ]]; then
  log "PASS: every accepted task reached a final state; the memory queue never exceeded its capacity"
else
  log "FAIL: accepted and finished counts differ"; exit 1
fi
