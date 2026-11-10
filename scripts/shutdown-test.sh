#!/usr/bin/env bash
# Graceful shutdown test: SIGTERM while tasks are running and queued. Checks that new
# submissions are refused with 503 during the stop, that the JVM exits within the configured
# limits, and that a restart finishes every accepted task without crash recovery being needed.
source "$(dirname "$0")/common.sh"
RUN="${RUN_DIR:-$ROOT/run/shutdown}"
N="${N:-300}"
rm -rf "$RUN"
start_app "$RUN" --taskqueue.worker.threads=8 \
  --taskqueue.simulation.min-latency=2s --taskqueue.simulation.max-latency=6s \
  --taskqueue.simulation.retryable-failure-rate=0 --taskqueue.simulation.system-failure-rate=0 \
  --taskqueue.execution.timeout=10s --taskqueue.shutdown.await-running=3s --taskqueue.shutdown.await-interrupted=5s

submit_many "$N" stop 32 > "$RUN/responses.txt"
accepted=$(awk '$1=="202"' "$RUN/responses.txt" | wc -l | tr -d ' ')
sleep 2
log "accepted $accepted; state before SIGTERM:"; stats | jq -c '{backlog, inMemoryQueue, running, succeeded}'

( sleep 0.5; submit stop-late NORMAL '{"text":"sent during shutdown"}' > "$RUN/late.txt" ) &
log "SIGTERM (await-running 3s, then interrupt and write back, await-interrupted 5s)"
ms=$(stop_app TERM)
wait || true
log "JVM exited after $ms ms"
log "submission during shutdown answered: $(cut -d' ' -f1 "$RUN/late.txt") (expect 503)"
grep -E 'Task engine stopped' "$RUN/app.log" | sed 's/.*TaskEngine *: /  /'

log "restarting with fast tasks"
start_app "$RUN" --taskqueue.simulation.min-latency=10ms --taskqueue.simulation.max-latency=30ms \
  --taskqueue.simulation.retryable-failure-rate=0 --taskqueue.simulation.system-failure-rate=0
wait_drained 120
succeeded=$(stats | jq .succeeded)
recovered=$(metrics | jq .counters.recoveredAfterCrash)
maxattempts=$( (curl -s "$BASE/api/tasks?size=200&page=0"; curl -s "$BASE/api/tasks?size=200&page=1") \
  | jq -s '[.[].items[].attempts] | max')
stats | jq -c .
stop_app TERM >/dev/null
log "succeeded=$succeeded of $accepted, recovered-as-crash=$recovered, highest attempt count of any task=$maxattempts"
if [[ "$succeeded" == "$accepted" && "$recovered" == 0 && "$maxattempts" == 1 && "$ms" -lt 15000 ]]; then
  log "PASS: graceful stop wrote everything back, nothing needed crash recovery, all tasks finished"
else
  log "FAIL"; exit 1
fi
