#!/usr/bin/env bash
# Crash test: kill -9 the JVM while clients are still submitting and workers are running.
# Then restart on the same H2 files and check:
#   1. every submission that got 202 before the crash still exists (the accept boundary holds)
#   2. tasks left RUNNING by the dead process are recovered and finish
#   3. clients retrying every request with the same business key (and again after a 429, as
#      Retry-After asks) get the same task id back for anything already accepted, so nothing is
#      lost and nothing runs as two tasks
source "$(dirname "$0")/common.sh"
RUN="${RUN_DIR:-$ROOT/run/crash}"
N="${N:-8000}"
rm -rf "$RUN"
ARGS=(--taskqueue.simulation.min-latency=200ms --taskqueue.simulation.max-latency=600ms)
# Extra app settings, e.g. EXTRA_ARGS="--taskqueue.admission.max-backlog=1000" to force backpressure.
read -r -a EXTRA <<< "${EXTRA_ARGS:-}"
ARGS+=("${EXTRA[@]}")
start_app "$RUN" "${ARGS[@]}"

log "submitting $N tasks from 32 clients in the background"
submit_many "$N" crash 32 > "$RUN/responses-before.txt" &
SUBMITTER=$!
sleep "${KILL_AFTER:-2}"
log "state just before the crash"; stats | jq -c '{backlog, running, succeeded}'
log "kill -9 $APP_PID"
stop_app KILL >/dev/null
wait "$SUBMITTER" || true
accepted=$(awk '$1=="202"' "$RUN/responses-before.txt" | wc -l | tr -d ' ')
lost_conn=$(awk '$1=="000"' "$RUN/responses-before.txt" | wc -l | tr -d ' ')
log "before the crash: $accepted accepted (202), $lost_conn requests got no answer"

log "restarting on the same data directory"
start_app "$RUN" "${ARGS[@]}"
grep -E 'Recovered [0-9]+ task' "$RUN/app.log" | tail -1 | sed 's/.*TaskEngine *: /  /' || log "nothing was RUNNING at the crash"

log "check 1: every 202 from before the crash exists"
missing=$(awk '$1=="202" {print $2}' "$RUN/responses-before.txt" \
  | xargs -P 32 -I{} sh -c 'c=$(curl -s -o /dev/null -w "%{http_code}" "$0/api/tasks/{}"); [ "$c" = 200 ] || echo {}' "$BASE" | wc -l | tr -d ' ')
log "  missing after restart: $missing of $accepted"

log "check 2: clients retry all $N requests with the same business keys"
submit_many "$N" crash 32 > "$RUN/responses-retry.txt"
cp "$RUN/responses-retry.txt" "$RUN/responses-retry-all.txt"
summarize_responses "$RUN/responses-retry.txt" | grep 'HTTP'
# The retry burst can itself hit backpressure (429). Like a real client, wait and send those again.
retry_refused "$RUN/responses-retry.txt" "$RUN/responses-retry-all.txt"
log "  answers over all retry rounds:"
summarize_responses "$RUN/responses-retry-all.txt" | grep 'HTTP'
changed=$(join <(awk '$1=="202" {print $3, $2}' "$RUN/responses-before.txt" | sort) \
               <(awk '$1=="200" || $1=="202" {print $3, $2}' "$RUN/responses-retry-all.txt" | sort -u) \
          | awk '$2 != $3' | wc -l | tr -d ' ')
log "  accepted-before-crash keys answered with a different task id: $changed"

log "check 3: wait for everything to finish"
wait_drained 600
final=$(stats | jq '.succeeded + .failed')
log "  tasks in the database: $final (expected $N, one per business key)"
stats | jq -c .
metrics | jq -c '.counters | {submitted, duplicates, succeeded, failed, deadLettered, retries, recoveredAfterCrash}'
stop_app TERM >/dev/null
if [[ "$missing" == 0 && "$changed" == 0 && "$final" == "$N" ]]; then
  log "PASS: no accepted task lost across kill -9, retries were idempotent, every key ran as exactly one task"
else
  log "FAIL"; exit 1
fi
