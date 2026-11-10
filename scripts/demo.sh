#!/usr/bin/env bash
# Starts the app, submits one task of each kind, and prints how each one ended.
source "$(dirname "$0")/common.sh"
RUN="${RUN_DIR:-$ROOT/run/demo}"
rm -rf "$RUN"
start_app "$RUN" --taskqueue.simulation.retryable-failure-rate=0 --taskqueue.simulation.system-failure-rate=0
trap 'stop_app TERM >/dev/null' EXIT

TEXT='"text":"Large language models can summarise long documents. This service runs those jobs asynchronously."'
log "submitting one task per scenario"
submit demo-ok          HIGH   "{$TEXT}"
submit demo-fail-once   NORMAL "{$TEXT,\"simulate\":\"fail-once\"}"
submit demo-fail-always NORMAL "{$TEXT,\"simulate\":\"fail-always\"}"
submit demo-invalid     LOW    "{$TEXT,\"simulate\":\"invalid\"}"
submit demo-system      NORMAL "{$TEXT,\"simulate\":\"system-once\"}"
submit demo-bug         LOW    "{$TEXT,\"simulate\":\"bug\"}"
log "resubmitting demo-ok with the same payload (expect 200, same id) and a different one (expect 409)"
submit demo-ok HIGH "{$TEXT}"
submit demo-ok HIGH '{"text":"something else"}'

wait_drained 60
log "final state of each task"
curl -s "$BASE/api/tasks?size=20" | jq -r '.items[] | [.bizKey, .status, "attempts=\(.attempts)", "systemRetries=\(.systemRetries)", (.lastError // "")] | @tsv' | sort
log "result of demo-ok"
ID=$(curl -s "$BASE/api/tasks?status=SUCCEEDED&size=20" | jq -r '.items[] | select(.bizKey=="demo-ok") | .taskId')
curl -s "$BASE/api/tasks/$ID/result" | jq .
log "attempt history of demo-fail-always"
ID=$(curl -s "$BASE/api/tasks?status=DEAD_LETTER&size=20" | jq -r '.items[] | select(.bizKey=="demo-fail-always") | .taskId')
curl -s "$BASE/api/tasks/$ID?history=true" | jq -c '.history[] | {attempt, outcome, errorType, error}'
log "stats"; stats | jq -c .
log "metrics"; metrics | jq .
