#!/usr/bin/env bash
# Builds the image, runs it, processes one task, stops it gracefully, starts it again on the
# same volume and checks the task and its result are still there. Needs: docker, curl, jq.
set -euo pipefail
cd "$(dirname "$0")/.."
PORT="${PORT:-18080}"
NAME="taskqueue-smoke-$$"
VOLUME="$NAME-data"
BASE="http://localhost:$PORT"
log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; docker volume rm "$VOLUME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 90); do
    if curl -sf "$BASE/actuator/health" >/dev/null; then return 0; fi
    sleep 1
  done
  docker logs "$NAME" | tail -50
  echo "container did not become ready" >&2
  exit 1
}

log "building the image"
docker build -q -t async-task-queue-lossless:smoke . >/dev/null
docker run -d --name "$NAME" -p "$PORT:8080" -v "$VOLUME:/data" async-task-queue-lossless:smoke >/dev/null
wait_ready
log "container up"

ID=$(curl -s -X POST "$BASE/api/tasks" -H 'content-type: application/json' \
  -d '{"bizKey":"docker-smoke-1","priority":"HIGH","payload":{"text":"Hello from inside a container. Summarise me.","latencyMs":200}}' | jq -r .taskId)
log "submitted $ID"
for _ in $(seq 1 30); do
  STATUS=$(curl -s "$BASE/api/tasks/$ID" | jq -r .status)
  [[ "$STATUS" == "SUCCEEDED" ]] && break
  sleep 1
done
log "status: $STATUS"
[[ "$STATUS" == "SUCCEEDED" ]]

log "docker stop (SIGTERM)"
docker stop -t 45 "$NAME" >/dev/null
docker logs "$NAME" 2>&1 | grep -E 'Task engine stopped' | sed 's/.*TaskEngine *: /  /'

log "starting again on the same volume"
docker start "$NAME" >/dev/null
wait_ready
AFTER=$(curl -s "$BASE/api/tasks/$ID" | jq -r .status)
SUMMARY=$(curl -s "$BASE/api/tasks/$ID/result" | jq -r .result.summary)
log "after restart: status=$AFTER summary=\"$SUMMARY\""
DUP=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/api/tasks" -H 'content-type: application/json' \
  -d '{"bizKey":"docker-smoke-1","priority":"HIGH","payload":{"text":"Hello from inside a container. Summarise me.","latencyMs":200}}')
log "same request again: HTTP $DUP (expect 200, the existing task)"
if [[ "$AFTER" == "SUCCEEDED" && -n "$SUMMARY" && "$SUMMARY" != "null" && "$DUP" == "200" ]]; then
  log "PASS: the image runs, stops gracefully, and keeps its data across a restart"
else
  log "FAIL"; exit 1
fi
