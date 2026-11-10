#!/usr/bin/env bash
# Shared helpers for the validation scripts. Needs: java 17+, curl, jq.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-18080}"
BASE="http://localhost:${PORT}"
JAVA="${JAVA_HOME:+$JAVA_HOME/bin/}java"
APP_PID=""

log() { printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

jar_path() {
  local jar
  jar=$(ls "$ROOT"/target/async-task-queue-lossless-*.jar 2>/dev/null | head -1 || true)
  if [[ -z "$jar" ]]; then
    log "building the jar"
    (cd "$ROOT" && ./mvnw -q -DskipTests package ${MAVEN_ARGS:-})
    jar=$(ls "$ROOT"/target/async-task-queue-lossless-*.jar | head -1)
  fi
  echo "$jar"
}

# start_app <run-dir> [extra --taskqueue.* args...]
start_app() {
  local dir="$1"; shift
  if curl -s -o /dev/null "$BASE/actuator/health"; then
    echo "something is already listening on port $PORT; set PORT to a free port" >&2
    exit 1
  fi
  mkdir -p "$dir"
  local jar; jar=$(jar_path)
  "$JAVA" -jar "$jar" --server.port="$PORT" --taskqueue.data-dir="$dir/data" "$@" >>"$dir/app.log" 2>&1 &
  APP_PID=$!
  for _ in $(seq 1 120); do
    if curl -s "$BASE/api/metrics" | jq -e '.queue.capacity' >/dev/null 2>&1; then
      log "app up (pid $APP_PID, port $PORT)"
      return 0
    fi
    if ! kill -0 "$APP_PID" 2>/dev/null; then
      echo "app exited during startup, see $dir/app.log" >&2
      exit 1
    fi
    sleep 0.5
  done
  echo "app did not become ready, see $dir/app.log" >&2
  exit 1
}

# stop_app TERM|KILL ; waits for the process to exit and prints how long it took (ms)
stop_app() {
  local sig="$1" start end
  start=$(now_ns)
  kill -"$sig" "$APP_PID"
  while kill -0 "$APP_PID" 2>/dev/null; do sleep 0.05; done
  end=$(now_ns)
  echo $(( (end - start) / 1000000 ))
}

now_ns() { python3 -c 'import time;print(time.time_ns())'; }

# submit <bizKey> <priority> <payload-json>
# prints "<http-code> <taskId-or-dash> <bizKey> <priority> <seconds>"
submit() {
  local out last code secs body
  out=$(curl -s -w '\n%{http_code} %{time_total}' -X POST "$BASE/api/tasks" -H 'content-type: application/json' \
    -d "{\"bizKey\":\"$1\",\"priority\":\"$2\",\"payload\":$3}" 2>/dev/null || true)
  last=$(tail -n1 <<<"$out")
  code=${last%% *}
  secs=${last##* }
  body=$(sed '$d' <<<"$out")
  echo "${code:-000} $(jq -r '.taskId // "-"' <<<"$body" 2>/dev/null || echo -) $1 $2 ${secs:-0}"
}
export -f submit
export BASE

stats() { curl -s "$BASE/api/tasks/stats"; }
metrics() { curl -s "$BASE/api/metrics"; }

# wait_drained <timeout-seconds> : until nothing is waiting or running
wait_drained() {
  local deadline=$(( $(date +%s) + $1 ))
  while (( $(date +%s) < deadline )); do
    if stats | jq -e '.backlog == 0 and .running == 0' >/dev/null; then return 0; fi
    sleep 1
  done
  echo "backlog not drained within $1 s" >&2
  return 1
}

# submit_many <count> <prefix> <parallelism> : submits <prefix>-1..N with scripts/LoadGen.java,
# priority by i % 10 (0-1 HIGH, 2-5 NORMAL, 6-9 LOW), payload derived from the key so a
# resubmission is identical. One output line per request, same format as submit.
submit_many() {
  "$JAVA" "$ROOT/scripts/LoadGen.java" "$BASE" "$2" "$1" "$3"
}

# summarize_responses <file> : counts by HTTP code and priority, and submit latency percentiles
summarize_responses() {
  awk '{c[$1]++; cp[$1" "$4]++} END {for (k in c) printf "  HTTP %s: %d\n", k, c[k]; for (k in cp) printf "    %s\n", k" "cp[k]}' "$1" | sort
  awk '$1=="202" || $1=="200" || $1=="429" {print $5*1000}' "$1" | sort -n | awk '{a[NR]=$1} END {if (NR) printf "  submit latency ms: p50=%.1f p95=%.1f p99=%.1f max=%.1f (n=%d)\n", a[int(NR*0.5)+1], a[int(NR*0.95)], a[int(NR*0.99)], a[NR], NR}'
}
