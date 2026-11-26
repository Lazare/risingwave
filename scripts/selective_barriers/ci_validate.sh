#!/usr/bin/env bash
# One step of the validation of selective barriers, on a cluster started by risedev with the
# profiles of `scripts/selective_barriers/risedev-profiles.yml`. Used by the workflow
# `.github/workflows/selective-barriers-validation.yml` and usable locally.
#
# Usage: scripts/selective_barriers/ci_validate.sh <step>
#   build                 build the binaries and start a cluster once
#   matrix <profile>      source connectors and sinks (connector_matrix.py)
#   streaming             streaming SLT with selective barriers on
#   batch                 batch SLT with selective barriers on
#   after-upgrade <profile>   restart the cluster of a previous `matrix <profile>` on the same
#                         data with the binary of the current tree, and check every object
#   udf                   SLT of embedded user-defined functions and of their volatility
#                         (needs a build with ENABLE_UDF=true and the WASM test module)
#   sink-into-table       SLT of sinks into tables with selective barriers on
#   sink-chaos            crash/restart test of decoupled sinks
#   suite                 SLT and chaos tests with the feature off, on, and on with
#                         checkpoint_frequency = 30 (run_tests.sh)
#
# Environment:
#   OUT                   directory of the outputs and of the copied logs (default: ./selbar-out)
#   PGHOST, PGPORT, ...   Postgres with wal_level = logical (CDC, Postgres and JDBC sinks)
#   KAFKA_BROKER, KAFKA_CONTAINER      Kafka API and the docker container that has `rpk`
#   MYSQL_HOST, MYSQL_CONTAINER, MYSQL_PASSWORD   MySQL with row binlog and its container
#   ROUNDS, SEED          chaos rounds (default 8) and seed (default 4242)
set -uo pipefail
cd "$(dirname "$0")/../.."
OUT=${OUT:-$PWD/selbar-out}
mkdir -p "$OUT"
export CARGO_INCREMENTAL=0
PORT=4577
ROUNDS=${ROUNDS:-8}
SEED=${SEED:-4242}

# Streaming SLT files that fail for reasons unrelated to selective barriers: one needs the `zx`
# command, the others run after a failed file and find its objects. Any other failure fails the
# step. `impure_expr_materialize.slt` needs a build with ENABLE_UDF=true; the `udf` step runs
# it on its own and requires it to pass.
KNOWN_STREAMING_FAILURES="
e2e_test/streaming/impure_expr_materialize.slt
e2e_test/streaming/queryable_internal_state/group_agg.slt
e2e_test/streaming/row_id_cast.slt
e2e_test/streaming/sink_into_table_ignore_delete.slt
e2e_test/streaming/sink_into_wide_table.slt
e2e_test/streaming/struct_table.slt
e2e_test/streaming/values.slt
e2e_test/streaming/watermark.slt
"

drop_slots() {
  [[ -n "${PGHOST:-}" ]] || return 0
  psql -q -Atc "SELECT pg_terminate_backend(active_pid) FROM pg_replication_slots WHERE active_pid IS NOT NULL; SELECT pg_sleep(1); SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots;" >/dev/null 2>&1 || true
}

drop_topics() {
  [[ -n "${KAFKA_CONTAINER:-}" ]] || return 0
  for topic in $(docker exec "$KAFKA_CONTAINER" rpk topic list 2>/dev/null | awk 'NR>1{print $1}' | grep '^m_'); do
    docker exec "$KAFKA_CONTAINER" rpk topic delete "$topic" >/dev/null 2>&1 || true
  done
}

fresh() {
  ./risedev k >/dev/null 2>&1 || true
  tmux -L risedev kill-server >/dev/null 2>&1 || true
  sleep 5
  drop_slots
  rm -rf risingwave-test .risingwave/data/sqlite
  if ! ./risedev d "$1" >"$OUT/dev-$1.log" 2>&1; then
    echo "FAILED to start profile $1"
    tail -40 "$OUT/dev-$1.log"
    return 1
  fi
  echo "cluster started with profile $1"
}

save_logs() {
  mkdir -p "$OUT/logs-$1"
  cp .risingwave/log/*.log "$OUT/logs-$1/" 2>/dev/null || true
}

# Fails if a process of the cluster panicked.
no_panic() {
  local files
  files=$(grep -a -l "panicked at" .risingwave/log/*.log 2>/dev/null || true)
  if [[ -n "$files" ]]; then
    echo "FAILED: panic in $files"
    grep -a -h -A3 "panicked at" $files | head -20
    return 1
  fi
}

step_build() {
  fresh selbar-on || return 1
  psql -h localhost -p $PORT -d dev -U root -X -Atc "select version()" || return 1
  ./risedev k >/dev/null 2>&1 || true
}

step_matrix() {
  local profile=$1 rc=0
  fresh "$profile" || return 1
  drop_topics
  matrix_args
  timeout 3000 python3 -I -u scripts/selective_barriers/connector_matrix.py "${MATRIX_ARGS[@]}" 2>&1 \
    | tee "$OUT/matrix-$profile.log" | cut -c1-1200
  rc=${PIPESTATUS[0]}
  save_logs "matrix-$profile"
  no_panic || rc=1
  return $rc
}

matrix_args() {
  MATRIX_ARGS=(--port $PORT)
  [[ -n "${KAFKA_BROKER:-}" ]] && MATRIX_ARGS+=(--kafka-broker "$KAFKA_BROKER" --kafka-container "${KAFKA_CONTAINER:-}")
  [[ -n "${MYSQL_HOST:-}" ]] && MATRIX_ARGS+=(--mysql-host "$MYSQL_HOST" --mysql-container "${MYSQL_CONTAINER:-}" --mysql-password "${MYSQL_PASSWORD:-}")
  return 0
}

# The data of the previous run is kept: only the binaries change.
step_after_upgrade() {
  local profile=$1 rc=0 version
  ./risedev k >/dev/null 2>&1 || true
  tmux -L risedev kill-server >/dev/null 2>&1 || true
  sleep 5
  if ! ./risedev d "$profile" >"$OUT/dev-upgrade-$profile.log" 2>&1; then
    echo "FAILED to restart profile $profile with the new binary"
    tail -40 "$OUT/dev-upgrade-$profile.log"
    return 1
  fi
  version=$(grep -m1 '^version = ' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
  echo "restarted on the existing data, expecting version $version"
  # The cluster recovers its streaming jobs after a restart, and refuses DML and webhook rows
  # until then: wait for the end of the recovery before checking the objects.
  local status="" waited=0
  until [[ "$status" == "RUNNING" ]]; do
    if ((waited >= 600)); then
      echo "FAILED: the cluster is not running ${waited}s after the restart (status: ${status:-unreachable})"
      return 1
    fi
    sleep 2
    waited=$((waited + 2))
    status=$(psql -h localhost -p $PORT -d dev -U root -X -q -At -c "select rw_recovery_status()" 2>/dev/null)
  done
  echo "the cluster is running ${waited}s after the restart"
  matrix_args
  timeout 1500 python3 -I -u scripts/selective_barriers/connector_matrix.py "${MATRIX_ARGS[@]}" \
    --after-restart --expect-version "$version" 2>&1 | tee "$OUT/after-upgrade-$profile.log" | cut -c1-1200
  rc=${PIPESTATUS[0]}
  save_logs "after-upgrade-$profile"
  no_panic || rc=1
  return $rc
}

step_streaming() {
  local rc=0 log="$OUT/slt-streaming.log"
  fresh selbar-on || return 1
  ./risedev slt -p $PORT -d dev './e2e_test/streaming/**/*.slt' >"$log" 2>&1
  echo "slt exit=$? ok=$(grep -a -c '\[OK\]' "$log") failed=$(grep -a -c '\[FAILED\]' "$log")"
  local failed unexpected=""
  failed=$(sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -a '^failed to run' | sed "s/^failed to run \`\(.*\)\`.*/\1/" | sort -u)
  for file in $failed; do
    if grep -qxF "$file" <<<"$KNOWN_STREAMING_FAILURES"; then
      echo "known failure: $file"
    else
      echo "UNEXPECTED failure: $file"
      unexpected+=" $file"
    fi
  done
  if [[ -n "$unexpected" ]]; then
    rc=1
    for file in $unexpected; do
      sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -a -A25 "^failed to run \`$file\`" | cut -c1-300 | head -40
    done
  fi
  # The suite must reach its end: a stuck cluster makes sqllogictest fail everything after.
  if [[ $(grep -a -c '\[OK\]' "$log") -lt 400 ]]; then
    echo "FAILED: fewer than 400 files passed"
    rc=1
  fi
  save_logs streaming
  no_panic || rc=1
  return $rc
}

step_batch() {
  local rc=0 log="$OUT/slt-batch.log"
  fresh selbar-on || return 1
  ./risedev slt -p $PORT -d dev './e2e_test/batch/**/*.slt' >"$log" 2>&1 || rc=1
  echo "ok=$(grep -a -c '\[OK\]' "$log") failed=$(grep -a -c '\[FAILED\]' "$log")"
  [[ $rc -eq 0 ]] || sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -a -A25 '^failed to run' | cut -c1-300 | head -80
  save_logs batch
  no_panic || rc=1
  return $rc
}

step_udf() {
  local rc=0 log="$OUT/slt-udf.log"
  fresh selbar-on || return 1
  ./risedev slt -p $PORT -d dev \
    './e2e_test/streaming/impure_expr_materialize.slt' \
    './e2e_test/udf/tests/scalar/javascript.slt' \
    './e2e_test/udf/tests/scalar/python.slt' \
    './e2e_test/udf/tests/scalar/wasm.slt' \
    './e2e_test/udf/tests/scalar/wasm_volatility.slt' >"$log" 2>&1 || rc=1
  echo "ok=$(grep -a -c '\[OK\]' "$log") failed=$(grep -a -c '\[FAILED\]' "$log")"
  [[ $rc -eq 0 ]] || sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -a -A25 '^failed to run' | cut -c1-300 | head -100
  save_logs udf
  no_panic || rc=1
  return $rc
}

step_sink_into_table() {
  local rc=0 log="$OUT/slt-sink-into-table.log"
  fresh selbar-on || return 1
  ./risedev slt -p $PORT -d dev './e2e_test/sink/sink_into_table/*.slt' >"$log" 2>&1 || rc=1
  echo "ok=$(grep -a -c '\[OK\]' "$log") failed=$(grep -a -c '\[FAILED\]' "$log")"
  [[ $rc -eq 0 ]] || sed 's/\x1b\[[0-9;]*m//g' "$log" | grep -a -A25 '^failed to run' | cut -c1-300 | head -60
  save_logs sink-into-table
  no_panic || rc=1
  return $rc
}

step_sink_chaos() {
  local rc=0
  [[ -n "${PGHOST:-}" ]] || { echo "skipped: no Postgres"; return 0; }
  fresh selbar-on || return 1
  python3 -I -u scripts/selective_barriers/sink_chaos_test.py --profile selbar-on --port $PORT \
    --rounds "$ROUNDS" --seed "$SEED" >"$OUT/sink-chaos.log" 2>&1 || rc=1
  tail -5 "$OUT/sink-chaos.log" | cut -c1-300
  save_logs sink-chaos
  no_panic || rc=1
  return $rc
}

step_suite() {
  local rc=0
  drop_slots
  scripts/selective_barriers/run_tests.sh "$ROUNDS" "$SEED" >"$OUT/suite.log" 2>&1 || rc=1
  grep -a "^===\|PASSED\|FAILED\|seed=\|rounds" "$OUT/suite.log" | cut -c1-200 | tail -40
  [[ $rc -eq 0 ]] || tail -30 "$OUT/suite.log" | cut -c1-300
  save_logs suite
  return $rc
}

step=${1:-}
shift || true
case "$step" in
  build) step_build ;;
  matrix) step_matrix "${1:?profile}" ;;
  streaming) step_streaming ;;
  batch) step_batch ;;
  udf) step_udf ;;
  after-upgrade) step_after_upgrade "${1:?profile}" ;;
  sink-into-table) step_sink_into_table ;;
  sink-chaos) step_sink_chaos ;;
  suite) step_suite ;;
  *) sed -n '2,20p' "$0"; exit 2 ;;
esac
rc=$?
./risedev k >/dev/null 2>&1 || true
exit $rc
