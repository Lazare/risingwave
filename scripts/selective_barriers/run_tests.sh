#!/usr/bin/env bash
# Run the selective barrier test suite on a local cluster, with the feature off then on.
# Requires the risedev profiles `selbar` (feature off) and `selbar-on` (feature on), both with
# persistent storage (sqlite meta store and fs object store) so that restarts recover state.
#
# Usage: scripts/selective_barriers/run_tests.sh [chaos rounds] [seed]
set -euo pipefail
cd "$(dirname "$0")/../.."
ROUNDS=${1:-30}
SEED=${2:-$RANDOM}
export CARGO_INCREMENTAL=0

fresh_cluster() {
  ./risedev k >/dev/null 2>&1 || true
  tmux -L risedev kill-server >/dev/null 2>&1 || true
  rm -rf risingwave-test .risingwave/data/sqlite
  ./risedev d "$1" | grep "All services started successfully" >/dev/null
}

no_panic() {
  if grep -l "panicked at" .risingwave/log/*.log; then
    echo "FAILED: panic found in the logs above"
    exit 1
  fi
}

for profile in selbar selbar-on; do
  echo "=== $profile: SLT"
  fresh_cluster "$profile"
  for f in e2e_test/streaming/selective_barrier/*.slt; do
    ./risedev slt -p 4577 -d dev "./$f" 2>&1 | grep -E "\[OK\]|\[END\]" | grep -v check.slt.part || {
      echo "FAILED: $f"
      exit 1
    }
  done
  no_panic
  for backfill in snapshot arrangement; do
    echo "=== $profile: chaos ($backfill backfill, $ROUNDS rounds, seed $SEED)"
    fresh_cluster "$profile"
    python3 -I -u scripts/selective_barriers/chaos_test.py --profile "$profile" --port 4577 \
      --rounds "$ROUNDS" --crash-rate 0.3 --seed "$SEED" --pause-before-flush 3 --idle 4 \
      --backfill "$backfill" | tail -1
    no_panic
  done
done
echo "ALL PASSED"
