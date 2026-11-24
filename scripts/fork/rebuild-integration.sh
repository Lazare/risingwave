#!/usr/bin/env bash
# Rebuild the integration branch of the fork: an upstream tag plus the `fork/*` branches.
#
# Every `fork/*` branch starts from the upstream tag and holds one theme, so that it can be
# rebased alone on a new upstream version:
#
#   git rebase --onto <new tag> <old tag> fork/<name>
#
# Then this script merges them, in the fixed order below, on top of the tag:
#
#   scripts/fork/rebuild-integration.sh <tag> <integration branch> [remote]
#
# With a remote, the branches are read from it (`<remote>/fork/<name>`), otherwise locally.
# The fixes of upstream code come first, then the features, then the tooling.
# `fork/ci-validation` tests every feature and is only meaningful once merged with them.
set -euo pipefail

BRANCHES=(
  fix-snapshot-backfill
  fix-batch-hash-agg-spill
  optimizer
  selective-barriers
  pg-cdc-native-engine
  postgres-sink-group
  sink-append-only-force-compaction
  udf-volatility
  batch-resource-governance
  resource-journal-and-nofile-limit
  license
  ci-multiarch-image
  ci-validation
)

# In a function, so that the script is read entirely before it checks out another tree.
main() {
  if [[ $# -lt 2 ]]; then
    sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
    exit 2
  fi
  local base="$1" target="$2" prefix="${3:+$3/}" name

  git checkout -B "$target" "$base"
  for name in "${BRANCHES[@]}"; do
    if ! git merge --no-ff --no-edit -m "Merge branch 'fork/$name' into $target" "${prefix}fork/$name"; then
      echo "CONFLICT while merging fork/$name: $(git diff --name-only --diff-filter=U | tr '\n' ' ')" >&2
      echo "Two branches touch the same lines: make them independent, then run again." >&2
      exit 1
    fi
  done
  echo "$target rebuilt from $base with ${#BRANCHES[@]} branches"
}

main "$@"
