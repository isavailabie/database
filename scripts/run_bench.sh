#!/usr/bin/env bash
# Convenience wrapper around benchmark/runner.py.
#
#   bash scripts/run_bench.sh quick        # 1M points, 60 s mixed
#   bash scripts/run_bench.sh standard     # 10M points, 600 s mixed, 3 repeats
#   bash scripts/run_bench.sh full         # 100M points, 3600 s mixed
#
# Extra arguments are forwarded, e.g.
#
#   bash scripts/run_bench.sh quick --cases perf_ingest --matrix

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
MODE="${1:-quick}"
shift || true

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="${RESULTS_DIR:-$ROOT/bench-results}/$MODE-$STAMP"

echo "profile=$MODE"
echo "output=$OUT"
python3 "$ROOT/benchmark/runner.py" --source "$ROOT" --output "$OUT" --profile "$MODE" "$@"
