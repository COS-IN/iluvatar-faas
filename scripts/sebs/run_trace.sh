#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

TRACE_DIR=${TRACE_DIR:-"$SEBS_DIR/traces/sebs_180_5.0"}
FUNCTION_DATA=${FUNCTION_DATA:-}
PREWARMS=${PREWARMS:-0}
MAX_PREWARMS=${MAX_PREWARMS:-20}
OUT_FOLDER=""

usage() {
	cat <<EOF
Usage: $(basename "$0") --function-data FILE [options]

  --function-data FILE  worker_function_benchmarks.json from run_benchmark.sh
  --trace-dir DIR        Directory containing funcs-in.csv and funcs-meta.csv
  --prewarms N           Requested prewarms (default: $PREWARMS)
  --max-prewarms N       Per-function prewarm cap (default: $MAX_PREWARMS)
  --out-folder DIR       Results directory
  --host HOST            Local worker host (default: $HOST)
  --port PORT            Local worker port (default: $PORT)
EOF
}

while (($#)); do
	case $1 in
		--function-data) FUNCTION_DATA=$2; shift ;;
		--trace-dir) TRACE_DIR=$2; shift ;;
		--prewarms) PREWARMS=$2; shift ;;
		--max-prewarms) MAX_PREWARMS=$2; shift ;;
		--out-folder) OUT_FOLDER=$2; shift ;;
		--host) HOST=$2; shift ;;
		--port) PORT=$2; shift ;;
		-h|--help) usage; exit 0 ;;
		*) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
	shift
done

[[ -n $FUNCTION_DATA ]] || {
	usage >&2
	exit 2
}

require_executable "$LOAD_GEN_BIN"
require_file "$TRACE_DIR/funcs-in.csv"
require_file "$TRACE_DIR/funcs-meta.csv"
require_file "$FUNCTION_DATA"
TRACE_DIR=$(realpath "$TRACE_DIR")
FUNCTION_DATA=$(realpath "$FUNCTION_DATA")
OUT_FOLDER=$(new_results_dir trace "$OUT_FOLDER")

cd "$SEBS_DIR"
date +%s >"$OUT_FOLDER/trace_start_time_including_prewarm.txt"
set +e
"$LOAD_GEN_BIN" trace \
	--input-csv "$TRACE_DIR/funcs-in.csv" \
	--metadata-csv "$TRACE_DIR/funcs-meta.csv" \
	--port "$PORT" \
	--host "$HOST" \
	--out-folder "$OUT_FOLDER" \
	--target worker \
	--setup live \
	--load-type functions \
	--function-data "$FUNCTION_DATA" \
	--prewarms "$PREWARMS" \
	--max-prewarms "$MAX_PREWARMS" \
	--log-stdout \
	2>&1 | tee "$OUT_FOLDER/load_gen.log"
status=${PIPESTATUS[0]}
set -e
date +%s >"$OUT_FOLDER/trace_end_time.txt"

echo "SEBS trace results: $OUT_FOLDER"
exit "$status"
