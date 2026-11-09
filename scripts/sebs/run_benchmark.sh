#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

FUNCTION_FILE=${FUNCTION_FILE:-"$SEBS_DIR/metadata/sebs_meta.csv"}
COLD_ITERS=${COLD_ITERS:-1}
WARM_ITERS=${WARM_ITERS:-10}
OUT_FOLDER=""

usage() {
	cat <<EOF
Usage: $(basename "$0") [options]

  --function-file FILE  SEBS benchmark metadata CSV
  --cold-iters N        Cold invocations per function (default: $COLD_ITERS)
  --warm-iters N        Warm invocations after each cold run (default: $WARM_ITERS)
  --out-folder DIR      Results directory
  --host HOST           Local worker host (default: $HOST)
  --port PORT           Local worker port (default: $PORT)
EOF
}

while (($#)); do
	case $1 in
		--function-file) FUNCTION_FILE=$2; shift ;;
		--cold-iters) COLD_ITERS=$2; shift ;;
		--warm-iters) WARM_ITERS=$2; shift ;;
		--out-folder) OUT_FOLDER=$2; shift ;;
		--host) HOST=$2; shift ;;
		--port) PORT=$2; shift ;;
		-h|--help) usage; exit 0 ;;
		*) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
	shift
done

require_executable "$LOAD_GEN_BIN"
require_file "$FUNCTION_FILE"
FUNCTION_FILE=$(realpath "$FUNCTION_FILE")
OUT_FOLDER=$(new_results_dir benchmark "$OUT_FOLDER")

cd "$SEBS_DIR"
"$LOAD_GEN_BIN" benchmark \
	--target worker \
	--function-file "$FUNCTION_FILE" \
	--cold-iters "$COLD_ITERS" \
	--warm-iters "$WARM_ITERS" \
	--port "$PORT" \
	--host "$HOST" \
	--out-folder "$OUT_FOLDER" \
	--log-stdout \
	2>&1 | tee "$OUT_FOLDER/load_gen.log"

echo "SEBS benchmark results: $OUT_FOLDER"
