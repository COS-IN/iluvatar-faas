#!/usr/bin/env bash

set -Eeuo pipefail

SEBS_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SEBS_DIR/../.." && pwd)
ILU_HOME=${ILU_HOME:-"$REPO_ROOT/src/Ilúvatar"}
BUILD=${BUILD:-debug}
HOST=${HOST:-127.0.0.1}
PORT=${PORT:-8079}
RESULTS_ROOT=${RESULTS_ROOT:-"$SEBS_DIR/results"}

if [[ -z ${LOAD_GEN_BIN:-} ]]; then
	if [[ -x "$ILU_HOME/target/$BUILD/iluvatar_load_gen" ]]; then
		LOAD_GEN_BIN="$ILU_HOME/target/$BUILD/iluvatar_load_gen"
	else
		LOAD_GEN_BIN="$ILU_HOME/target/x86_64-unknown-linux-gnu/$BUILD/iluvatar_load_gen"
	fi
fi

require_file() {
	[[ -f $1 ]] || {
		echo "Required file not found: $1" >&2
		exit 1
	}
}

require_executable() {
	[[ -x $1 ]] || {
		echo "Required executable not found: $1" >&2
		exit 1
	}
}

new_results_dir() {
	local name=$1
	local output=${2:-"$RESULTS_ROOT/${name}-$(date +%Y%m%d%H%M%S)"}
	mkdir -p "$output"
	realpath "$output"
}
