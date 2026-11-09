#!/usr/bin/env bash

set -Eeuo pipefail
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
BPFTRACE_DIR="$SCRIPT_DIR/bpftrace"
BPFTRACE_BIN=${BPFTRACE_BIN:-$(command -v bpftrace || true)}
ACTION=${1:-}
shift || true
OUTPUT_DIR=""
PROFILES=${PROFILES:-cpufrequency,dsq-length,task-cpu,task-queue}

declare -A PROGRAMS=(
	[cpufrequency]=cpu_cycles.bt
	[cache-misses]=cpu_cachemisses.bt
	[cache-misses-per-cycle]=cpu_cachemisses_per_cycle.bt
	[cache-references]=cpu_cacherefs.bt
	[cache-references-per-cycle]=cpu_cacherefs_per_cycle.bt
	[instructions]=cpu_instructions.bt
	[instructions-per-cycle]=cpu_instructions_per_cycle.bt
	[dsq-length]=cpu_dsqlen.bt
	[task-cpu]=taskcputiming.bt
	[task-queue]=taskqueuetiming.bt
	[task-enqueue]=task_enqueue_timing.bt
	[task-wakeup]=taskwakeuptiming.bt
	[task-timeslice-distribution]=tasktimeslicedistribution.bt
	[task-cpu-distribution]=taskcputimedistribution.bt
	[task-wait]=taskcputimemeanwaittime.bt
	[task-wait-tracepoint]=taskcputimemeanwaittime_tracepoint.bt
	[task-ip-variance]=taskipvariancedistribution.bt
)

usage() {
	cat <<EOF
Usage: $(basename "$0") <start|stop|status|list> [options]

  --output-dir DIR    Profiling output directory
  --profiles LIST     Comma-separated profile names

Set BPFTRACE_BIN to override the system bpftrace executable.
EOF
}

if [[ $ACTION == -h || $ACTION == --help ]]; then
	usage
	exit 0
fi

while (($#)); do
	case $1 in
		--output-dir) OUTPUT_DIR=$2; shift ;;
		--profiles) PROFILES=$2; shift ;;
		-h|--help) usage; exit 0 ;;
		*) echo "Unknown option: $1" >&2; usage >&2; exit 2 ;;
	esac
	shift
done

if [[ $ACTION == list ]]; then
	printf '%s\n' "${!PROGRAMS[@]}" | sort
	exit 0
fi
[[ -n $ACTION && -n $OUTPUT_DIR ]] || { usage >&2; exit 2; }
OUTPUT_DIR=$(realpath -m "$OUTPUT_DIR")
RUNTIME_DIR="$OUTPUT_DIR/.profiling"

start_profile() {
	local name=$1
	local program=${PROGRAMS[$name]:-}
	[[ -n $program ]] || { echo "Unknown profile: $name" >&2; exit 2; }
	[[ -f $BPFTRACE_DIR/$program ]] || { echo "Missing profile program: $program" >&2; exit 1; }
	local pid_file="$RUNTIME_DIR/$name.pid"
	local log_file="$OUTPUT_DIR/bpftrace_$name.log"
	sudo sh -c 'echo $$ > "$1"; exec "$2" "$3" > "$4" 2>&1' sh \
		"$pid_file" "$BPFTRACE_BIN" "$BPFTRACE_DIR/$program" "$log_file" &
	for _ in $(seq 1 50); do [[ -s $pid_file ]] && break; sleep 0.1; done
	[[ -s $pid_file ]] || { echo "Failed to start profile: $name" >&2; exit 1; }
	echo "Started $name (PID $(cat "$pid_file"))"
}

stop_profile() {
	local pid_file=$1
	local pid
	pid=$(cat "$pid_file")
	sudo kill -INT "$pid" 2>/dev/null || true
	rm -f "$pid_file"
}

case $ACTION in
	start)
		[[ -n $BPFTRACE_BIN && -x $BPFTRACE_BIN ]] || { echo "bpftrace not found; set BPFTRACE_BIN" >&2; exit 1; }
		mkdir -p "$RUNTIME_DIR"
		IFS=',' read -ra selected <<<"$PROFILES"
		for profile in "${selected[@]}"; do start_profile "$profile"; done
		;;
	stop)
		[[ -d $RUNTIME_DIR ]] || exit 0
		shopt -s nullglob
		for pid_file in "$RUNTIME_DIR"/*.pid; do stop_profile "$pid_file"; done
		;;
	status)
		shopt -s nullglob
		for pid_file in "$RUNTIME_DIR"/*.pid; do
			pid=$(cat "$pid_file")
			if sudo kill -0 "$pid" 2>/dev/null; then state=running; else state=stopped; fi
			printf '%s %s pid=%s\n' "$(basename "$pid_file" .pid)" "$state" "$pid"
		done
		;;
	*) usage >&2; exit 2 ;;
esac
