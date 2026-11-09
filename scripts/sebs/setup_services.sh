#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"

ACTION=${1:-start}
SERVICE_BIND_ADDRESS=${SERVICE_BIND_ADDRESS:-0.0.0.0}
MINIO_ADDRESS=${MINIO_ADDRESS:-127.0.0.1:9000}
MINIO_IMAGE=${MINIO_IMAGE:-quay.io/minio/minio:RELEASE.2022-01-08T03-11-54Z}
MINIO_CONTAINER=${MINIO_CONTAINER:-iluvatar-sebs-minio}
RUNTIME_DIR=${RUNTIME_DIR:-"$SEBS_DIR/.runtime"}
SEBS_SOURCE_ROOT=${SEBS_SOURCE_ROOT:-}

stop_services() {
	for service in udp tcp; do
		if [[ -f "$RUNTIME_DIR/$service.pid" ]]; then
			kill "$(cat "$RUNTIME_DIR/$service.pid")" 2>/dev/null || true
			rm -f "$RUNTIME_DIR/$service.pid"
		fi
	done
	docker rm -f "$MINIO_CONTAINER" >/dev/null 2>&1 || true
}

start_services() {
	command -v docker >/dev/null
	command -v socat >/dev/null
	mkdir -p "$RUNTIME_DIR"
	stop_services

	docker run --rm --detach \
		--name "$MINIO_CONTAINER" \
		-p 9000:9000 -p 9001:9001 \
		-e MINIO_ROOT_USER=minioadmin \
		-e MINIO_ROOT_PASSWORD=minioadmin \
		"$MINIO_IMAGE" server /data --console-address :9001 >/dev/null

	nohup socat UDP4-LISTEN:9003,so-broadcast,so-timestamp,ip-pktinfo,ip-recverr,ip-recvopts,ip-recvtos,ip-recvttl,reuseaddr,fork,bind="$SERVICE_BIND_ADDRESS" SYSTEM:cat \
		>"$RUNTIME_DIR/udp.log" 2>&1 &
	echo $! >"$RUNTIME_DIR/udp.pid"
	nohup socat TCP4-LISTEN:9004,reuseaddr,fork,bind="$SERVICE_BIND_ADDRESS" SYSTEM:'echo "Reply from $SOCAT_SOCKADDR"' \
		>"$RUNTIME_DIR/tcp.log" 2>&1 &
	echo $! >"$RUNTIME_DIR/tcp.pid"

	if [[ -n $SEBS_SOURCE_ROOT ]]; then
		command -v curl >/dev/null
		for _ in $(seq 1 30); do
			if curl --silent --fail "http://$MINIO_ADDRESS/minio/health/live" >/dev/null; then
				break
			fi
			sleep 1
		done
		curl --silent --fail "http://$MINIO_ADDRESS/minio/health/live" >/dev/null || {
			echo "MinIO did not become ready at $MINIO_ADDRESS" >&2
			exit 1
		}
		python3 "$SEBS_DIR/services/prepare_benchmark_data.py" \
			--benchmarks "$SEBS_SOURCE_ROOT/benchmarks" \
			--benchmark-data "$SEBS_SOURCE_ROOT/benchmarks-data" \
			--minio-address "$MINIO_ADDRESS"
	else
		cat <<EOF
SEBS services started on $SERVICE_BIND_ADDRESS (MinIO: $MINIO_ADDRESS).
Storage-backed functions still require benchmark data. Set SEBS_SOURCE_ROOT to
a directory containing benchmarks/ and benchmarks-data/, then rerun this script.
EOF
	fi
}

case $ACTION in
	start) start_services ;;
	stop) stop_services ;;
	restart) stop_services; start_services ;;
	*) echo "Usage: $(basename "$0") [start|stop|restart]" >&2; exit 2 ;;
esac
