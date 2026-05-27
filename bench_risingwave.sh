#!/usr/bin/env bash

# 这个脚本是 RisingWave Nexmark benchmark 的本地入口。
# 它负责准备临时目录、启动 Kafka 和 RisingWave standalone 容器，然后把网络、
# 端口、镜像和工作目录参数传给 `risingwave_bench_runner.py`。真正的 fixture 准备、
# query 执行、指标统计和 report 生成都在 Python runner 里完成。

set -euo pipefail

usage() {
	cat <<'EOF'
运行本地 RisingWave Nexmark benchmark。

Usage:
  bench_risingwave.sh [--rows N] [--queries q0,q1,q2,q14,q21,q22] [--parallelism N] [--sink table|blackhole]
                      [--bench-root DIR] [--no-cleanup] [--image IMAGE]
EOF
}

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

log() {
	printf '[%s UTC] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S')" "$1"
}

rows="1000000"
queries="q0,q1,q2,q14,q21,q22"
parallelism="1"
sink="table"
no_cleanup="0"
bench_root=""
rw_image="${RISINGWAVE_IMAGE:-risingwavelabs/risingwave:v2.8.3}"

while [[ $# -gt 0 ]]; do
	case "$1" in
	--rows)
		rows="$2"
		shift 2
		;;
	--queries)
		queries="$2"
		shift 2
		;;
	--parallelism)
		parallelism="$2"
		shift 2
		;;
	--sink)
		sink="$2"
		shift 2
		;;
	--bench-root)
		bench_root="$2"
		shift 2
		;;
	--no-cleanup)
		no_cleanup="1"
		shift
		;;
	--image)
		rw_image="$2"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		echo "unknown argument: $1" >&2
		usage >&2
		exit 1
		;;
	esac
done

unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY

if [[ -z "$bench_root" ]]; then
	bench_root="$(mktemp -d "${TMPDIR:-/tmp}/nexmark-bench.XXXXXX")"
fi
work_dir="$bench_root/risingwave"
risingwave_store_dir="$work_dir/risingwave-store"
run_id="$(basename "$bench_root" | tr -c '[:alnum:]' '-')"
kafka_network="risingwave-nexmark-net-${run_id}"
kafka_container="risingwave-nexmark-kafka-${run_id}"
rw_container="risingwave-nexmark-standalone-${run_id}"
rw_host_port=""
kafka_host_port=""

cleanup_services() {
	if [[ "$no_cleanup" == "1" ]]; then
		return
	fi
	docker rm -f "$rw_container" >/dev/null 2>&1 || true
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
	docker network rm "$kafka_network" >/dev/null 2>&1 || true
}

trap cleanup_services EXIT

wait_for_port() {
	local host="$1"
	local port="$2"
	local name="$3"
	local deadline=$((SECONDS + 60))
	until nc -z "$host" "$port" >/dev/null 2>&1; do
		if ((SECONDS >= deadline)); then
			echo "timeout waiting for $name on $host:$port" >&2
			exit 1
		fi
		sleep 1
	done
}

find_free_port() {
	local port
	for port in $(seq "$1" "$2"); do
		if ! ss -ltn "( sport = :$port )" | tail -n +2 | grep -q .; then
			echo "$port"
			return 0
		fi
	done
	echo "cannot find a free port in range $1-$2" >&2
	exit 1
}

prepare_workspace() {
	# Keep every run isolated because RisingWave persists local state under the work directory.
	log "Preparing RisingWave benchmark workspace at $work_dir"
	cleanup_services
	rm -rf "$work_dir"
	mkdir -p "$work_dir" "$risingwave_store_dir"
}

ensure_network() {
	# RisingWave reaches Kafka through a dedicated Docker network rather than host networking.
	docker network inspect "$kafka_network" >/dev/null 2>&1 || docker network create "$kafka_network" >/dev/null
}

start_kafka() {
	# Kafka is recreated per run so every query can replay from a clean earliest offset.
	log "Starting Kafka container $kafka_container on host port $kafka_host_port"
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
	docker run -d --name "$kafka_container" \
		--network "$kafka_network" \
		--network-alias kafka \
		--label datalayers.nexmark.bench=1 \
		--label datalayers.nexmark.run_id="$run_id" \
		-p "${kafka_host_port}:29092" \
		-e KAFKA_NODE_ID=1 \
		-e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://:29092,CONTROLLER://:9093 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://127.0.0.1:${kafka_host_port} \
		-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@kafka:9093 \
		-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		-e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
		-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
		-e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
		-e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
		confluentinc/cp-kafka:7.7.1 >/dev/null
	wait_for_port 127.0.0.1 "$kafka_host_port" kafka
	local deadline=$((SECONDS + 120))
	until docker exec "$kafka_container" kafka-topics --bootstrap-server 127.0.0.1:9092 --list >/dev/null 2>&1; do
		if ((SECONDS >= deadline)); then
			docker logs "$kafka_container" >&2 || true
			echo "timeout waiting for kafka broker readiness" >&2
			exit 1
		fi
		sleep 2
	done
	log "Kafka container $kafka_container is ready"
}

start_risingwave() {
	# The Python runner samples the main process inside this container during the replay window.
	log "Starting RisingWave container $rw_container on host port $rw_host_port with parallelism=$parallelism"
	docker rm -f "$rw_container" >/dev/null 2>&1 || true
	docker run -d --name "$rw_container" \
		--network "$kafka_network" \
		--label datalayers.nexmark.bench=1 \
		--label datalayers.nexmark.run_id="$run_id" \
		-p "${rw_host_port}:4566" \
		-v "$risingwave_store_dir:/risingwave/store" \
		"$rw_image" \
		single_node \
		--store-directory /risingwave/store \
		--listen-addr 0.0.0.0:4566 \
		--parallelism "$parallelism" >/dev/null
	wait_for_port 127.0.0.1 "$rw_host_port" risingwave
	log "RisingWave container $rw_container is ready"
}

wait_for_risingwave_kafka_connectivity() {
	# Wait until the container can resolve and connect to the Kafka network alias.
	local deadline=$((SECONDS + 60))
	until docker exec "$rw_container" bash -lc 'echo > /dev/tcp/kafka/9092' >/dev/null 2>&1; do
		if ((SECONDS >= deadline)); then
			echo "timeout waiting for RisingWave container to reach kafka:9092" >&2
			exit 1
		fi
		sleep 1
	done
}

run_bench() {
	# The Python runner performs the actual per-query replay and metric collection.
	log "Running RisingWave Nexmark benchmark: rows=$rows queries=$queries sink=$sink parallelism=$parallelism"
	python3 ./risingwave_bench_runner.py \
		--host 127.0.0.1 \
		--port "$rw_host_port" \
		--user root \
		--database dev \
		--rw-container "$rw_container" \
		--rw-kafka-brokers kafka:9092 \
		--fixture-kafka-brokers "127.0.0.1:${kafka_host_port}" \
		--kafka-container "$kafka_container" \
		--workdir "$work_dir" \
		--rows "$rows" \
		--queries "$queries" \
		--sink "$sink" \
		--no-cleanup "$no_cleanup"
	log "RisingWave Nexmark benchmark finished"
}

prepare_workspace
rw_host_port="$(find_free_port 4567 4667)"
kafka_host_port="$(find_free_port 9092 9192)"
ensure_network
start_kafka
start_risingwave
wait_for_risingwave_kafka_connectivity
run_bench

echo "report: $work_dir/report.md"
echo "json:   $work_dir/report.json"
echo "root:   $bench_root"
