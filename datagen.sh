#!/usr/bin/env bash

# 这个脚本负责一次性生成可复用的 Nexmark keyed bid dataset。
# 它会启动临时 Kafka，调用 `nexmark_fixture.py prepare` 使用 nexmark-flink 生成官方
# combined events，再抽取 bid 事件并写成稳定命名的 keyed JSONL 文件。

set -euo pipefail

usage() {
	cat <<'EOF'
生成可复用的 Nexmark keyed bid dataset。

Usage:
  datagen.sh [--dataset PATH] [--stats-output PATH] [--rows N] [--partitions N]
             [--bench-root DIR] [--no-cleanup]

Options:
  --dataset PATH       Output keyed JSONL dataset path. Default: ./nexmark_bid.keyed.jsonl
  --stats-output PATH  Output dataset stats JSON path. Default: ./nexmark_bid.stats.json
  --rows N             Target number of bid rows kept in the generated dataset.
  --partitions N       Number of logical keys used when writing the keyed dataset.
  --bench-root DIR     Temporary work root used while generating the dataset.
  --no-cleanup         Keep the temporary Kafka container and work directory.
  --help               Show this message.
EOF
}

project_root="$(cd "$(dirname "$0")" && pwd)"
cd "$project_root"

log() {
	printf '[%s UTC] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S')" "$1"
}

rows="1000000"
partitions="4"
dataset_path="$project_root/nexmark_bid.keyed.jsonl"
stats_output="$project_root/nexmark_bid.stats.json"
no_cleanup="0"
bench_root=""

while [[ $# -gt 0 ]]; do
	case "$1" in
	--dataset)
		dataset_path="$2"
		shift 2
		;;
	--stats-output)
		stats_output="$2"
		shift 2
		;;
	--rows)
		rows="$2"
		shift 2
		;;
	--partitions)
		partitions="$2"
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
	--help)
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
	bench_root="$(mktemp -d "${TMPDIR:-/tmp}/nexmark-datagen.XXXXXX")"
fi
work_dir="$bench_root/datagen"
run_id="$(basename "$bench_root" | tr -c '[:alnum:]' '-')"
kafka_container="nexmark-datagen-kafka-${run_id}"
kafka_host_port=""

cleanup_services() {
	if [[ "$no_cleanup" == "1" ]]; then
		return
	fi
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
	rm -rf "$work_dir"
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
	log "Preparing datagen workspace at $work_dir"
	rm -rf "$work_dir"
	mkdir -p "$work_dir"
}

start_kafka() {
	log "Starting Kafka container $kafka_container on host port $kafka_host_port"
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
	docker run -d --name "$kafka_container" \
		--label nexmark.bench.datagen=1 \
		-p "${kafka_host_port}:9092" \
		-e KAFKA_NODE_ID=1 \
		-e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:${kafka_host_port} \
		-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
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

run_datagen() {
	local dataset_abs stats_abs
	dataset_abs="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$dataset_path")"
	stats_abs="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$stats_output")"
	log "Generating keyed dataset into $dataset_abs"
	python3 ./nexmark_fixture.py prepare \
		--workdir "$work_dir" \
		--output "$dataset_abs" \
		--stats-output "$stats_abs" \
		--kafka-container "$kafka_container" \
		--kafka-brokers "127.0.0.1:${kafka_host_port}" \
		--rows "$rows" \
		--partitions "$partitions"
	log "Datagen finished"
}

prepare_workspace
kafka_host_port="$(find_free_port 9092 9192)"
start_kafka
run_datagen

echo "dataset: $dataset_path"
echo "stats:   $stats_output"
echo "root:    $bench_root"
