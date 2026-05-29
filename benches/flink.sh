#!/usr/bin/env bash

# 这个脚本是 Flink Nexmark benchmark 的本地入口。
# 它负责准备临时目录、启动 Kafka，并把 Kafka 地址、dataset 和工作目录参数传给
# `runners/flink.py`。Flink toolchain 准备、query 执行、指标统计和 report
# 生成都在 Python runner 里完成。

set -euo pipefail

usage() {
	cat <<'EOF'
运行本地 Flink Nexmark benchmark。

Usage:
  flink.sh [--dataset PATH] [--queries q0,q1,q2,q14,q21,q22,q16,q17] [--bench-root DIR] [--parallelism N] [--timeout SEC] [--no-cleanup] [--kafka-container KAFKA_CONTAINER]

参数:
  --dataset PATH
      用于 Kafka preload 的 keyed JSONL dataset 路径。
      关联的 stats 文件会按同名规则自动推导并由 runner 读取。
      默认: ./nexmark_bid.keyed.jsonl

  --queries LIST
      逗号分隔的 query 列表。支持: q0,q1,q2,q14,q21,q22,q16,q17。
      默认: q0,q1,q2,q14,q21,q22,q16,q17

  --bench-root DIR
      benchmark 临时根目录。

  --parallelism N
      Flink 任务并行度。
      默认: 1

  --timeout SEC
      每个 query 的完成等待超时时间，单位秒。
      默认: 600

  --no-cleanup
      保留 Kafka 容器。
      未传时会在 bench 结束后执行 cleanup。

  --kafka-container KAFKA_CONTAINER
      复用已有的 Kafka 容器。
      传入一个正在运行的 Docker 容器名。
      runner 会检查 topic 中已有 message 数量是否与 dataset 行数匹配，
      若匹配则跳过 preload 直接开始 benchmark。
      未传时会自动创建并启动一个新的 Kafka 容器。

  --help
      显示本帮助信息。
EOF
}

project_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$project_root"

log() {
	printf '[%s UTC] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S')" "$1"
}

queries="q0,q1,q2,q14,q21,q22,q16,q17"
parallelism="1"
no_cleanup="0"
bench_root=""
dataset="$project_root/nexmark_bid.keyed.jsonl"
timeout_sec="600"
kafka_container_arg=""

while [[ $# -gt 0 ]]; do
	case "$1" in
	--queries)
		queries="$2"
		shift 2
		;;
	--dataset)
		dataset="$2"
		shift 2
		;;
	--bench-root)
		bench_root="$2"
		shift 2
		;;
	--parallelism)
		parallelism="$2"
		shift 2
		;;
	--timeout)
		timeout_sec="$2"
		shift 2
		;;
	--no-cleanup)
		no_cleanup="1"
		shift
		;;
	--kafka-container)
		kafka_container_arg="$2"
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

dataset="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$dataset")"
if [[ ! -f "$dataset" ]]; then
	echo "dataset does not exist: $dataset" >&2
	exit 1
fi

unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY

if [[ -z "$bench_root" ]]; then
	bench_root="$(mktemp -d "${TMPDIR:-/tmp}/nexmark-bench.XXXXXX")"
fi
work_dir="$bench_root/flink"
run_id="$(basename "$bench_root" | tr -c '[:alnum:]' '-')"
if [[ -n "$kafka_container_arg" ]]; then
	if [[ "$(docker inspect -f '{{.State.Running}}' "$kafka_container_arg" 2>/dev/null)" != "true" ]]; then
		echo "Kafka container $kafka_container_arg is not running" >&2
		exit 1
	fi
	kafka_container="$kafka_container_arg"
	kafka_host_port=$(docker port "$kafka_container" 29092 2>/dev/null | head -1 | cut -d: -f2)
	if [[ -z "$kafka_host_port" ]]; then
		echo "Cannot detect Kafka port on container $kafka_container" >&2
		exit 1
	fi
	kafka_container_user="1"
else
	kafka_container="flink-nexmark-kafka-${run_id}"
	kafka_host_port=""
	kafka_container_user=""
fi

cleanup_services() {
	if [[ "$no_cleanup" == "1" ]]; then
		return
	fi
	if [[ -n "$kafka_container_user" ]]; then
		return
	fi
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
}

pre_cleanup() {
	if [[ -n "$kafka_container_user" ]]; then
		return
	fi
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
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
	# Keep per-run outputs isolated because Flink leaves logs and local state in the workdir.
	log "Preparing Flink benchmark workspace at $work_dir"
	pre_cleanup
	rm -rf "$work_dir"
	mkdir -p "$work_dir"
}

start_kafka() {
	# Kafka is recreated per run so the replay harness starts from a clean topic set.
	log "Starting Kafka container $kafka_container on host port $kafka_host_port"
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
	docker run -d --name "$kafka_container" \
		--label flink.nexmark.bench=1 \
		--label flink.nexmark.run_id="$run_id" \
		-p "${kafka_host_port}:29092" \
		-e KAFKA_NODE_ID=1 \
		-e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS=PLAINTEXT://:9092,PLAINTEXT_HOST://:29092,CONTROLLER://:9093 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092,PLAINTEXT_HOST://127.0.0.1:${kafka_host_port} \
		-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT \
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
	until docker logs "$kafka_container" 2>&1 | grep -q 'Kafka Server started'; do
		if ((SECONDS >= deadline)); then
			docker logs "$kafka_container" >&2 || true
			echo "timeout waiting for kafka broker readiness" >&2
			exit 1
		fi
		sleep 2
	done
	log "Kafka container $kafka_container is ready"
}

run_bench() {
	# The Python runner starts Flink, submits jobs and aggregates metrics.
	log "Running Flink Nexmark benchmark: dataset=$dataset queries=$queries parallelism=$parallelism"
	python3 ./runners/flink.py \
		--kafka-container "$kafka_container" \
		--kafka-port "$kafka_host_port" \
		--workdir "$work_dir" \
		--dataset "$dataset" \
		--queries "$queries" \
		--parallelism "$parallelism" \
		--timeout "$timeout_sec" \
		--no-cleanup "$no_cleanup"
	log "Flink Nexmark benchmark finished"
}

if [[ -z "$kafka_container_arg" ]]; then
	kafka_host_port="$(find_free_port 9092 9192)"
	prepare_workspace
	start_kafka
	run_bench
else
	prepare_workspace
	run_bench
fi

echo "report: $work_dir/report.md"
echo "json:   $work_dir/report.json"
echo "root:   $bench_root"
