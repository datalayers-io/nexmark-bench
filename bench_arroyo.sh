#!/usr/bin/env bash

# 这个脚本是 Arroyo Nexmark benchmark 的本地入口。
# 它负责准备临时目录、启动 Kafka 和 Arroyo 单节点容器，然后把连接参数、dataset 和
# 工作目录传给 `arroyo_bench_runner.py`。真正的 pipeline 提交、状态轮询、指标统计和
# report 生成都在 Python runner 里完成。

set -euo pipefail

usage() {
	cat <<'EOF'
运行本地 Arroyo Nexmark benchmark。

Usage:
  bench_arroyo.sh [--dataset PATH] [--queries q0,q1,q2,q14,q21,q22]
                  [--bench-root DIR] [--no-cleanup] [--image IMAGE]

参数:
  --dataset PATH
      用于 Kafka preload 的 keyed JSONL dataset 路径。
      关联的 stats 文件会按同名规则自动推导并由 runner 读取。
      默认: ./nexmark_bid.keyed.jsonl

  --queries LIST
      逗号分隔的 query 列表。支持: q0,q1,q2,q14,q21,q22。
      默认: q0,q1,q2,q14,q21,q22

  --bench-root DIR
      benchmark 临时根目录。

  --no-cleanup
      保留 Kafka、Arroyo 容器、network 和 benchmark 创建的 pipeline。
      未传时会在 bench 结束后执行 cleanup。

  --image IMAGE
      覆盖默认的 Arroyo 镜像。

  --help
      显示本帮助信息。
EOF
}

project_root="$(cd "$(dirname "$0")" && pwd)"
cd "$project_root"

log() {
	printf '[%s UTC] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S')" "$1"
}

queries="q0,q1,q2,q14,q21,q22"
no_cleanup="0"
bench_root=""
arroyo_image="${ARROYO_IMAGE:-ghcr.io/arroyosystems/arroyo:0.14.1}"
dataset="$project_root/nexmark_bid.keyed.jsonl"

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
	--no-cleanup)
		no_cleanup="1"
		shift
		;;
	--image)
		arroyo_image="$2"
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
work_dir="$bench_root/arroyo"
run_id="$(basename "$bench_root" | tr -c '[:alnum:]' '-')"
kafka_network="arroyo-nexmark-net-${run_id}"
kafka_container="arroyo-nexmark-kafka-${run_id}"
arroyo_container="arroyo-nexmark-${run_id}"
arroyo_host_port=""
kafka_host_port=""

cleanup_services() {
	if [[ "$no_cleanup" == "1" ]]; then
		return
	fi
	docker rm -f "$arroyo_container" >/dev/null 2>&1 || true
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
	log "Preparing Arroyo benchmark workspace at $work_dir"
	cleanup_services
	rm -rf "$work_dir"
	mkdir -p "$work_dir"
}

ensure_network() {
	docker network inspect "$kafka_network" >/dev/null 2>&1 || docker network create "$kafka_network" >/dev/null
}

start_kafka() {
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

start_arroyo() {
	log "Starting Arroyo container $arroyo_container on host port $arroyo_host_port"
	docker rm -f "$arroyo_container" >/dev/null 2>&1 || true
	docker run -d --name "$arroyo_container" \
		--network "$kafka_network" \
		--label datalayers.nexmark.bench=1 \
		--label datalayers.nexmark.run_id="$run_id" \
		-p "${arroyo_host_port}:5115" \
		"$arroyo_image" >/dev/null
	wait_for_port 127.0.0.1 "$arroyo_host_port" arroyo
	local deadline=$((SECONDS + 120))
	until curl -fsS "http://127.0.0.1:${arroyo_host_port}/api/v1/ping" >/dev/null 2>&1; do
		if ((SECONDS >= deadline)); then
			docker logs "$arroyo_container" >&2 || true
			echo "timeout waiting for arroyo api readiness" >&2
			exit 1
		fi
		sleep 2
	done
	log "Arroyo container $arroyo_container is ready"
}

run_bench() {
	log "Running Arroyo Nexmark benchmark: dataset=$dataset queries=$queries image=$arroyo_image"
	python3 ./arroyo_bench_runner.py \
		--host 127.0.0.1 \
		--port "$arroyo_host_port" \
		--kafka-brokers kafka:9092 \
		--kafka-container "$kafka_container" \
		--workdir "$work_dir" \
		--dataset "$dataset" \
		--queries "$queries" \
		--arroyo-container "$arroyo_container" \
		--no-cleanup "$no_cleanup"
	log "Arroyo Nexmark benchmark finished"
}

arroyo_host_port="$(find_free_port 5115 5215)"
kafka_host_port="$(find_free_port 9092 9192)"
prepare_workspace
ensure_network
start_kafka
start_arroyo
run_bench

echo "report: $work_dir/report.md"
echo "json:   $work_dir/report.json"
echo "root:   $bench_root"
