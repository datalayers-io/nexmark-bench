#!/usr/bin/env bash

# 这个脚本是 Datalayers Nexmark benchmark 的本地入口。
# 它负责准备临时目录、启动 Kafka，并通过 HTTP SQL 连接一个已经启动好的 Datalayers
# 实例。真正的 query 执行、指标统计和 report 生成都在 `runners/datalayers.py`
# 里完成。

set -euo pipefail

usage() {
	cat <<'EOF'
运行本地 Datalayers Nexmark benchmark。

Usage:
  datalayers.sh [--host HOST] [--port HTTP_PORT] [--dataset PATH]
                      [--queries q0,q1,q2,q14,q21,q22,q16,q17] [--sink table|blackhole]
                      [--bench-root DIR] [--timeout SEC] [--no-cleanup]
                      [--kafka-container KAFKA_CONTAINER]

参数:
  -h, --host HOST
      已启动 Datalayers HTTP SQL endpoint 的 host 地址。
      默认: 127.0.0.1

  -P, --port HTTP_PORT
      已启动 Datalayers HTTP SQL endpoint 的端口。
      默认: 8361

  --dataset PATH
      用于 Kafka preload 的 keyed JSONL dataset 路径。
      关联的 stats 文件会按同名规则自动推导并由 runner 读取。
      默认: ./nexmark_bid.keyed.jsonl

  --queries LIST
      逗号分隔的 query 列表。支持: q0,q1,q2,q14,q21,q22,q16,q17。
      Datalayers 暂不支持 q16,q17，runner 会自动跳过。
      默认: q0,q1,q2,q14,q21,q22,q16,q17

  --sink MODE
      `table` 创建表 sink，并通过行数判定完成。
      `blackhole` 创建 blackhole sink，并通过 kafka consumer lag 判定完成。
      默认: table

  --bench-root DIR
      benchmark 临时根目录。

  --timeout SEC
      每个 query 的完成等待超时时间，单位秒。
      默认: 600

  --no-cleanup
      保留 Kafka 容器以及 benchmark 创建的 database/source/pipeline/sink/table 等对象。
      未传时会在 bench 结束后执行 cleanup。

  --kafka-container KAFKA_CONTAINER
      复用已有的 Kafka 容器。
      传入一个正在运行的 Docker 容器名。
      runner 会检查 topic 中已有 message 数量是否与 dataset 行数匹配，
      若匹配则跳过 preload 直接开始 benchmark。
      未传时会自动创建并启动一个新的 Kafka 容器。

  --profile
      使用 perf 采集 Datalayers 进程的 CPU profile，
      并在 benchmark 结束后生成 flamegraph.svg。
      要求 Datalayers 以以下方式编译：
        RUSTFLAGS="-C force-frame-pointers" cargo build --profile reldev --bin datalayers

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
sink="table"
no_cleanup="0"
bench_root=""
dataset="$project_root/nexmark_bid.keyed.jsonl"
external_host="127.0.0.1"
external_port="8361"
timeout_sec="600"
kafka_container_arg=""
profile_mode=""

while [[ $# -gt 0 ]]; do
	case "$1" in
	-h | --host)
		external_host="$2"
		shift 2
		;;
	-P | --port)
		external_port="$2"
		shift 2
		;;
	--dataset)
		dataset="$2"
		shift 2
		;;
	--queries)
		queries="$2"
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
	--profile)
		profile_mode="1"
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

dataset="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$dataset")"
if [[ ! -f "$dataset" ]]; then
	echo "dataset does not exist: $dataset" >&2
	exit 1
fi

unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY

if [[ -z "$bench_root" ]]; then
	bench_root="$(mktemp -d "${TMPDIR:-/tmp}/nexmark-bench.XXXXXX")"
fi
work_dir="$bench_root/datalayers"
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
	kafka_container="datalayers-nexmark-kafka-${run_id}"
	kafka_host_port=""
	kafka_container_user=""
fi
engine_pid=""

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

trap 'cleanup_profile; cleanup_services' EXIT

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

PROFILE_PERF_PID=""

start_perf_profile() {
	local pid="$1"
	if ! command -v perf >/dev/null 2>&1; then
		log "ERROR: 'perf' not found. Install linux-tools-common or perf package."
		exit 1
	fi
	local perf_output="$work_dir/perf.data"
	log "Starting perf record on PID $pid ..."
	local perf_cmd="perf"
	PROFILE_PERF_USE_SUDO=""
	if ! perf record -o /dev/null -- sleep 1 2>/dev/null; then
		if sudo -n true 2>/dev/null; then
			perf_cmd="sudo perf"
			PROFILE_PERF_USE_SUDO=1
		else
			log "ERROR: perf requires sudo or /proc/sys/kernel/perf_event_paranoid=-1"
			exit 1
		fi
	fi
	$perf_cmd record -p "$pid" -g --call-graph fp -F 49 -o "$perf_output" &
	PROFILE_PERF_PID=$!
	sleep 2
	if ! kill -0 "$PROFILE_PERF_PID" 2>/dev/null; then
		log "ERROR: perf record failed to start"
		exit 1
	fi
	log "perf record started (pid $PROFILE_PERF_PID)"
}

stop_perf_profile() {
	if [[ -z "${PROFILE_PERF_PID:-}" ]]; then
		return
	fi
	log "Stopping perf record ..."
	sudo kill -INT "$PROFILE_PERF_PID" 2>/dev/null || kill -INT "$PROFILE_PERF_PID" 2>/dev/null || true
	wait "$PROFILE_PERF_PID" 2>/dev/null || true
	PROFILE_PERF_PID=""

	local perf_output="$work_dir/perf.data"
	local flame_svg="$work_dir/flamegraph.svg"

	if [[ "${PROFILE_PERF_USE_SUDO:-}" == "1" ]]; then
		sudo chown "$(id -u):$(id -g)" "$perf_output" 2>/dev/null || true
	fi
	if [[ ! -f "$perf_output" ]]; then
		log "WARNING: perf.data not found, skipping flamegraph generation"
		return
	fi
	log "Generating flamegraph from $perf_output ..."
	if command -v flamegraph >/dev/null 2>&1; then
		flamegraph --perfdata "$perf_output" -o "$flame_svg" --no-inline
	else
		local fg_dir="$work_dir/FlameGraph"
		if [[ ! -d "$fg_dir" ]]; then
			log "Downloading FlameGraph scripts ..."
			git clone --depth 1 https://github.com/brendangregg/FlameGraph.git "$fg_dir" 2>/dev/null || {
				log "ERROR: Failed to clone FlameGraph. Install flamegraph-rs: cargo install flamegraph"
				return
			}
		fi
		perf script -i "$perf_output" | "$fg_dir/stackcollapse-perf.pl" | "$fg_dir/flamegraph.pl" >"$flame_svg" || {
			log "ERROR: flamegraph generation failed"
			return
		}
	fi
	log "Flamegraph saved to $flame_svg"
}

cleanup_profile() {
	if [[ -n "${PROFILE_PERF_PID:-}" ]]; then
		sudo kill -INT "$PROFILE_PERF_PID" 2>/dev/null || kill -INT "$PROFILE_PERF_PID" 2>/dev/null || true
		wait "$PROFILE_PERF_PID" 2>/dev/null || true
		PROFILE_PERF_PID=""
	fi
	if [[ "${PROFILE_PERF_USE_SUDO:-}" == "1" ]] && [[ -f "$work_dir/perf.data" ]]; then
		sudo chown "$(id -u):$(id -g)" "$work_dir/perf.data" 2>/dev/null || true
	fi
}

prepare_workspace() {
	log "Preparing Datalayers benchmark workspace at $work_dir"
	pre_cleanup
	rm -rf "$work_dir"
	mkdir -p "$work_dir"
}

detect_engine_pid() {
	local pid=""
	if command -v lsof >/dev/null 2>&1; then
		pid="$(lsof -tiTCP:"$external_port" -sTCP:LISTEN 2>/dev/null | head -n 1 || true)"
	fi
	if [[ -z "$pid" ]] && command -v fuser >/dev/null 2>&1; then
		pid="$(fuser -n tcp "$external_port" 2>/dev/null | awk '{print $1}' || true)"
	fi
	if [[ -z "$pid" ]]; then
		pid="$(ss -ltnp "( sport = :$external_port )" 2>/dev/null | sed -n 's/.*pid=\([0-9][0-9]*\).*/\1/p' | head -n 1 || true)"
	fi
	if [[ -z "$pid" ]] && command -v pgrep >/dev/null 2>&1; then
		pid="$(ps -ef 2>/dev/null | awk '$8 ~ /(^|\/)datalayers$/ && $0 ~ / standalone( |$)/ {pid=$2} END {print pid}' || true)"
	fi
	if [[ -z "$pid" ]] && command -v pgrep >/dev/null 2>&1; then
		pid="$(pgrep -x datalayers | tail -n 1 || true)"
	fi
	if [[ -z "$pid" ]]; then
		pid="$(ps -ef 2>/dev/null | awk '/\/datalayers standalone/ && $8 !~ /sudo$/ && $2 ~ /^[0-9]+$/ {pid=$2} END {print pid}' || true)"
	fi
	if [[ -n "$pid" ]]; then
		log "Detected Datalayers listener PID $pid on port $external_port"
	else
		log "Could not detect a listener PID on port $external_port; CPU and memory stats will stay at 0"
	fi
	engine_pid="$pid"
}

start_kafka() {
	log "Starting Kafka container $kafka_container on host port $kafka_host_port"
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
	docker run -d --name "$kafka_container" \
		--label datalayers.nexmark.bench=1 \
		--label datalayers.nexmark.run_id="$run_id" \
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
	log "Running Datalayers Nexmark benchmark: host=$external_host port=$external_port dataset=$dataset queries=$queries sink=$sink"
	local cmd=(
		python3 ./runners/datalayers.py
		--host "$external_host"
		--port "$external_port"
		--kafka-brokers "127.0.0.1:${kafka_host_port}"
		--kafka-container "$kafka_container"
		--workdir "$work_dir"
		--dataset "$dataset"
		--queries "$queries"
		--sink "$sink"
		--timeout "$timeout_sec"
		--no-cleanup "$no_cleanup"
	)
	if [[ -n "$engine_pid" ]]; then
		cmd+=(--engine-pid "$engine_pid")
	fi
	"${cmd[@]}"
	log "Datalayers Nexmark benchmark finished"
}

if [[ -z "$kafka_container_arg" ]]; then
	kafka_host_port="$(find_free_port 9092 9192)"
	prepare_workspace
	detect_engine_pid
	start_kafka
else
	prepare_workspace
	detect_engine_pid
fi

if [[ "$profile_mode" == "1" ]]; then
	if [[ -z "$engine_pid" ]]; then
		log "ERROR: --profile requires a detectable Datalayers PID. Is Datalayers running on port $external_port?"
		exit 1
	fi
	start_perf_profile "$engine_pid"
fi

run_bench

if [[ "$profile_mode" == "1" ]]; then
	stop_perf_profile
fi

echo "report: $work_dir/report.md"
echo "json:   $work_dir/report.json"
if [[ "$profile_mode" == "1" ]]; then
	echo "flame:  $work_dir/flamegraph.svg"
fi
echo "root:   $bench_root"
