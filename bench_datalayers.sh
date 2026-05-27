#!/usr/bin/env bash

# 这个脚本是 Datalayers Nexmark benchmark 的本地入口。
# 它负责准备临时目录、启动 Kafka、启动 Datalayers standalone，然后把所有连接参数
# 传给 `datalayers_bench_runner.py`。真正的 query 执行、fixture 准备、指标统计和
# report 生成都在 Python runner 里完成。

set -euo pipefail

usage() {
	cat <<'EOF'
运行本地 Datalayers Nexmark benchmark。

Usage:
  bench_datalayers.sh --datalayers-path ABS_PATH [--rows N] [--queries q0,q1,q2,q14,q21,q22] [--sink table|blackhole]
                     [--skip-build] [--bench-root DIR] [--no-cleanup]
EOF
}

project_root="$(cd "$(dirname "$0")" && pwd)"
cd "$project_root"

log() {
	printf '[%s UTC] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S')" "$1"
}

rows="1000000"
queries="q0,q1,q2,q14,q21,q22"
sink="table"
skip_build="0"
no_cleanup="0"
bench_root=""
datalayers_path=""
datalayers_bin=""
dlsql_bin=""

while [[ $# -gt 0 ]]; do
	case "$1" in
	--datalayers-path)
		datalayers_path="$2"
		shift 2
		;;
	--rows)
		rows="$2"
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
	--skip-build)
		skip_build="1"
		shift
		;;
	--bench-root)
		bench_root="$2"
		shift 2
		;;
	--no-cleanup)
		no_cleanup="1"
		shift
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

if [[ -z "$datalayers_path" ]]; then
	echo "missing required argument: --datalayers-path" >&2
	usage >&2
	exit 1
fi
if [[ "${datalayers_path:0:1}" != "/" ]]; then
	echo "--datalayers-path must be an absolute path" >&2
	exit 1
fi
if [[ ! -d "$datalayers_path" ]]; then
	echo "datalayers path does not exist: $datalayers_path" >&2
	exit 1
fi

datalayers_bin="$datalayers_path/target/reldev/datalayers"
dlsql_bin="$datalayers_path/target/reldev/dlsql"

unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY ALL_PROXY

if [[ -z "$bench_root" ]]; then
	bench_root="$(mktemp -d "${TMPDIR:-/tmp}/nexmark-bench.XXXXXX")"
fi
work_dir="$bench_root/datalayers"
run_id="$(basename "$bench_root" | tr -c '[:alnum:]' '-')"
base_dir="$work_dir/base_dir"
config="$work_dir/standalone.toml"
log_file="$work_dir/datalayers.log"
pid_file="$work_dir/datalayers.pid"
kafka_container="datalayers-nexmark-kafka-${run_id}"
datalayers_port=""
kafka_host_port=""
datalayers_http_port=""
datalayers_prom_port=""
datalayers_pg_port=""

cleanup_datalayers() {
	if [[ -f "$pid_file" ]]; then
		pid="$(cat "$pid_file")"
		if [[ -n "$pid" ]] && kill -0 "$pid" >/dev/null 2>&1; then
			kill "$pid" >/dev/null 2>&1 || true
			sleep 1
			kill -9 "$pid" >/dev/null 2>&1 || true
		fi
	fi
	rm -f "$pid_file"
}

cleanup_services() {
	if [[ "$no_cleanup" == "1" ]]; then
		return
	fi
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
}

cleanup() {
	cleanup_datalayers
	cleanup_services
}

trap cleanup EXIT

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

ensure_port_free() {
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
	# Build an isolated workspace per run so logs, config and local state do not leak across runs.
	log "Preparing Datalayers benchmark workspace at $work_dir"
	cleanup_datalayers
	cleanup_services
	rm -rf "$work_dir"
	mkdir -p "$work_dir" "$base_dir"
	cp "$datalayers_path/.github/e2e-config/e2e-standalone-config.toml" "$config"
	escaped_base_dir="${base_dir//\//\\/}"
	sed -i \
		-e "s#^base_dir = .*#base_dir = \"$escaped_base_dir\"#" \
		-e "s#path = \"/var/lib/datalayers/wal\"#path = \"$escaped_base_dir/wal\"#" \
		-e "s#^addr = \"127.0.0.1:19360\"#addr = \"127.0.0.1:${datalayers_port}\"#" \
		-e "s#^http = \"127.0.0.1:19361\"#http = \"127.0.0.1:${datalayers_http_port}\"#" \
		-e "s#^addr = \"0.0.0.0:9090\"#addr = \"127.0.0.1:${datalayers_prom_port}\"#" \
		-e "s#^addr = \"0.0.0.0:5432\"#addr = \"127.0.0.1:${datalayers_pg_port}\"#" \
		"$config"
}

build_binaries() {
	if [[ -x "$datalayers_bin" && -x "$dlsql_bin" ]]; then
		log "Reusing existing Datalayers binaries from $datalayers_path/target/reldev"
		return
	fi
	if [[ "$skip_build" == "1" ]]; then
		echo "missing compiled binaries under $datalayers_path/target/reldev and --skip-build was set" >&2
		exit 1
	fi
	log "Building datalayers and dlsql with reldev profile under $datalayers_path"
	(
		cd "$datalayers_path"
		cargo build --profile reldev --bin datalayers --bin dlsql
	)
	log "Finished building Datalayers binaries"
}

start_kafka() {
	# Kafka is created per benchmark run because the replay harness relies on clean topics.
	log "Starting Kafka container $kafka_container on host port $kafka_host_port"
	docker rm -f "$kafka_container" >/dev/null 2>&1 || true
	docker run -d --name "$kafka_container" \
		--label datalayers.nexmark.bench=1 \
		--label datalayers.nexmark.run_id="$run_id" \
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

start_datalayers() {
	# The Python runner samples this process directly, so we keep its pid for later use.
	log "Starting Datalayers on port $datalayers_port"
	"$datalayers_bin" standalone -c "$config" >"$log_file" 2>&1 &
	echo "$!" >"$pid_file"
	wait_for_port 127.0.0.1 "$datalayers_port" datalayers
	log "Datalayers is ready on port $datalayers_port"
}

run_bench() {
	# The Python runner performs fixture preparation, per-query replay and metric aggregation.
	log "Running Datalayers Nexmark benchmark: rows=$rows queries=$queries sink=$sink"
	python3 ./datalayers_bench_runner.py \
		--dlsql "$dlsql_bin" \
		--host 127.0.0.1 \
		--port "$datalayers_port" \
		--kafka-brokers "127.0.0.1:${kafka_host_port}" \
		--kafka-container "$kafka_container" \
		--workdir "$work_dir" \
		--rows "$rows" \
		--queries "$queries" \
		--sink "$sink" \
		--no-cleanup "$no_cleanup" \
		--engine-pid "$(cat "$pid_file")"
	log "Datalayers Nexmark benchmark finished"
}

datalayers_port="$(ensure_port_free 19360 19460)"
datalayers_http_port="$(ensure_port_free 19461 19561)"
datalayers_prom_port="$(ensure_port_free 19562 19662)"
datalayers_pg_port="$(ensure_port_free 19663 19763)"
kafka_host_port="$(ensure_port_free 9092 9192)"
prepare_workspace
build_binaries
start_kafka
start_datalayers
run_bench

echo "report: $work_dir/report.md"
echo "json:   $work_dir/report.json"
echo "root:   $bench_root"
