#!/usr/bin/env bash

# 这个脚本是 Datalayers Nexmark benchmark 的本地入口。
# 它支持两种模式：
# 1. `--datalayers-path ABS_PATH`
#    复用或编译本地 Datalayers 仓库的 `target/reldev/datalayers` 和 `dlsql`，临时启动一套
#    standalone Datalayers，再跑 benchmark。
# 2. `-h HOST -P PORT`
#    直接连接已经启动好的 Datalayers HTTP SQL endpoint，不负责启动或清理该实例。

set -euo pipefail

usage() {
	cat <<'EOF'
运行本地 Datalayers Nexmark benchmark。

Usage:
  bench_datalayers.sh --datalayers-path ABS_PATH [--dataset PATH] [--queries q0,q1,q2,q14,q21,q22]
                      [--sink table|blackhole] [--skip-build] [--bench-root DIR] [--no-cleanup]

  bench_datalayers.sh -h HOST -P HTTP_PORT [--dataset PATH] [--queries q0,q1,q2,q14,q21,q22]
                      [--sink table|blackhole] [--bench-root DIR]

参数:
  --datalayers-path ABS_PATH
      本地 Datalayers 仓库的绝对路径。指定后脚本会检查
      `<ABS_PATH>/target/reldev/datalayers` 和 `<ABS_PATH>/target/reldev/dlsql`。
      如果二进制缺失且没有传 `--skip-build`，脚本会执行
      `cargo build --profile reldev --bin datalayers --bin dlsql`。

  -h, --host HOST
      已启动 Datalayers HTTP SQL endpoint 的 host 地址。
      必须与 `-P, --port` 一起指定。
      不能与 `--datalayers-path` 同时使用。

  -P, --port HTTP_PORT
      已启动 Datalayers 实例的 HTTP 端口。
      必须与 `-h, --host` 一起指定。
      不能与 `--datalayers-path` 同时使用。

  --dataset PATH
      用于 Kafka preload 的 keyed JSONL dataset 路径。
      默认: ./nexmark_bid.keyed.jsonl

  --queries LIST
      逗号分隔的 query 列表。支持: q0,q1,q2,q14,q21,q22。

  --sink MODE
      `table` 通过 sink table 的插入行数判定完成。
      `blackhole` 通过 Kafka consumer group lag 归零判定完成。

  --skip-build
      仅在 `--datalayers-path` 模式下有效。
      即使 `target/reldev` 二进制缺失，也不自动编译。

  --bench-root DIR
      benchmark 临时根目录。

  --no-cleanup
      仅在 `--datalayers-path` 模式下有意义。
      运行结束后保留 Kafka、临时启动的 Datalayers 进程和 benchmark 创建的 SQL 对象。

  --help
      显示本帮助信息。
EOF
}

project_root="$(cd "$(dirname "$0")" && pwd)"
cd "$project_root"

RESET=$'\033[0m'
YELLOW=$'\033[33m'

log() {
	printf '[%s UTC] %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S')" "$1"
}

log_yellow() {
	printf '%s[%s UTC] %s%s\n' "$YELLOW" "$(date -u '+%Y-%m-%d %H:%M:%S')" "$1" "$RESET"
}

queries="q0,q1,q2,q14,q21,q22"
sink="table"
skip_build="0"
no_cleanup="0"
bench_root=""
datalayers_path=""
datalayers_bin=""
dlsql_bin=""
dataset="$project_root/nexmark_bid.keyed.jsonl"
external_host="localhost"
external_port="8361"
host_set="0"
port_set="0"

while [[ $# -gt 0 ]]; do
	case "$1" in
	--datalayers-path)
		datalayers_path="$2"
		shift 2
		;;
	-h | --host)
		external_host="$2"
		host_set="1"
		shift 2
		;;
	-P | --port)
		external_port="$2"
		port_set="1"
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

if [[ -n "$datalayers_path" && ("$host_set" == "1" || "$port_set" == "1") ]]; then
	echo "--datalayers-path cannot be used together with -h/--host or -P/--port" >&2
	exit 1
fi
if [[ -z "$datalayers_path" && "$host_set" != "$port_set" ]]; then
	echo "-h/--host and -P/--port must be specified together" >&2
	exit 1
fi
if [[ -z "$datalayers_path" && "$host_set" == "0" ]]; then
	echo "either --datalayers-path or (-h/--host and -P/--port) must be specified" >&2
	usage >&2
	exit 1
fi
if [[ -n "$datalayers_path" && "${datalayers_path:0:1}" != "/" ]]; then
	echo "--datalayers-path must be an absolute path" >&2
	exit 1
fi
if [[ -n "$datalayers_path" && ! -d "$datalayers_path" ]]; then
	echo "datalayers path does not exist: $datalayers_path" >&2
	exit 1
fi
if [[ -n "$datalayers_path" ]]; then
	datalayers_bin="$datalayers_path/target/reldev/datalayers"
	dlsql_bin="$datalayers_path/target/reldev/dlsql"
fi
dataset="$(python3 -c 'import os,sys; print(os.path.abspath(sys.argv[1]))' "$dataset")"
if [[ ! -f "$dataset" ]]; then
	echo "dataset does not exist: $dataset" >&2
	exit 1
fi
if [[ -n "$datalayers_path" && "$host_set" == "0" && "$no_cleanup" != "0" ]]; then
	log "No-cleanup mode enabled for locally started Datalayers"
fi

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
	if [[ -n "$datalayers_path" ]]; then
		cleanup_datalayers
	fi
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
	log "Preparing Datalayers benchmark workspace at $work_dir"
	if [[ -n "$datalayers_path" ]]; then
		cleanup_datalayers
	fi
	cleanup_services
	rm -rf "$work_dir"
	mkdir -p "$work_dir" "$base_dir"
	if [[ -n "$datalayers_path" ]]; then
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
		log_yellow "Using Datalayers config file: $config"
	fi
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
	log "Starting Datalayers on SQL port $datalayers_port and HTTP port $datalayers_http_port"
	"$datalayers_bin" standalone -c "$config" >"$log_file" 2>&1 &
	echo "$!" >"$pid_file"
	wait_for_port 127.0.0.1 "$datalayers_port" datalayers-sql
	wait_for_port 127.0.0.1 "$datalayers_http_port" datalayers-http
	log "Datalayers is ready"
}

run_bench_local() {
	log "Running Datalayers Nexmark benchmark against local process: dataset=$dataset queries=$queries sink=$sink"
	python3 ./datalayers_bench_runner.py \
		--sql-mode dlsql \
		--dlsql "$dlsql_bin" \
		--host 127.0.0.1 \
		--port "$datalayers_port" \
		--kafka-brokers "127.0.0.1:${kafka_host_port}" \
		--kafka-container "$kafka_container" \
		--workdir "$work_dir" \
		--dataset "$dataset" \
		--queries "$queries" \
		--sink "$sink" \
		--no-cleanup "$no_cleanup" \
		--engine-pid "$(cat "$pid_file")"
}

run_bench_remote() {
	log "Running Datalayers Nexmark benchmark against existing HTTP endpoint: host=$external_host port=$external_port dataset=$dataset queries=$queries sink=$sink"
	no_cleanup="1"
	python3 ./datalayers_bench_runner.py \
		--sql-mode http \
		--http-host "$external_host" \
		--http-port "$external_port" \
		--kafka-brokers "127.0.0.1:${kafka_host_port}" \
		--kafka-container "$kafka_container" \
		--workdir "$work_dir" \
		--dataset "$dataset" \
		--queries "$queries" \
		--sink "$sink" \
		--no-cleanup 1
}

if [[ -n "$datalayers_path" ]]; then
	datalayers_port="$(ensure_port_free 19360 19460)"
	datalayers_http_port="$(ensure_port_free 19461 19561)"
	datalayers_prom_port="$(ensure_port_free 19562 19662)"
	datalayers_pg_port="$(ensure_port_free 19663 19763)"
fi
kafka_host_port="$(ensure_port_free 9092 9192)"
prepare_workspace
if [[ -n "$datalayers_path" ]]; then
	build_binaries
fi
start_kafka
if [[ -n "$datalayers_path" ]]; then
	start_datalayers
	run_bench_local
else
	run_bench_remote
fi

echo "report: $work_dir/report.md"
echo "json:   $work_dir/report.json"
echo "root:   $bench_root"
