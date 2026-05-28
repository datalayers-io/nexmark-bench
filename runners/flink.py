#!/usr/bin/env python3
"""
这个 bench runner 用来在本地 standalone Flink 上运行一组固定的 Nexmark 风格查询。

和另外两个 runner 一样，它不会直接使用手工造数，而是消费一个已经预先准备好的
keyed bid JSONL dataset。当前 Flink runner 固定使用 blackhole sink。

执行方式：

1. `benches/flink.sh` 启动 Kafka，并把 Kafka 地址和工作目录传入本文件。
2. 本文件准备 Flink toolchain，并读取与 dataset 关联的 stats JSON，拿到输入行数和几个 query 的理论输出行数。
3. 对每个 query：
   - 重建独立的 Kafka topic
   - 把 keyed dataset preload 到 topic
   - 启动本地 Flink standalone cluster
   - 创建 source/sink table 并提交 insert job
   - 在 replay 窗口内采样 Flink 相关 Java 进程的 CPU 和 RSS
   - 通过 Kafka consumer group lag 到 0 判定 source 已消费完输入
   - 主动 cancel 该 job 并收集结果
4. 每个 query 的结果会写入 `report.md`、`report.json` 和采样 CSV。

当前统计的指标：

- `replay_sec`
  指从提交本轮 query 的 source/sink/insert job，到 lag 为 0 并结束 job 为止的耗时。
- `throughput_rps`
  定义为 `input_rows / replay_sec`。
- `avg_cpu_percent`
  replay 窗口内，对 Flink 相关 Java 进程做周期性 `ps` 采样后取平均值。
- `avg_mem_gib`
  replay 窗口内，对 Flink 相关 Java 进程 RSS 做周期性 `ps` 采样后取平均值，并换算为 GiB。
- `kafka_preload_sec`
  本轮输入 preload 到 Kafka 的耗时，单独记录，不计入 throughput 分母。

注意：

- Flink SQL source 在当前这套实现里不是 bounded source，因此这里的“完成”是
  `lag == 0` 之后再主动 cancel job，而不是等待 job 自然 FINISHED。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nexmark_fixture import load_bid_dataset_stats, prepare_flink_toolchain


RESET = "\033[0m"
GREEN = "\033[32m"
FLINK_JAVA_OPTS = (
    "--add-exports=java.base/sun.net.util=ALL-UNNAMED "
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens=java.base/java.text=ALL-UNNAMED "
    "--add-opens=java.base/java.time=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED"
)


@dataclass
class QuerySpec:
    name: str
    sink_columns: str
    insert_sql: str
    expected_rows: Callable[[dict[str, int]], int]


QUERY_SPECS: dict[str, QuerySpec] = {
    "q0": QuerySpec(
        name="q0",
        sink_columns="""
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel STRING,
            url STRING,
            ts TIMESTAMP(3)
        """,
        insert_sql="""
            INSERT INTO {sink}
            SELECT auction, bidder, price, channel, url, ts
            FROM {source}
        """,
        expected_rows=lambda stats: stats["total_rows"],
    ),
    "q1": QuerySpec(
        name="q1",
        sink_columns="""
            auction BIGINT,
            bidder BIGINT,
            price DOUBLE,
            channel STRING,
            url STRING,
            ts TIMESTAMP(3)
        """,
        insert_sql="""
            INSERT INTO {sink}
            SELECT auction, bidder, CAST(price AS DOUBLE) * 0.89 AS price_eur, channel, url, ts
            FROM {source}
        """,
        expected_rows=lambda stats: stats["total_rows"],
    ),
    "q2": QuerySpec(
        name="q2",
        sink_columns="""
            auction BIGINT,
            price BIGINT,
            ts TIMESTAMP(3)
        """,
        insert_sql="""
            INSERT INTO {sink}
            SELECT auction, price, ts
            FROM {source}
            WHERE auction IN (1007, 1020, 2001, 2019, 2087)
        """,
        expected_rows=lambda stats: stats["q2_expected_rows"],
    ),
    "q14": QuerySpec(
        name="q14",
        sink_columns="""
            auction BIGINT,
            bidder BIGINT,
            price DOUBLE,
            bid_time_type STRING,
            ts TIMESTAMP(3),
            extra STRING
        """,
        insert_sql="""
            INSERT INTO {sink}
            SELECT
              auction,
              bidder,
              CAST(price AS DOUBLE) * 0.908 AS price,
              CASE
                WHEN EXTRACT(HOUR FROM ts) >= 8 AND EXTRACT(HOUR FROM ts) <= 18 THEN 'dayTime'
                WHEN EXTRACT(HOUR FROM ts) <= 6 OR EXTRACT(HOUR FROM ts) >= 20 THEN 'nightTime'
                ELSE 'otherTime'
              END AS bid_time_type,
              ts,
              extra
            FROM {source}
            WHERE CAST(price AS DOUBLE) * 0.908 > 1000000
              AND CAST(price AS DOUBLE) * 0.908 < 50000000
        """,
        expected_rows=lambda stats: stats["q14_expected_rows"],
    ),
    "q21": QuerySpec(
        name="q21",
        sink_columns="""
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel STRING,
            channel_id STRING
        """,
        insert_sql="""
            INSERT INTO {sink}
            SELECT
              auction,
              bidder,
              price,
              channel,
              CASE
                WHEN LOWER(channel) = 'apple' THEN '0'
                WHEN LOWER(channel) = 'google' THEN '1'
                WHEN LOWER(channel) = 'facebook' THEN '2'
                WHEN LOWER(channel) = 'baidu' THEN '3'
                ELSE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2)
              END AS channel_id
            FROM {source}
            WHERE REGEXP_EXTRACT(url, '(&|^)channel_id=([^&]*)', 2) IS NOT NULL
               OR LOWER(channel) IN ('apple', 'google', 'facebook', 'baidu')
        """,
        expected_rows=lambda stats: stats["q21_expected_rows"],
    ),
    "q22": QuerySpec(
        name="q22",
        sink_columns="""
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel STRING,
            dir1 STRING,
            dir2 STRING,
            dir3 STRING
        """,
        insert_sql="""
            INSERT INTO {sink}
            SELECT
              auction,
              bidder,
              price,
              channel,
              SPLIT_INDEX(url, '/', 3) AS dir1,
              SPLIT_INDEX(url, '/', 4) AS dir2,
              SPLIT_INDEX(url, '/', 5) AS dir3
            FROM {source}
        """,
        expected_rows=lambda stats: stats["total_rows"],
    ),
    "q16": QuerySpec(
        name="q16",
        sink_columns="""
            channel STRING,
            total_bids BIGINT,
            min_price BIGINT,
            max_price BIGINT,
            avg_price DOUBLE,
            distinct_bidders BIGINT,
            distinct_auctions BIGINT
        """,
        insert_sql="""
            INSERT INTO {sink}
            SELECT
                channel,
                count(*) AS total_bids,
                min(price) AS min_price,
                max(price) AS max_price,
                CAST(avg(price) AS DOUBLE) AS avg_price,
                count(distinct bidder) AS distinct_bidders,
                count(distinct auction) AS distinct_auctions
            FROM {source}
            GROUP BY channel
        """,
        expected_rows=lambda stats: stats["q16_expected_rows"],
    ),
    "q17": QuerySpec(
        name="q17",
        sink_columns="""
            auction BIGINT,
            bid_count BIGINT,
            min_price BIGINT,
            max_price BIGINT,
            avg_price DOUBLE,
            sum_price BIGINT,
            distinct_bidders BIGINT
        """,
        insert_sql="""
            INSERT INTO {sink}
            SELECT
                auction,
                count(*) AS bid_count,
                min(price) AS min_price,
                max(price) AS max_price,
                CAST(avg(price) AS DOUBLE) AS avg_price,
                sum(price) AS sum_price,
                count(distinct bidder) AS distinct_bidders
            FROM {source}
            GROUP BY auction
        """,
        expected_rows=lambda stats: stats["q17_expected_rows"],
    ),
}


class BenchError(RuntimeError):
    pass


def log(message: str, *, color: str | None = None) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if color is None:
        print(f"[{now} UTC] {message}", flush=True)
    else:
        print(f"{color}[{now} UTC] {message}{RESET}", flush=True)


class MultiProcessMonitor:
    def __init__(
        self,
        pid_supplier: Callable[[], list[int]],
        output_csv: Path,
        sample_interval: float,
    ):
        self.pid_supplier = pid_supplier
        self.output_csv = output_csv
        self.sample_interval = sample_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self.output_csv.parent.mkdir(parents=True, exist_ok=True)
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        with self.output_csv.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["ts_epoch", "cpu_percent", "rss_kib"])
            while not self._stop.is_set():
                pids = self.pid_supplier()
                if pids:
                    sample = read_process_sample(pids)
                    if sample is not None:
                        writer.writerow(sample)
                        fh.flush()
                time.sleep(self.sample_interval)


def read_process_sample(pids: list[int]) -> tuple[float, float, int] | None:
    pid_list = ",".join(str(pid) for pid in pids)
    result = subprocess.run(
        ["ps", "-p", pid_list, "-o", "%cpu=", "-o", "rss="],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if result.returncode != 0:
        return None
    total_cpu = 0.0
    total_rss = 0
    count = 0
    for line in result.stdout.splitlines():
        fields = line.strip().split()
        if len(fields) != 2:
            continue
        count += 1
        total_cpu += float(fields[0])
        total_rss += int(fields[1])
    if count == 0:
        return None
    return (time.time(), total_cpu, total_rss)


def summarize_samples(csv_path: Path) -> tuple[float, float]:
    cpu_values: list[float] = []
    rss_values: list[int] = []
    if not csv_path.exists():
        return 0.0, 0.0
    with csv_path.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cpu_values.append(float(row["cpu_percent"]))
            rss_values.append(int(row["rss_kib"]))
    if not cpu_values:
        return 0.0, 0.0
    avg_cpu = sum(cpu_values) / len(cpu_values)
    avg_mem_gib = (sum(rss_values) / len(rss_values)) / 1024 / 1024
    return avg_cpu, avg_mem_gib


def run_cmd(
    cmd: list[str],
    *,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout: int = 300,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        cwd=str(cwd) if cwd is not None else None,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise BenchError(
            result.stderr.strip() or result.stdout.strip() or f"command failed: {cmd}"
        )
    return result


def ensure_topic(container: str, topic: str, partitions: int) -> None:
    # Recreate the topic for every query to avoid mixing rows and consumer offsets across runs.
    log(f"Recreating Kafka topic {topic} with {partitions} partitions")
    for cmd in (
        [
            "docker",
            "exec",
            container,
            "kafka-topics",
            "--bootstrap-server",
            "127.0.0.1:9092",
            "--delete",
            "--if-exists",
            "--topic",
            topic,
        ],
        [
            "docker",
            "exec",
            container,
            "kafka-topics",
            "--bootstrap-server",
            "127.0.0.1:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
            "--config",
            "cleanup.policy=delete",
            "--config",
            "retention.ms=3600000",
        ],
    ):
        run_cmd(cmd, timeout=60)
        time.sleep(1)


def load_topic(container: str, topic: str, dataset_path: Path) -> float:
    log(f"Starting Kafka preload for topic {topic} from {dataset_path}")
    start = time.time()
    with dataset_path.open("rb") as fh:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "-i",
                container,
                "bash",
                "-lc",
                (
                    "kafka-console-producer --bootstrap-server 127.0.0.1:9092 "
                    f"--topic {topic} --property parse.key=true --property key.separator=$'\\t'"
                ),
            ],
            stdin=fh,
            capture_output=True,
            timeout=3600,
        )
    if result.returncode != 0:
        raise BenchError(
            result.stderr.decode("utf-8", errors="ignore")
            or result.stdout.decode("utf-8", errors="ignore")
            or "failed to load topic"
        )
    elapsed = time.time() - start
    log(f"Finished Kafka preload for topic {topic} in {elapsed:.3f}s")
    return elapsed


def wait_for_http(port: int, timeout: int = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/overview", timeout=2):
                return
        except Exception:
            time.sleep(1)
    raise BenchError(f"timeout waiting for Flink REST on 127.0.0.1:{port}")


def job_state(rest_port: int, job_id: str) -> str:
    with urllib.request.urlopen(
        f"http://127.0.0.1:{rest_port}/jobs/{job_id}", timeout=5
    ) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return str(payload.get("state", "UNKNOWN"))


def wait_for_job(rest_port: int, job_id: str, timeout: int = 600) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        state = job_state(rest_port, job_id)
        if state in {"FINISHED", "FAILED", "CANCELED"}:
            return state
        time.sleep(1)
    raise BenchError(f"timeout waiting for Flink job {job_id}")


def cancel_job(rest_port: int, job_id: str) -> None:
    req = urllib.request.Request(
        f"http://127.0.0.1:{rest_port}/jobs/{job_id}?mode=cancel",
        method="PATCH",
    )
    try:
        urllib.request.urlopen(req, timeout=5).read()
    except Exception:
        pass


def parse_job_id(output: str) -> str:
    match = re.search(r"Job ID:\s*([A-Za-z0-9]+)", output)
    if not match:
        raise BenchError(f"failed to parse Flink job id from output:\n{output}")
    return match.group(1)


def read_job_id_from_proc(proc: subprocess.Popen[str], timeout: int) -> str:
    deadline = time.time() + timeout
    accumulated = ""
    while time.time() < deadline:
        line = proc.stdout.readline()
        if line:
            accumulated += line
        match = re.search(r"Job ID:\s*([A-Za-z0-9]+)", accumulated)
        if match:
            return match.group(1)
        if proc.poll() is not None:
            break
        time.sleep(0.1)
    if proc.poll() is not None:
        stderr_text = proc.stderr.read() if proc.stderr else ""
        raise BenchError(
            f"sql-client exited early (rc={proc.returncode}):\n{accumulated}\n{stderr_text}"
        )
    raise BenchError(
        f"timeout waiting for Flink job id from sql-client:\n{accumulated}"
    )


def kafka_group_lag(container: str, group: str, topic: str) -> int | None:
    result = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "kafka-consumer-groups",
            "--bootstrap-server",
            "127.0.0.1:9092",
            "--describe",
            "--group",
            group,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if result.returncode != 0:
        return None
    total = 0
    seen = False
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 6 or fields[1] != topic:
            continue
        seen = True
        lag_text = fields[-1]
        if lag_text == "-":
            continue
        total += int(lag_text)
    return total if seen else None


def wait_for_group_lag_zero(
    container: str, group: str, topic: str, timeout: int
) -> None:
    # For Flink we treat "source consumed all preloaded rows" as the end of the replay phase.
    deadline = time.time() + timeout
    while time.time() < deadline:
        lag = kafka_group_lag(container, group, topic)
        if lag == 0:
            return
        time.sleep(1)
    raise BenchError(
        f"timeout waiting for kafka group {group} lag to reach 0 on topic {topic}"
    )


def create_runtime(
    workdir: Path, parallelism: int, rest_port: int
) -> tuple[Path, Path]:
    cache_root = Path.home() / ".cache" / "flink-nexmark"
    flink_home, nexmark_home = prepare_flink_toolchain(cache_root)
    runtime_root = workdir / "flink_runtime"
    if runtime_root.exists():
        shutil.rmtree(runtime_root)
    runtime_root.mkdir(parents=True, exist_ok=True)
    runtime_flink = runtime_root / "flink"
    runtime_nexmark = runtime_root / "nexmark-flink"
    shutil.copytree(flink_home, runtime_flink, symlinks=True)
    shutil.copytree(nexmark_home, runtime_nexmark, symlinks=True)
    conf = runtime_flink / "conf" / "flink-conf.yaml"
    lines = conf.read_text(encoding="utf-8").splitlines()
    new_lines: list[str] = []
    found_rest = False
    for line in lines:
        if line.startswith("rest.port:"):
            found_rest = True
            new_lines.append(f"rest.port: {rest_port}")
        elif line.startswith("rest.address:"):
            new_lines.append("rest.address: localhost")
        elif line.startswith("jobmanager.rpc.address:"):
            new_lines.append("jobmanager.rpc.address: localhost")
        elif line.startswith("parallelism.default:"):
            new_lines.append(f"parallelism.default: {parallelism}")
        elif line.startswith("taskmanager.numberOfTaskSlots:"):
            new_lines.append(f"taskmanager.numberOfTaskSlots: {parallelism}")
        else:
            new_lines.append(line)
    if not found_rest:
        new_lines.append(f"rest.port: {rest_port}")
    conf.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    return runtime_flink, runtime_nexmark


def flink_pids(runtime_flink: Path) -> list[int]:
    pids: list[int] = []
    log_dir = runtime_flink / "log"
    for pid_file in log_dir.glob("*.pid"):
        try:
            pids.append(int(pid_file.read_text(encoding="utf-8").strip()))
        except Exception:
            continue
    return pids


def render_sql(topic: str, query: QuerySpec, kafka_port: int) -> str:
    source = f"bid_src_{query.name}"
    sink = f"discard_sink_{query.name}"
    return (
        f"""
SET 'execution.runtime-mode' = 'streaming';
DROP TABLE IF EXISTS {source};
DROP TABLE IF EXISTS {sink};
CREATE TABLE {source} (
    auction BIGINT,
    bidder BIGINT,
    price BIGINT,
    channel STRING,
    url STRING,
    ts TIMESTAMP(3),
    extra STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '{topic}',
    'properties.bootstrap.servers' = '127.0.0.1:{kafka_port}',
    'properties.group.id' = 'flink-{query.name}',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);
CREATE TABLE {sink} (
    {query.sink_columns}
) WITH (
    'connector' = 'blackhole'
);
{query.insert_sql.format(source=source, sink=sink)}
""".strip()
        + "\n"
    )


def markdown_report(
    args: argparse.Namespace,
    dataset_stats: dict[str, int],
    results: list[dict[str, object]],
) -> str:
    # Keep the report readable on its own because Flink runs often leave multiple temp
    # directories behind for later inspection.
    lines = [
        "# Flink Nexmark Benchmark Report",
        "",
        f"- Generated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"- Kafka brokers: `127.0.0.1:{args.kafka_port}`",
        f"- Task parallelism: `{args.parallelism}`",
        f"- Fixture: `official keyed bid dataset`",
        f"- Sink mode: `blackhole`",
        f"- Input rows: `{dataset_stats['total_rows']}`",
        f"- Dataset path: `{Path(args.dataset).resolve()}`",
        "",
        "| Query | Input Rows | Expected Rows | Replay Seconds | Throughput (input records/s) | Avg CPU (%) | Avg Mem (GiB) | Kafka Preload Seconds | Final State |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in results:
        lines.append(
            "| {query} | {input_rows} | {expected_rows} | {replay_sec:.3f} | {throughput_rps:.1f} | {avg_cpu_percent:.2f} | {avg_mem_gib:.3f} | {kafka_preload_sec:.3f} | {state} |".format(
                **item
            )
        )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="针对预先生成好的 keyed bid JSONL dataset 运行 Flink Nexmark benchmark。",
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="help", help="显示本帮助信息并退出。")
    parser._positionals.title = "位置参数"
    parser._optionals.title = "可选参数"
    parser.add_argument(
        "--workdir",
        default=".flink-nexmark",
        help="报告、SQL 文件和临时 Flink runtime 的工作目录。",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="用于 Kafka preload 的 keyed JSONL dataset 路径。",
    )
    parser.add_argument(
        "--partitions", type=int, default=4, help="Kafka topic 分区数。"
    )
    parser.add_argument(
        "--queries",
        default="q0,q1,q2,q14,q21,q22",
        help="逗号分隔的 query 列表。",
    )
    parser.add_argument(
        "--kafka-container",
        required=True,
        help="用于管理 topic 和 preload 数据的 Kafka 容器名。",
    )
    parser.add_argument(
        "--kafka-port",
        type=int,
        default=9092,
        help="暴露给本地 Flink runtime 使用的宿主机 Kafka 端口。",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
        help="Flink 任务并行度。",
    )
    parser.add_argument(
        "--no-cleanup",
        type=int,
        choices=[0, 1],
        default=0,
        help="设为 1 时保留 Flink runtime 目录和 Kafka 数据。",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="每个 query 的完成等待超时时间，单位秒。",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=1.0,
        help="CPU 和 RSS 采样间隔，单位秒。",
    )
    return parser.parse_args()


def find_free_port(start: int, end: int) -> int:
    import socket

    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return port
    raise BenchError(f"cannot find a free port in range {start}-{end}")


def main() -> int:
    args = parse_args()
    log(
        f"Starting Flink Nexmark benchmark: dataset={args.dataset} queries={args.queries} "
        f"workdir={args.workdir} kafka_port={args.kafka_port} parallelism={args.parallelism}"
    )
    workdir = Path(args.workdir).resolve()
    report_md = workdir / "report.md"
    report_json = workdir / "report.json"
    workdir.mkdir(parents=True, exist_ok=True)

    queries = []
    for name in [item.strip() for item in args.queries.split(",") if item.strip()]:
        spec = QUERY_SPECS.get(name)
        if spec is None:
            raise BenchError(f"unsupported query: {name}")
        queries.append(spec)

    dataset_path = Path(args.dataset).resolve()
    dataset_stats = load_bid_dataset_stats(dataset_path)
    fixture_metadata = {"dataset_path": str(dataset_path)}
    rest_port = find_free_port(18081, 18181)
    runtime_flink, _runtime_nexmark = create_runtime(
        workdir, args.parallelism, rest_port
    )
    env = dict(os.environ)
    env["FLINK_HOME"] = str(runtime_flink)
    env["JAVA_TOOL_OPTIONS"] = FLINK_JAVA_OPTS
    env["_JAVA_OPTIONS"] = FLINK_JAVA_OPTS

    try:
        run_cmd(
            [str(runtime_flink / "bin" / "start-cluster.sh")],
            cwd=runtime_flink,
            env=env,
            timeout=120,
        )
        wait_for_http(rest_port, timeout=60)
        results: list[dict[str, object]] = []
        for query in queries:
            log(f"Starting benchmark for query {query.name}", color=GREEN)
            topic = f"nexmark_{query.name}"
            ensure_topic(args.kafka_container, topic, args.partitions)
            kafka_preload_sec = load_topic(args.kafka_container, topic, dataset_path)
            sql_file = workdir / f"{query.name}.sql"
            sql_file.write_text(
                render_sql(topic, query, args.kafka_port), encoding="utf-8"
            )
            sample_csv = workdir / f"{query.name}_samples.csv"
            monitor = MultiProcessMonitor(
                lambda: flink_pids(runtime_flink), sample_csv, args.sample_interval
            )
            sql_proc = None
            try:
                sql_proc = subprocess.Popen(
                    [
                        str(runtime_flink / "bin" / "sql-client.sh"),
                        "embedded",
                        "-f",
                        str(sql_file),
                    ],
                    cwd=workdir,
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                job_id = read_job_id_from_proc(sql_proc, 120)
                log(f"Flink job submitted: {job_id}")
                replay_t0 = time.time()
                monitor.start()
                wait_for_group_lag_zero(
                    args.kafka_container, f"flink-{query.name}", topic, args.timeout
                )
                cancel_job(rest_port, job_id)
                state = wait_for_job(rest_port, job_id, timeout=args.timeout)
            finally:
                if sql_proc is not None:
                    try:
                        sql_proc.terminate()
                        sql_proc.wait(timeout=10)
                    except Exception:
                        sql_proc.kill()
                monitor.stop()
            replay_sec = time.time() - replay_t0
            avg_cpu_percent, avg_mem_gib = summarize_samples(sample_csv)
            log(
                f"Finished benchmark for query {query.name}: state={state} replay_sec={replay_sec:.3f} "
                f"throughput={dataset_stats['total_rows'] / replay_sec if replay_sec > 0 else 0.0:.1f}",
                color=GREEN,
            )
            results.append(
                {
                    "query": query.name,
                    "input_rows": dataset_stats["total_rows"],
                    "expected_rows": query.expected_rows(dataset_stats),
                    "replay_sec": round(replay_sec, 3),
                    "throughput_rps": round(dataset_stats["total_rows"] / replay_sec, 1)
                    if replay_sec > 0
                    else 0.0,
                    "avg_cpu_percent": round(avg_cpu_percent, 2),
                    "avg_mem_gib": round(avg_mem_gib, 3),
                    "kafka_preload_sec": round(kafka_preload_sec, 3),
                    "state": state,
                    "sample_csv": str(sample_csv),
                }
            )
        report_md.write_text(
            markdown_report(args, dataset_stats, results), encoding="utf-8"
        )
        report_json.write_text(
            json.dumps(
                {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "engine": "flink",
                    "mode": "preload_then_earliest_replay",
                    "fixture": "official keyed bid dataset",
                    "fixture_metadata": fixture_metadata,
                    "dataset_stats": dataset_stats,
                    "parallelism": args.parallelism,
                    "sink_mode": "blackhole",
                    "results": results,
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        log(f"Benchmark completed, report written to {report_md}")
        print(report_md)
        print(report_json)
        return 0
    finally:
        subprocess.run(
            [str(runtime_flink / "bin" / "stop-cluster.sh")],
            cwd=runtime_flink,
            env=env,
            capture_output=True,
            text=True,
            timeout=120,
        )
        if not args.no_cleanup:
            runtime_root = runtime_flink.parent
            if runtime_root.exists():
                try:
                    shutil.rmtree(runtime_root)
                except Exception:
                    pass


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BenchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
