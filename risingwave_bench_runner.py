#!/usr/bin/env python3
"""
这个 bench runner 用来在本地 standalone RisingWave 上运行一组固定的 Nexmark 风格查询。

它复用 `nexmark_fixture.py` 生成的官方 bid fixture：先从官方 Nexmark combined events
中抽取 `Bid`，再把这些数据 preload 到 Kafka topic，随后让 RisingWave source 以
`earliest` 方式回放。

执行方式：

1. `bench_risingwave.sh` 负责启动 Kafka、RisingWave standalone 容器，并把连接参数传入本文件。
2. 本文件准备官方 bid fixture，并为 Kafka 生成按 partition 打 key 的 preload 文件。
3. 对每个 query：
   - 重建独立的 Kafka topic
   - 在 RisingWave 中创建 source
   - 按 sink 模式创建 materialized view 或 blackhole sink
   - 在 replay 窗口内采样 RisingWave 容器主进程的 CPU 和 RSS
   - 依据 sink 模式等待 query 完成
4. 每个 query 的结果会写入 `report.md`、`report.json` 和采样 CSV。

当前支持的完成判定：

- `sink=table`：轮询 materialized view 的 `COUNT(*)`，直到达到理论输出行数。
- `sink=blackhole`：轮询 `rw_catalog.rw_kafka_job_lag`，直到 lag 为 0。

当前统计的指标：

- `replay_sec`
  指从创建 source + sink/MV 并开始 replay，到完成判定满足为止的耗时。
- `throughput_rps`
  定义为 `input_rows / replay_sec`。
- `avg_cpu_percent`
  replay 窗口内，对 RisingWave 容器主进程做周期性 `ps` 采样后取平均值。
- `avg_mem_gib`
  replay 窗口内，对 RisingWave 容器主进程 RSS 做周期性 `ps` 采样后取平均值，并换算为 GiB。
- `kafka_preload_sec`
  把本轮 query 输入写入 Kafka topic 的耗时，单独记录，不计入 throughput 分母。

注意：

- 这是本地 `single_node` harness 的测量口径。
- `sink=table` 和常见的 `blackhole` benchmark 不是同一口径，不能直接比较绝对值。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable

from nexmark_fixture import prepare_official_bid_fixture


RESET = "\033[0m"
GREEN = "\033[32m"


@dataclass
class QuerySpec:
    name: str
    select_sql: str
    expected_rows: Callable[[dict[str, int]], int]


QUERY_SPECS: dict[str, QuerySpec] = {
    "q0": QuerySpec(
        name="q0",
        select_sql="""
            SELECT auction, bidder, price, channel, url, ts
            FROM {source}
        """,
        expected_rows=lambda stats: stats["total_rows"],
    ),
    "q1": QuerySpec(
        name="q1",
        select_sql="""
            SELECT
              auction,
              bidder,
              CAST(price AS DOUBLE PRECISION) * 0.89 AS price_eur,
              channel,
              url,
              ts
            FROM {source}
        """,
        expected_rows=lambda stats: stats["total_rows"],
    ),
    "q2": QuerySpec(
        name="q2",
        select_sql="""
            SELECT auction, price, ts
            FROM {source}
            WHERE auction IN (1007, 1020, 2001, 2019, 2087)
        """,
        expected_rows=lambda stats: stats["q2_expected_rows"],
    ),
    "q14": QuerySpec(
        name="q14",
        select_sql="""
            SELECT
              auction,
              bidder,
              CAST(price AS DOUBLE PRECISION) * 0.908 AS price,
              CASE
                WHEN EXTRACT(HOUR FROM ts) >= 8 AND EXTRACT(HOUR FROM ts) <= 18 THEN 'dayTime'
                WHEN EXTRACT(HOUR FROM ts) <= 6 OR EXTRACT(HOUR FROM ts) >= 20 THEN 'nightTime'
                ELSE 'otherTime'
              END AS bid_time_type,
              ts,
              extra
            FROM {source}
            WHERE CAST(price AS DOUBLE PRECISION) * 0.908 > 1000000
              AND CAST(price AS DOUBLE PRECISION) * 0.908 < 50000000
        """,
        expected_rows=lambda stats: stats["q14_expected_rows"],
    ),
    "q21": QuerySpec(
        name="q21",
        select_sql="""
            SELECT
              ts,
              auction,
              bidder,
              price,
              channel,
              CASE
                WHEN LOWER(channel) = 'apple' THEN '0'
                WHEN LOWER(channel) = 'google' THEN '1'
                WHEN LOWER(channel) = 'facebook' THEN '2'
                WHEN LOWER(channel) = 'baidu' THEN '3'
                ELSE (regexp_match(url, 'channel_id=([^&]*)'))[1]
              END AS channel_id
            FROM {source}
            WHERE (regexp_match(url, 'channel_id=([^&]*)'))[1] IS NOT NULL
               OR LOWER(channel) IN ('apple', 'google', 'facebook', 'baidu')
        """,
        expected_rows=lambda stats: stats["q21_expected_rows"],
    ),
    "q22": QuerySpec(
        name="q22",
        select_sql="""
            SELECT
              ts,
              auction,
              bidder,
              price,
              channel,
              split_part(url, '/', 4) AS dir1,
              split_part(url, '/', 5) AS dir2,
              split_part(url, '/', 6) AS dir3
            FROM {source}
        """,
        expected_rows=lambda stats: stats["total_rows"],
    ),
}


class BenchError(RuntimeError):
    pass


class SinkMode(str, Enum):
    TABLE = "table"
    BLACKHOLE = "blackhole"


def log(message: str, *, color: str | None = None) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if color is None:
        print(f"[{now} UTC] {message}", flush=True)
    else:
        print(f"{color}[{now} UTC] {message}{RESET}", flush=True)


class RisingWaveSql:
    def __init__(self, host: str, port: int, user: str, database: str):
        self.base_cmd = [
            "psql",
            "-h",
            host,
            "-p",
            str(port),
            "-U",
            user,
            "-d",
            database,
            "-v",
            "ON_ERROR_STOP=1",
            "-t",
            "-A",
        ]

    def run(self, sql: str, timeout: int = 60) -> str:
        env = os.environ.copy()
        env.setdefault("PGPASSWORD", "")
        result = subprocess.run(
            self.base_cmd + ["-c", sql],
            capture_output=True,
            text=True,
            timeout=timeout,
            env=env,
        )
        if result.returncode != 0:
            raise BenchError(
                result.stderr.strip() or result.stdout.strip() or "psql failed"
            )
        return result.stdout.strip()

    def scalar_i64(self, sql: str, timeout: int = 60) -> int:
        output = self.run(sql, timeout=timeout)
        for line in reversed(output.splitlines()):
            line = line.strip()
            if line.isdigit():
                return int(line)
        raise BenchError(f"cannot parse scalar result from output: {output}")


class ContainerMonitor:
    def __init__(self, container: str, output_csv: Path, sample_interval: float):
        self.container = container
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
                sample = read_container_sample(self.container)
                if sample is not None:
                    writer.writerow(sample)
                    fh.flush()
                time.sleep(self.sample_interval)


def read_container_sample(container: str) -> tuple[float, float, int] | None:
    cmd = [
        "docker",
        "exec",
        container,
        "ps",
        "-p",
        "1",
        "-o",
        "%cpu=",
        "-o",
        "rss=",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=5)
    if result.returncode != 0:
        return None
    fields = result.stdout.strip().split()
    if len(fields) < 2:
        return None
    try:
        cpu_percent = float(fields[0])
        rss_kib = int(fields[1])
    except ValueError:
        return None
    return time.time(), cpu_percent, rss_kib


def summarize_samples(path: Path) -> tuple[float, float]:
    cpu_values: list[float] = []
    rss_values: list[int] = []
    with path.open("r", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            cpu_values.append(float(row["cpu_percent"]))
            rss_values.append(int(row["rss_kib"]))
    if not cpu_values or not rss_values:
        return 0.0, 0.0
    avg_cpu = sum(cpu_values) / len(cpu_values)
    avg_mem_gib = (sum(rss_values) / len(rss_values)) / 1024 / 1024
    return avg_cpu, avg_mem_gib


def prepare_fixture(
    *,
    workdir: Path,
    dataset_name: str,
    rows: int,
    kafka_container: str,
    kafka_brokers: str,
    partitions: int,
) -> tuple[Path, Path, dict[str, int], dict[str, object]]:
    # Reuse the same official bid fixture as the Datalayers/Flink runners so cross-engine
    # comparisons are based on identical input rows.
    dataset_path = workdir / dataset_name
    keyed_path = workdir / f"{dataset_path.stem}.keyed{dataset_path.suffix}"
    result = prepare_official_bid_fixture(
        workdir=workdir,
        kafka_container=kafka_container,
        kafka_brokers=kafka_brokers,
        rows=rows,
        partitions=partitions,
        log=log,
    )
    shutil.copy2(result.dataset_path, dataset_path)
    rewrite_dataset_with_keys(dataset_path, keyed_path, partitions)
    return dataset_path, keyed_path, result.stats, result.metadata


def run_cmd(cmd: list[str], input_path: Path | None = None, timeout: int = 300) -> None:
    stdin = None
    try:
        if input_path is not None:
            stdin = input_path.open("rb")
        result = subprocess.run(cmd, stdin=stdin, capture_output=True, timeout=timeout)
    finally:
        if stdin is not None:
            stdin.close()
    if result.returncode != 0:
        raise BenchError(
            (
                result.stderr.decode("utf-8", errors="ignore")
                or result.stdout.decode("utf-8", errors="ignore")
            ).strip()
        )


def ensure_topic(container: str, topic: str, partitions: int) -> None:
    # Recreate the topic for every query run to avoid retained rows and old offsets affecting
    # the replay window.
    log(f"Recreating Kafka topic {topic} with {partitions} partitions")
    run_cmd(
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
        timeout=60,
    )
    time.sleep(1)
    run_cmd(
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
        timeout=60,
    )


def load_topic(container: str, topic: str, dataset_path: Path) -> float:
    log(f"Starting Kafka preload for topic {topic} from {dataset_path}")
    start = time.time()
    run_cmd(
        [
            "docker",
            "exec",
            "-i",
            container,
            "bash",
            "-lc",
            (
                "kafka-console-producer --bootstrap-server 127.0.0.1:9092 "
                f"--topic {topic} "
                "--property parse.key=true --property key.separator=$'\\t'"
            ),
        ],
        input_path=dataset_path,
        timeout=3600,
    )
    elapsed = time.time() - start
    log(f"Finished Kafka preload for topic {topic} in {elapsed:.3f}s")
    return elapsed


def rewrite_dataset_with_keys(
    dataset_path: Path, keyed_path: Path, partitions: int
) -> None:
    # Deterministic keys keep partition assignment stable across repeated runs.
    log(f"Rewriting dataset with Kafka keys into {keyed_path}")
    with (
        dataset_path.open("r", encoding="utf-8") as src,
        keyed_path.open("w", encoding="utf-8") as dst,
    ):
        for index, line in enumerate(src):
            dst.write(f"p{index % partitions}\t{line}")
    log(f"Finished keyed dataset rewrite for {partitions} partitions")


def wait_for_count(
    sql: RisingWaveSql, table: str, expected_rows: int, timeout: int
) -> int:
    # MV completion is defined as "the materialized result contains at least the expected
    # number of rows for the current query semantics".
    log(f"Waiting for materialized view {table} to reach {expected_rows} rows")
    start = time.time()
    while True:
        count = sql.scalar_i64(f"SELECT COUNT(*) FROM {table}")
        if count >= expected_rows:
            return count
        if time.time() - start > timeout:
            raise BenchError(
                f"timeout waiting for {table} to reach {expected_rows} rows, current={count}"
            )
        time.sleep(1)


def wait_for_lag_zero(
    sql: RisingWaveSql, relation_name: str, relation_type: str, timeout: int
) -> None:
    # Blackhole completion relies on RisingWave's Kafka lag catalog for the source job behind
    # the sink or materialized view.
    log(f"Waiting for RisingWave {relation_type} {relation_name} Kafka lag to reach 0")
    if relation_type == "mv":
        relation_sql = f"SELECT id FROM rw_catalog.rw_materialized_views WHERE name = '{relation_name}'"
    else:
        relation_sql = (
            f"SELECT id FROM rw_catalog.rw_sinks WHERE name = '{relation_name}'"
        )
    lag_sql = (
        "SELECT COALESCE(SUM(COALESCE(lag, 0)), 0) "
        "FROM rw_catalog.rw_kafka_job_lag "
        f"WHERE job_id = ({relation_sql})"
    )
    start = time.time()
    while True:
        lag = sql.scalar_i64(lag_sql, timeout=30)
        if lag == 0:
            return
        if time.time() - start > timeout:
            raise BenchError(
                f"timeout waiting for kafka lag to reach 0 for {relation_type} `{relation_name}`, current={lag}"
            )
        time.sleep(1)


def create_source(
    sql: RisingWaveSql, source: str, topic: str, kafka_brokers: str
) -> None:
    # Sources are created per query so each replay starts from a clean earliest offset.
    log(f"Creating RisingWave source {source} for topic {topic}")
    sql.run(
        f"""
        CREATE SOURCE {source} (
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel VARCHAR,
            url VARCHAR,
            ts TIMESTAMPTZ,
            extra VARCHAR
        ) WITH (
            connector = 'kafka',
            topic = '{topic}',
            properties.bootstrap.server = '{kafka_brokers}',
            scan.startup.mode = 'earliest'
        ) FORMAT PLAIN ENCODE JSON
        """,
        timeout=120,
    )


def create_mv(sql: RisingWaveSql, mv: str, source: str, query: QuerySpec) -> None:
    log(f"Creating RisingWave materialized view {mv} for query {query.name}")
    select_sql = query.select_sql.format(source=source)
    sql.run(
        f"""
        CREATE MATERIALIZED VIEW {mv} AS
        {select_sql}
        """,
        timeout=120,
    )


def create_blackhole_sink(
    sql: RisingWaveSql, sink: str, source: str, query: QuerySpec
) -> None:
    log(f"Creating RisingWave blackhole sink {sink} for query {query.name}")
    select_sql = query.select_sql.format(source=source)
    sql.run(
        f"""
        CREATE SINK {sink} AS
        {select_sql}
        WITH (
            connector = 'blackhole',
            type = 'append-only',
            force_append_only = 'true'
        )
        """,
        timeout=120,
    )


def cleanup_objects(
    sql: RisingWaveSql, target: str, source: str, sink_mode: SinkMode
) -> None:
    log(f"Cleaning up RisingWave objects source={source} target={target}")
    if sink_mode == SinkMode.TABLE:
        statements = [
            f"DROP MATERIALIZED VIEW IF EXISTS {target}",
            f"DROP SOURCE IF EXISTS {source}",
        ]
    else:
        statements = [
            f"DROP SINK IF EXISTS {target}",
            f"DROP SOURCE IF EXISTS {source}",
        ]
    for statement in statements:
        try:
            sql.run(statement)
        except Exception:
            pass


def markdown_report(
    args: argparse.Namespace,
    dataset_stats: dict[str, int],
    results: list[dict[str, object]],
) -> str:
    # Keep the report self-contained so the result can be interpreted without reopening the code.
    lines = [
        "# RisingWave Nexmark Benchmark Report",
        "",
        f"- Generated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"- RisingWave container: `{args.rw_container}`",
        f"- Kafka brokers in RisingWave: `{args.rw_kafka_brokers}`",
        f"- Topic partitions: `{args.partitions}`",
        f"- Fixture: `official`",
        f"- Sink mode: `{args.sink}`",
        f"- Input rows: `{dataset_stats['total_rows']}`",
        f"- Dataset: bid events extracted from official Nexmark combined events",
        f"- Measurement mode: preload Kafka then replay from `earliest`",
        "",
        "## Results",
        "",
        "| Query | Input Rows | Expected Rows | Actual Rows | Replay Seconds | Throughput (input records/s) | Avg CPU (%) | Avg Mem (GiB) | Kafka Preload Seconds |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in results:
        lines.append(
            "| {query} | {input_rows} | {expected_rows} | {actual_rows} | {replay_sec:.3f} | {throughput_rps:.1f} | {avg_cpu_percent:.2f} | {avg_mem_gib:.3f} | {kafka_preload_sec:.3f} |".format(
                **item
            )
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This benchmark samples the standalone RisingWave container main process during the replay window.",
            "- `sink=table` maps to a RisingWave materialized view and waits on `COUNT(*)`.",
            "- `sink=blackhole` creates a RisingWave blackhole sink and waits on `rw_catalog.rw_kafka_job_lag` reaching 0.",
            "- `throughput` is computed as input rows divided by replay time.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run RisingWave Nexmark benchmarks")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4566)
    parser.add_argument("--user", default="root")
    parser.add_argument("--database", default="dev")
    parser.add_argument("--rw-container", default="risingwave-standalone")
    parser.add_argument("--rw-kafka-brokers", default="kafka:9092")
    parser.add_argument("--fixture-kafka-brokers", default="127.0.0.1:9092")
    parser.add_argument("--kafka-container", default="risingwave-nexmark-kafka")
    parser.add_argument("--workdir", default=".risingwave-nexmark")
    parser.add_argument("--dataset-name", default="nexmark_bid.jsonl")
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--partitions", type=int, default=4)
    parser.add_argument("--queries", default="q0,q1,q2,q14,q21,q22")
    parser.add_argument(
        "--sink",
        choices=[mode.value for mode in SinkMode],
        default=SinkMode.TABLE.value,
    )
    parser.add_argument("--no-cleanup", type=int, choices=[0, 1], default=0)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--sample-interval", type=float, default=1.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log(
        f"Starting RisingWave Nexmark benchmark: rows={args.rows} queries={args.queries} "
        f"sink={args.sink} workdir={args.workdir}"
    )
    workdir = Path(args.workdir).resolve()
    report_md = workdir / "report.md"
    report_json = workdir / "report.json"

    requested_queries = [
        item.strip() for item in args.queries.split(",") if item.strip()
    ]
    queries: list[QuerySpec] = []
    for name in requested_queries:
        spec = QUERY_SPECS.get(name)
        if spec is None:
            raise BenchError(f"unsupported query name: {name}")
        queries.append(spec)

    if shutil.which("docker") is None:
        raise BenchError(
            "docker is required because Kafka tooling and metrics sampling rely on containers"
        )
    sink_mode = SinkMode(args.sink)

    sql = RisingWaveSql(args.host, args.port, args.user, args.database)
    workdir.mkdir(parents=True, exist_ok=True)
    _dataset_path, keyed_path, dataset_stats, fixture_metadata = prepare_fixture(
        workdir=workdir,
        dataset_name=args.dataset_name,
        rows=args.rows,
        kafka_container=args.kafka_container,
        kafka_brokers=args.fixture_kafka_brokers,
        partitions=args.partitions,
    )

    results: list[dict[str, object]] = []
    for query in queries:
        log(f"Starting benchmark for query {query.name}", color=GREEN)
        topic = f"nexmark_{query.name}"
        suffix = f"{int(time.time() * 1000)}"
        source = f"{query.name}_src_{suffix}"
        target = (
            f"{query.name}_mv" if sink_mode == SinkMode.TABLE else f"{query.name}_bh"
        )
        ensure_topic(args.kafka_container, topic, args.partitions)
        if not args.no_cleanup:
            cleanup_objects(sql, target, source, sink_mode)
        try:
            kafka_preload_sec = load_topic(args.kafka_container, topic, keyed_path)
            expected_rows = query.expected_rows(dataset_stats)
            sample_csv = workdir / f"{query.name}_samples.csv"
            monitor = ContainerMonitor(
                args.rw_container, sample_csv, args.sample_interval
            )
            replay_t0 = time.time()
            log(f"Starting replay window for query {query.name}")
            monitor.start()
            try:
                # The replay window intentionally includes source and sink/MV creation.
                create_source(sql, source, topic, args.rw_kafka_brokers)
                if sink_mode == SinkMode.TABLE:
                    create_mv(sql, target, source, query)
                    actual_rows = wait_for_count(
                        sql, target, expected_rows, args.timeout
                    )
                else:
                    create_blackhole_sink(sql, target, source, query)
                    wait_for_lag_zero(sql, target, "sink", args.timeout)
                    actual_rows = expected_rows
            finally:
                monitor.stop()
            replay_t1 = time.time()
            replay_sec = replay_t1 - replay_t0
            avg_cpu_percent, avg_mem_gib = summarize_samples(sample_csv)
            log(
                f"Finished benchmark for query {query.name}: actual_rows={actual_rows} "
                f"replay_sec={replay_sec:.3f} throughput={dataset_stats['total_rows'] / replay_sec if replay_sec > 0 else 0.0:.1f}",
                color=GREEN,
            )
            results.append(
                {
                    "query": query.name,
                    "input_rows": dataset_stats["total_rows"],
                    "expected_rows": expected_rows,
                    "actual_rows": actual_rows,
                    "replay_sec": round(replay_sec, 3),
                    "throughput_rps": round(dataset_stats["total_rows"] / replay_sec, 1)
                    if replay_sec > 0
                    else 0.0,
                    "avg_cpu_percent": round(avg_cpu_percent, 2),
                    "avg_mem_gib": round(avg_mem_gib, 3),
                    "kafka_preload_sec": round(kafka_preload_sec, 3),
                    "sink_mode": sink_mode.value,
                    "sample_csv": str(sample_csv),
                }
            )
        finally:
            if not args.no_cleanup:
                cleanup_objects(sql, target, source, sink_mode)

    report_md.write_text(
        markdown_report(args, dataset_stats, results), encoding="utf-8"
    )
    report_json.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "engine": "risingwave",
                "mode": "preload_then_earliest_replay",
                "fixture": "official",
                "fixture_metadata": fixture_metadata,
                "dataset_stats": dataset_stats,
                "sink_mode": sink_mode.value,
                "results": results,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    log(f"Benchmark completed, report written to {report_md}")
    if args.no_cleanup:
        log("No cleanup requested, keeping Kafka container data and RisingWave objects")
    print(report_md)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BenchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
