#!/usr/bin/env python3
"""
这个 bench runner 用来在本地 standalone RisingWave 上运行一组固定的 Nexmark 风格查询。

它消费的是一个已经预先准备好的 keyed bid JSONL dataset。这个 dataset 通常由
`datagen.sh` 调用 `nexmark_fixture.py` 生成，并稳定保存在 `nexmark-bench` 目录下。

执行方式：

1. `benches/risingwave.sh` 负责启动 Kafka、RisingWave standalone 容器，并把连接参数传入本文件。
2. 本文件读取与 dataset 关联的 stats JSON，拿到输入行数和几个 query 的理论输出行数。
3. 对每个 query：
   - 重建独立的 Kafka topic
   - 把 keyed dataset preload 到 topic
   - 在 RisingWave 中创建 source
   - 按 sink 模式创建 materialized view 或 blackhole sink
   - 在 replay 窗口内采样 RisingWave 容器主进程的 CPU 和 RSS
   - 依据 sink 模式等待 query 完成
4. 每个 query 的结果会写入 `report.md`、`report.json` 和采样 CSV。

当前支持的完成判定：

- `sink=table`：轮询 materialized view 的 `COUNT(*)`，直到达到理论输出行数。
- `sink=blackhole`：轮询对应 Kafka consumer group 的 lag，直到为 0。

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
  把本轮 query 输入数据写入 Kafka topic 的耗时，单独记录，不计入 throughput 分母。

注意：

- 这是本地 `single_node` harness 的测量口径。
- `sink=table` 和常见的 `blackhole` benchmark 不是同一口径，不能直接比较绝对值。
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
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from nexmark_fixture import load_bid_dataset_stats


RESET = "\033[0m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
RW_GROUP_ID_PREFIX = "nexmark-rw-consumer"
RW_METRIC_NAME = "stream_sink_input_row_count"


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
    "q16": QuerySpec(
        name="q16",
        select_sql="""
            SELECT channel,
                   count(*) AS total_bids,
                   min(price) AS min_price,
                   max(price) AS max_price,
                   avg(price) AS avg_price,
                   count(distinct bidder) AS distinct_bidders,
                   count(distinct auction) AS distinct_auctions
            FROM {source}
            GROUP BY channel
        """,
        expected_rows=lambda stats: stats["q16_expected_rows"],
    ),
    "q17": QuerySpec(
        name="q17",
        select_sql="""
            SELECT auction,
                   count(*) AS bid_count,
                   min(price) AS min_price,
                   max(price) AS max_price,
                   avg(price) AS avg_price,
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


class SinkMode(str, Enum):
    TABLE = "table"
    BLACKHOLE = "blackhole"


def log(message: str, *, color: str | None = None) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if color is None:
        print(f"[{now} UTC] {message}", flush=True)
    else:
        print(f"{color}[{now} UTC] {message}{RESET}", flush=True)


def log_cli_args(args: argparse.Namespace) -> None:
    log(
        f"CLI args: {json.dumps(vars(args), sort_keys=True, default=str)}",
        color=YELLOW,
    )


def log_sql(label: str, sql_text: str) -> None:
    log(
        f"{label} SQL:\n---8<---\n{sql_text.strip()}\n--->8---",
        color=YELLOW,
    )


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


def configure_benchmark_session(
    sql: RisingWaveSql, parallelism: int, sink_mode: SinkMode
) -> None:
    sql_text = f"SET streaming_parallelism = {parallelism}"
    log_sql("Configure streaming parallelism", sql_text)
    sql.run(sql_text)
    if sink_mode == SinkMode.BLACKHOLE:
        sql_text = "SET streaming_use_snapshot_backfill = false"
        log_sql("Disable snapshot backfill for blackhole sink benchmark", sql_text)
        sql.run(sql_text)


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


def wait_for_count(
    sql: RisingWaveSql,
    table: str,
    expected_rows: int,
    container: str,
    group: str,
    topic: str,
    timeout: int,
) -> int:
    # MV completion requires both the expected result cardinality and the source job to have
    # drained Kafka, otherwise selective queries can finish too early.
    log(
        f"Waiting for materialized view {table} to reach {expected_rows} rows "
        f"and Kafka group `{group}` to finish consuming topic `{topic}`"
    )
    start = time.time()
    stable_count_polls = 0
    previous_count: int | None = None
    fallback_logged = False
    while True:
        count = sql.scalar_i64(f"SELECT COUNT(*) FROM {table}")
        drained, rows = kafka_group_drained(container, group, topic)
        if count >= expected_rows and drained:
            return count
        if count >= expected_rows and not rows:
            if previous_count == count:
                stable_count_polls += 1
            else:
                stable_count_polls = 1
                previous_count = count
            if stable_count_polls >= 3:
                if not fallback_logged:
                    log(
                        f"Kafka group `{group}` is unavailable; falling back to stable COUNT(*) "
                        f"completion for materialized view {table}"
                    )
                    fallback_logged = True
                return count
        else:
            stable_count_polls = 0
            previous_count = count
        if time.time() - start > timeout:
            raise BenchError(
                f"timeout waiting for {table} count>={expected_rows} and kafka group to drain, "
                f"current_count={count} current_offsets={rows}"
            )
        time.sleep(1)


def kafka_group_offsets(
    container: str, group: str, topic: str
) -> list[tuple[int, int, int]]:
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
        return []
    rows: list[tuple[int, int, int]] = []
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 6 or fields[1] != topic:
            continue
        current_offset, log_end_offset, lag_value = fields[3], fields[4], fields[5]
        if current_offset == "-" or log_end_offset == "-" or lag_value == "-":
            continue
        try:
            rows.append((int(current_offset), int(log_end_offset), int(lag_value)))
        except ValueError:
            continue
    return rows


def kafka_group_drained(
    container: str, group: str, topic: str
) -> tuple[bool, list[tuple[int, int, int]]]:
    rows = kafka_group_offsets(container, group, topic)
    if not rows:
        return False, rows
    # RisingWave does not rely on committed offsets and reports them only for monitoring.
    # In practice a fully drained partition may remain at lag=1 while still satisfying
    # current_offset + lag == log_end_offset.
    drained = all(current + lag == end and lag in (0, 1) for current, end, lag in rows)
    return drained, rows


def wait_for_group_lag_zero(
    container: str, group: str, topic: str, timeout: int
) -> None:
    log(f"Waiting for Kafka group `{group}` to finish consuming topic `{topic}`")
    start = time.time()
    stable_polls = 0
    while True:
        drained, rows = kafka_group_drained(container, group, topic)
        if drained:
            stable_polls += 1
            if stable_polls >= 3:
                return
        else:
            stable_polls = 0
        if time.time() - start > timeout:
            raise BenchError(
                f"timeout waiting for kafka group `{group}` to drain topic `{topic}`, current={rows}"
            )
        time.sleep(1)


def sink_id(sql: RisingWaveSql, sink: str) -> int:
    return sql.scalar_i64(
        f"SELECT id FROM rw_catalog.rw_sinks WHERE name = '{sink}'",
        timeout=30,
    )


def metrics_ports(container: str) -> list[int]:
    result = subprocess.run(
        [
            "docker",
            "exec",
            container,
            "bash",
            "-lc",
            "ss -ltnH | awk '{print $4}'",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise BenchError(
            result.stderr.strip() or result.stdout.strip() or "cannot inspect ports"
        )
    ports: list[int] = []
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        port_text = line.rsplit(":", 1)[-1]
        if not port_text.isdigit():
            continue
        port = int(port_text)
        if port not in ports:
            ports.append(port)
    preferred = [1222, 1260]
    for port in preferred:
        if port not in ports:
            ports.append(port)
    ports.sort(
        key=lambda port: (
            port not in preferred,
            preferred.index(port) if port in preferred else port,
        )
    )
    if not ports:
        ports = preferred.copy()
    return ports


def metrics_text(container: str) -> str:
    errors: list[str] = []
    for port in metrics_ports(container):
        result = subprocess.run(
            [
                "docker",
                "exec",
                container,
                "bash",
                "-lc",
                (
                    f"curl -fsS --max-time 3 http://127.0.0.1:{port}/metrics "
                    f"|| wget -qO- http://127.0.0.1:{port}/metrics"
                ),
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            errors.append(
                f"{port}: {result.stderr.strip() or result.stdout.strip() or 'request failed'}"
            )
            continue
        body = result.stdout
        if RW_METRIC_NAME in body:
            return body
    raise BenchError(
        "cannot fetch RisingWave metrics containing "
        f"{RW_METRIC_NAME} from container {container}: {'; '.join(errors)}"
    )


def sink_input_row_count(container: str, sink_id_value: int) -> int:
    pattern = re.compile(
        rf'^{RW_METRIC_NAME}\{{[^}}]*sink_id="{sink_id_value}"(?:,|}})[^}}]*\}}\s+(\d+)$'
    )
    total = 0
    try:
        body = metrics_text(container)
    except BenchError:
        return 0
    for line in body.splitlines():
        match = pattern.match(line.strip())
        if match is None:
            continue
        total += int(match.group(1))
    return total


def wait_for_sink_input_rows(
    container: str,
    sink_name: str,
    sink_id_value: int,
    expected_rows: int,
    timeout: int,
) -> int:
    log(
        f"Waiting for blackhole sink {sink_name} to consume {expected_rows} rows "
        f"via RisingWave metric `{RW_METRIC_NAME}`"
    )
    start = time.time()
    stable_polls = 0
    last_count: int | None = None
    while True:
        count = sink_input_row_count(container, sink_id_value)
        if count >= expected_rows:
            if count == last_count:
                stable_polls += 1
            else:
                stable_polls = 1
                last_count = count
            if stable_polls >= 3:
                return count
        else:
            stable_polls = 0
            last_count = count
        if time.time() - start > timeout:
            raise BenchError(
                f"timeout waiting for sink {sink_name} metric rows>={expected_rows}, current={count}"
            )
        time.sleep(1)


def create_source(
    sql: RisingWaveSql,
    source: str,
    topic: str,
    kafka_brokers: str,
    group_id_prefix: str,
) -> None:
    # Sources are created per query so each replay starts from a clean earliest offset.
    log(f"Creating RisingWave source {source} for topic {topic}")
    sql_text = f"""
        CREATE SOURCE {source} (
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel VARCHAR,
            url VARCHAR,
            ts TIMESTAMP,
            extra VARCHAR
        ) WITH (
            connector = 'kafka',
            topic = '{topic}',
            properties.bootstrap.server = '{kafka_brokers}',
            properties.enable.auto.commit = 'true',
            group.id.prefix = '{group_id_prefix}',
            scan.startup.mode = 'earliest'
        ) FORMAT PLAIN ENCODE JSON
        """
    log_sql(f"Create source for {source}", sql_text)
    sql.run(sql_text, timeout=120)


def source_fragment_id(sql: RisingWaveSql, source: str) -> int:
    return sql.scalar_i64(
        "SELECT fragment_id "
        "FROM rw_catalog.rw_sources s "
        "JOIN rw_catalog.rw_fragments f ON s.id = f.table_id "
        f"WHERE s.name = '{source}'",
        timeout=30,
    )


def create_mv(sql: RisingWaveSql, mv: str, source: str, query: QuerySpec) -> None:
    log(f"Creating RisingWave materialized view {mv} for query {query.name}")
    select_sql = query.select_sql.format(source=source)
    sql_text = f"""
        CREATE MATERIALIZED VIEW {mv} AS
        {select_sql}
        """
    log_sql(f"Create materialized view for {query.name}", sql_text)
    sql.run(sql_text, timeout=1800)


def create_blackhole_sink(
    sql: RisingWaveSql, sink: str, source: str, query: QuerySpec
) -> None:
    log(f"Creating RisingWave blackhole sink {sink} for query {query.name}")
    select_sql = query.select_sql.format(source=source)
    sql_text = f"""
        CREATE SINK {sink} AS
        {select_sql}
        WITH (
            connector = 'blackhole',
            type = 'append-only',
            force_append_only = 'true'
        )
        """
    log_sql(f"Create blackhole sink for {query.name}", sql_text)
    sql.run(sql_text, timeout=1800)


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
        f"- Topic partitions: `{dataset_stats['partitions']}`",
        f"- Streaming parallelism: `{args.parallelism}`",
        f"- Fixture: `official keyed bid dataset`",
        f"- Sink mode: `{args.sink}`",
        f"- Input rows: `{dataset_stats['total_rows']}`",
        f"- Dataset path: `{Path(args.dataset).resolve()}`",
        f"- Measurement mode: preload Kafka then replay from `earliest`",
        "",
        "## Results",
        "",
        "| Query | Input Rows | Expected Rows | Inserted Rows | Replay Seconds | Throughput (input records/s) | Avg CPU (%) | Avg Mem (GiB) | Kafka Preload Seconds |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in results:
        lines.append(
            "| {query} | {input_rows} | {expected_rows} | {inserted_rows} | {replay_sec:.3f} | {throughput_rps:.1f} | {avg_cpu_percent:.2f} | {avg_mem_gib:.3f} | {kafka_preload_sec:.3f} |".format(
                **item
            )
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This benchmark samples the standalone RisingWave container main process during the replay window.",
            "- `sink=table` maps to a RisingWave materialized view and waits on `COUNT(*)` plus a best-effort Kafka drain signal; if the Kafka group is unavailable, it falls back to stable `COUNT(*)` completion.",
            "- `sink=blackhole` creates a RisingWave blackhole sink and waits on the `stream_sink_input_row_count` metric for that sink.",
            "- `throughput` is computed as input rows divided by replay time.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="针对预先生成好的 keyed bid JSONL dataset 运行 RisingWave Nexmark benchmark。",
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="help", help="显示本帮助信息并退出。")
    parser._positionals.title = "位置参数"
    parser._optionals.title = "可选参数"
    parser.add_argument("--host", default="127.0.0.1", help="RisingWave SQL host。")
    parser.add_argument("--port", type=int, default=4566, help="RisingWave SQL 端口。")
    parser.add_argument("--user", default="root", help="RisingWave SQL 用户名。")
    parser.add_argument("--database", default="dev", help="RisingWave 数据库名。")
    parser.add_argument(
        "--rw-container",
        default="risingwave-standalone",
        help="用于采样 CPU 和内存的 RisingWave 容器名。",
    )
    parser.add_argument(
        "--rw-kafka-brokers",
        default="kafka:9092",
        help="RisingWave 容器内可访问的 Kafka bootstrap servers。",
    )
    parser.add_argument(
        "--kafka-container",
        default="risingwave-nexmark-kafka",
        help="用于管理 topic 和 preload 数据的 Kafka 容器名。",
    )
    parser.add_argument(
        "--workdir",
        default=".risingwave-nexmark",
        help="报告和每个 query 采样 CSV 的工作目录。",
    )
    parser.add_argument(
        "--dataset",
        required=True,
        help="用于 Kafka preload 的 keyed JSONL dataset 路径。",
    )
    parser.add_argument(
        "--queries",
        default="q0,q1,q2,q14,q21,q22,q16,q17",
        help="逗号分隔的 query 列表。",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=1,
        help="RisingWave streaming parallelism，创建 source/MV/sink 前通过 SET streaming_parallelism 生效。",
    )
    parser.add_argument(
        "--sink",
        choices=[mode.value for mode in SinkMode],
        default=SinkMode.TABLE.value,
        help="sink 类型：table 通过 count 判定，blackhole 通过 sink input row metric 判定。",
    )
    parser.add_argument(
        "--no-cleanup",
        type=int,
        choices=[0, 1],
        default=0,
        help="设为 1 时保留 Kafka topic 和 RisingWave 对象。",
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


def main() -> int:
    args = parse_args()
    log_cli_args(args)
    log(
        f"Starting RisingWave Nexmark benchmark: dataset={args.dataset} queries={args.queries} "
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

    deadline = time.time() + 60
    while True:
        try:
            configure_benchmark_session(sql, args.parallelism, sink_mode)
            break
        except BenchError:
            if time.time() > deadline:
                raise BenchError(
                    "RisingWave did not become ready within 60s after port was open"
                ) from None
            time.sleep(1)

    workdir.mkdir(parents=True, exist_ok=True)
    dataset_path = Path(args.dataset).resolve()
    dataset_stats = load_bid_dataset_stats(dataset_path)
    topic_partitions = dataset_stats["partitions"]
    fixture_metadata = {"dataset_path": str(dataset_path)}

    results: list[dict[str, object]] = []
    for query in queries:
        log(f"Starting benchmark for query {query.name}", color=GREEN)
        topic = f"nexmark_{query.name}"
        suffix = f"{int(time.time() * 1000)}"
        source = f"{query.name}_src_{suffix}"
        target = (
            f"{query.name}_mv" if sink_mode == SinkMode.TABLE else f"{query.name}_bh"
        )
        ensure_topic(args.kafka_container, topic, topic_partitions)
        if not args.no_cleanup:
            cleanup_objects(sql, target, source, sink_mode)
        try:
            kafka_preload_sec = load_topic(args.kafka_container, topic, dataset_path)
            expected_rows = query.expected_rows(dataset_stats)
            sample_csv = workdir / f"{query.name}_samples.csv"
            monitor = ContainerMonitor(
                args.rw_container, sample_csv, args.sample_interval
            )
            group_id_prefix = f"{RW_GROUP_ID_PREFIX}-{source}"
            create_source(sql, source, topic, args.rw_kafka_brokers, group_id_prefix)
            fragment_id = source_fragment_id(sql, source)
            group = f"{group_id_prefix}-{fragment_id}"
            if sink_mode == SinkMode.TABLE:
                create_mv(sql, target, source, query)
            else:
                create_blackhole_sink(sql, target, source, query)
            replay_t0 = time.time()
            log(f"Starting replay window for query {query.name}")
            monitor.start()
            try:
                if sink_mode == SinkMode.TABLE:
                    inserted_rows = wait_for_count(
                        sql,
                        target,
                        expected_rows,
                        args.kafka_container,
                        group,
                        topic,
                        args.timeout,
                    )
                else:
                    target_sink_id = sink_id(sql, target)
                    inserted_rows = wait_for_sink_input_rows(
                        args.rw_container,
                        target,
                        target_sink_id,
                        expected_rows,
                        args.timeout,
                    )
            finally:
                monitor.stop()
            replay_t1 = time.time()
            replay_sec = replay_t1 - replay_t0
            avg_cpu_percent, avg_mem_gib = summarize_samples(sample_csv)
            log(
                f"Finished benchmark for query {query.name}: inserted_rows={inserted_rows} "
                f"replay_sec={replay_sec:.3f} throughput={dataset_stats['total_rows'] / replay_sec if replay_sec > 0 else 0.0:.1f}",
                color=GREEN,
            )
            results.append(
                {
                    "query": query.name,
                    "input_rows": dataset_stats["total_rows"],
                    "expected_rows": expected_rows,
                    "inserted_rows": inserted_rows,
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
                "fixture": "official keyed bid dataset",
                "fixture_metadata": fixture_metadata,
                "dataset_stats": dataset_stats,
                "parallelism": args.parallelism,
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
