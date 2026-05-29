#!/usr/bin/env python3
"""
这个 bench runner 用来在本地 Datalayers 实例上运行一组固定的 Nexmark 风格基准查询。

它消费的是一个已经预先准备好的 keyed bid JSONL dataset。这个 dataset 通常由
`datagen.sh` 调用 `nexmark_fixture.py` 生成，并稳定保存在 `nexmark-bench` 目录下。

执行方式：

1. `benches/datalayers.sh` 负责启动 Kafka，并把 Datalayers HTTP SQL 连接参数传入本文件。
2. 本文件读取与 dataset 关联的 stats JSON，拿到输入行数和几个 query 的理论输出行数。
3. 对每个 query：
   - 重建独立的 Kafka topic
   - 在 Datalayers 中创建 database/source/sink/pipeline
   - 把 keyed dataset preload 到 topic
   - 从 source 的 `offset='earliest'` 开始回放
   - 在 replay 窗口内等待 query 完成
   - 根据 sink 类型等待 query 完成
4. 每个 query 的结果会写入 `report.md` 和 `report.json`。

当前支持的完成判定：

- `sink=table`：轮询 sink table 的 `COUNT(*)`，直到达到该 query 的理论输出行数。
- `sink=blackhole`：轮询对应 Kafka consumer group 的 lag，直到为 0。

当前统计的指标：

- `replay_sec`
  指从创建 source/pipeline 并开始 replay，到完成判定满足为止的耗时。
- `throughput_rps`
  定义为 `input_rows / replay_sec`，分母只包含 replay 窗口，不包含 dataset 生成或 Kafka preload。
- `avg_cpu_percent`
  replay 窗口内，对监听 HTTP 端口的本机 Datalayers 进程做周期性 `ps` 采样后取平均值；探测不到 PID 时为 `0.0`。
- `avg_mem_gib`
  replay 窗口内，对监听 HTTP 端口的本机 Datalayers 进程 RSS 做周期性 `ps` 采样后取平均值，并换算为 GiB；探测不到 PID 时为 `0.0`。
- `kafka_preload_sec`
  把本轮 query 输入数据写入 Kafka topic 的耗时，单独记录，不计入 throughput 分母。

注意：

- 这是一个“preload Kafka -> earliest replay -> wait for sink completion”的 benchmark runner。
- 它统计的是本地 harness 口径下的 replay 性能，不等同于其他系统官方网页上的 blackhole benchmark 口径。
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
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


@dataclass
class QuerySpec:
    name: str
    sink_columns: str
    select_sql: str
    expected_rows: Callable[[dict[str, int]], int]
    unsupported: bool = False


QUERY_SPECS: dict[str, QuerySpec] = {
    "q0": QuerySpec(
        name="q0",
        sink_columns="""
            ts TIMESTAMP(9),
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel STRING,
            url STRING,
            TIMESTAMP KEY(ts)
        """,
        select_sql="""
            SELECT ts, auction, bidder, price, channel, url
            FROM {source}
        """,
        expected_rows=lambda stats: stats["total_rows"],
    ),
    "q1": QuerySpec(
        name="q1",
        sink_columns="""
            ts TIMESTAMP(9),
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            price_eur FLOAT64,
            channel STRING,
            url STRING,
            TIMESTAMP KEY(ts)
        """,
        select_sql="""
            SELECT
              ts,
              auction,
              bidder,
              price,
              CAST(price AS DOUBLE) * 0.89 AS price_eur,
              channel,
              url
            FROM {source}
        """,
        expected_rows=lambda stats: stats["total_rows"],
    ),
    "q2": QuerySpec(
        name="q2",
        sink_columns="""
            ts TIMESTAMP(9),
            auction BIGINT,
            price BIGINT,
            TIMESTAMP KEY(ts)
        """,
        select_sql="""
            SELECT ts, auction, price
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
            price FLOAT64,
            bid_time_type STRING,
            ts TIMESTAMP(9),
            extra STRING,
            TIMESTAMP KEY(ts)
        """,
        select_sql="""
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
            ts TIMESTAMP(9),
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel STRING,
            channel_id STRING,
            TIMESTAMP KEY(ts)
        """,
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
        sink_columns="""
            ts TIMESTAMP(9),
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel STRING,
            dir1 STRING,
            dir2 STRING,
            dir3 STRING,
            TIMESTAMP KEY(ts)
        """,
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
        sink_columns="",
        select_sql="",
        expected_rows=lambda stats: stats.get("q16_expected_rows", 0),
        unsupported=True,
    ),
    "q17": QuerySpec(
        name="q17",
        sink_columns="",
        select_sql="",
        expected_rows=lambda stats: stats.get("q17_expected_rows", 0),
        unsupported=True,
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


class DatalayersHttpSql:
    def __init__(self, host: str, port: int, user: str, password: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password

    def run(self, sql: str, database: str | None = None, timeout: int = 60) -> str:
        query = {}
        if database:
            query["db"] = database
        url = f"http://{self.host}:{self.port}/api/v1/sql"
        if query:
            url += "?" + urllib.parse.urlencode(query)
        auth = base64.b64encode(f"{self.user}:{self.password}".encode("utf-8")).decode(
            "ascii"
        )
        request = urllib.request.Request(
            url=url,
            method="POST",
            data=sql.encode("utf-8"),
            headers={
                "Authorization": f"Basic {auth}",
                "Content-Type": "text/plain; charset=utf-8",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                return response.read().decode("utf-8")
        except Exception as exc:
            raise BenchError(f"http sql request failed: {exc}") from exc

    def scalar_i64(self, sql: str, database: str) -> int:
        payload = json.loads(self.run(sql, database=database))
        if "affected_rows" in payload:
            return int(payload["affected_rows"])
        values = payload.get("result", {}).get("values", [])
        if values and values[0]:
            value = values[0][0]
            if isinstance(value, int):
                return value
            if isinstance(value, str) and value.isdigit():
                return int(value)
        raise BenchError(f"cannot parse scalar result from http output: {payload}")


_CLK_TCK = os.sysconf(os.sysconf_names["SC_CLK_TCK"])


class ProcessMonitor:
    def __init__(self, pid: int, output_csv: Path, sample_interval: float):
        self.pid = pid
        self.output_csv = output_csv
        self.sample_interval = sample_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._prev_utime: int | None = None
        self._prev_stime: int | None = None
        self._prev_ts: float | None = None
        self._pagesize = os.sysconf(os.sysconf_names["SC_PAGESIZE"])

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
                sample = self._read_sample()
                if sample is not None:
                    writer.writerow(sample)
                    fh.flush()
                time.sleep(self.sample_interval)

    def _read_sample(self) -> tuple[float, float, int] | None:
        try:
            with open(f"/proc/{self.pid}/stat", "r") as f:
                content = f.read()
        except (FileNotFoundError, PermissionError):
            return None
        close_paren = content.rfind(")")
        if close_paren < 0:
            return None
        fields = content[close_paren + 2 :].split()
        if len(fields) < 22:
            return None
        try:
            utime = int(fields[11])
            stime = int(fields[12])
            rss_kib = (int(fields[21]) * self._pagesize) // 1024
        except (IndexError, ValueError):
            return None
        now = time.time()
        cpu_pct = 0.0
        if self._prev_ts is not None and self._prev_utime is not None:
            delta_cpu = (utime - self._prev_utime) + (stime - self._prev_stime)
            delta_time = now - self._prev_ts
            if delta_time > 0:
                cpu_pct = (delta_cpu / _CLK_TCK) / delta_time * 100.0
        self._prev_utime = utime
        self._prev_stime = stime
        self._prev_ts = now
        return now, cpu_pct, rss_kib


def read_process_sample(pid: int) -> tuple[float, float, int] | None:
    result = subprocess.run(
        ["ps", "-p", str(pid), "-o", "%cpu=", "-o", "rss="],
        capture_output=True,
        text=True,
        timeout=5,
    )
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
    if not path.exists():
        return 0.0, 0.0
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


def run_cmd(
    cmd: list[str], input_path: Path | None = None, timeout: int = 300
) -> subprocess.CompletedProcess[str]:
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
    return result


def ensure_topic(container: str, topic: str, partitions: int) -> None:
    # Recreate the topic for every query run so retained data and old offsets do not leak into
    # the current measurement.
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


def count_kafka_messages(container: str, topic: str) -> int | None:
    try:
        result = run_cmd(
            [
                "docker",
                "exec",
                container,
                "kafka-run-class",
                "kafka.tools.GetOffsetShell",
                "--bootstrap-server",
                "127.0.0.1:9092",
                "--topic",
                topic,
                "--time",
                "-1",
            ],
            timeout=30,
        )
        total = 0
        for line in result.stdout.strip().splitlines():
            if line:
                total += int(line.rsplit(":", 1)[-1])
        return total
    except Exception:
        return None


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
    sql: DatalayersHttpSql,
    database: str,
    table: str,
    expected_rows: int,
    container: str,
    group: str,
    topic: str,
    timeout: int,
) -> tuple[int, float]:
    # Table sink completion requires both the expected result cardinality and the upstream source
    # to fully drain, otherwise selective queries can finish too early.
    log(
        f"Waiting for sink table {database}.{table} to reach {expected_rows} rows "
        f"and Kafka group `{group}` lag to reach 0"
    )
    start = time.time()
    last_progress = start
    last_count: int | None = None
    last_lag: int | None = None
    while True:
        count = sql.scalar_i64(f"SELECT COUNT(*) AS c FROM {table}", database)
        lag = kafka_group_lag(container, group, topic)
        if count >= expected_rows and lag == 0:
            return count, time.time() - start
        if last_count is None or count != last_count or lag != last_lag:
            last_progress = time.time()
        if time.time() - last_progress > timeout:
            raise BenchError(
                f"timeout waiting for {table} count>={expected_rows} and kafka lag=0, "
                f"current_count={count} current_lag={lag}"
            )
        last_count = count
        last_lag = lag
        time.sleep(1)


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
    total_lag = 0
    seen_partition = False
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 6 or fields[1] != topic:
            continue
        lag_value = fields[5]
        if lag_value == "-":
            continue
        try:
            total_lag += int(lag_value)
            seen_partition = True
        except ValueError:
            continue
    if not seen_partition:
        return None
    return total_lag


def wait_for_group_lag_zero(
    container: str, group: str, topic: str, timeout: int
) -> tuple[int, float]:
    # Blackhole sink completion is approximated by Kafka consumer lag reaching zero.
    start = time.time()
    last_progress = start
    last_lag: int | None = None
    while True:
        lag = kafka_group_lag(container, group, topic)
        if lag == 0:
            return 0, time.time() - start
        if last_lag is None or lag != last_lag:
            last_progress = time.time()
        if time.time() - last_progress > timeout:
            raise BenchError(
                f"timeout waiting for kafka group `{group}` lag to reach 0 on topic `{topic}`, current={lag}"
            )
        last_lag = lag
        time.sleep(1)


def pipeline_state(sql: DatalayersHttpSql, database: str, pipeline: str) -> str:
    output = sql.run(
        f"SELECT state FROM information_schema.pipelines WHERE pipeline_name = '{pipeline}'",
        database=database,
    )
    for line in output.splitlines():
        line = line.strip()
        if line in {"Running", "Failed", "Stopped", "Finished"}:
            return line
    return "Unknown"


def create_database(sql: DatalayersHttpSql, database: str) -> None:
    log(f"Creating benchmark database {database}")
    sql_text = f"CREATE DATABASE IF NOT EXISTS {database}"
    log_sql("Create benchmark database", sql_text)
    sql.run(sql_text)


def create_table_sink(
    sql: DatalayersHttpSql,
    database: str,
    query: QuerySpec,
) -> str:
    sink = f"{query.name}_sink"
    sql_text = f"""
        CREATE TABLE {sink} (
            {query.sink_columns}
        )
        ENGINE=TimeSeries
        WITH (memtable_size=1024MB)
        PARTITION BY HASH(auction)
        PARTITIONS 4
        """
    log_sql(f"Create table sink for {query.name}", sql_text)
    sql.run(sql_text, database=database)
    return sink


def create_blackhole_sink(
    sql: DatalayersHttpSql, database: str, query: QuerySpec
) -> str:
    sink = f"{query.name}_bh"
    sql_text = f"CREATE SINK {sink} WITH (connector='blackhole')"
    log_sql(f"Create blackhole sink for {query.name}", sql_text)
    sql.run(sql_text, database=database)
    return sink


def create_source_and_pipeline(
    sql: DatalayersHttpSql,
    database: str,
    topic: str,
    kafka_brokers: str,
    query: QuerySpec,
    sink: str,
) -> tuple[str, str]:
    # Every query gets an isolated source + pipeline pair so setup and teardown stay local to the
    # query under test.
    source = f"{query.name}_src"
    pipeline = f"{query.name}_pipeline"
    log(
        f"Creating source {database}.{source} and pipeline {database}.{pipeline} for query {query.name}"
    )
    source_sql = f"""
        CREATE SOURCE {source} (
            ts TIMESTAMP(9),
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel STRING,
            url STRING,
            extra STRING
        ) WITH (
            connector='kafka',
            brokers='{kafka_brokers}',
            topic='{topic}',
            offset='earliest',
            format='json',
            bad_data='fail'
        )
        """
    pipeline_sql = f"""
        CREATE PIPELINE {pipeline}
        SINK TO {sink}
        AS
        {query.select_sql.format(source=source)}
        """
    log_sql(f"Create source for {query.name}", source_sql)
    sql.run(source_sql, database=database)
    log_sql(f"Create pipeline for {query.name}", pipeline_sql)
    sql.run(pipeline_sql, database=database, timeout=120)
    return source, pipeline


def pipeline_id(sql: DatalayersHttpSql, database: str, pipeline: str) -> int:
    return sql.scalar_i64(
        f"SELECT pipeline_id FROM information_schema.pipelines WHERE pipeline_name = '{pipeline}'",
        database,
    )


def cleanup_objects(
    sql: DatalayersHttpSql,
    database: str,
    source: str,
    sink: str,
    pipeline: str,
    sink_mode: SinkMode,
) -> None:
    log(f"Cleaning up Datalayers objects in {database} for pipeline {pipeline}")
    statements = [
        f"ALTER PIPELINE {pipeline} STOP",
        f"DROP PIPELINE {pipeline}",
        f"DROP SOURCE {source}",
    ]
    if sink_mode == SinkMode.TABLE:
        statements.append(f"DROP TABLE {sink}")
    else:
        statements.append(f"DROP SINK {sink}")
    for statement in statements:
        try:
            sql.run(statement, database=database)
        except Exception:
            pass
    try:
        statement = f"DROP DATABASE {database}"
        sql.run(statement)
    except Exception:
        pass


def preclean_benchmark_databases(
    sql: DatalayersHttpSql, queries: list[QuerySpec], sink_mode: SinkMode
) -> None:
    for query in queries:
        database = f"nexmark_{query.name}"
        source = f"{query.name}_src"
        pipeline = f"{query.name}_pipeline"
        sink = (
            f"{query.name}_sink" if sink_mode == SinkMode.TABLE else f"{query.name}_bh"
        )
        log(f"Pre-cleaning stale Datalayers objects in {database}")
        statements = [
            f"ALTER PIPELINE {pipeline} STOP",
            f"DROP PIPELINE {pipeline}",
        ]
        if sink_mode == SinkMode.TABLE:
            statements.append(f"DROP TABLE {sink}")
        else:
            statements.append(f"DROP SINK {sink}")
        statements.append(f"DROP SOURCE {source}")
        for statement in statements:
            try:
                sql.run(statement, database=database)
            except Exception:
                pass
        try:
            statement = f"DROP DATABASE {database}"
            sql.run(statement)
        except Exception:
            pass


def markdown_report(
    args: argparse.Namespace,
    dataset_stats: dict[str, int],
    results: list[dict[str, object]],
) -> str:
    # Keep the report self-describing because benchmark results are often inspected after the
    # original terminal session is gone.
    lines = [
        "# Datalayers Nexmark Benchmark Report",
        "",
        f"- Generated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"- Kafka brokers: `{args.kafka_brokers}`",
        f"- Topic partitions: `{dataset_stats['partitions']}`",
        f"- Fixture: `official keyed bid dataset`",
        f"- Sink mode: `{args.sink}`",
        f"- Input rows: `{dataset_stats['total_rows']}`",
        f"- Dataset path: `{Path(args.dataset).resolve()}`",
        f"- Measurement mode: preload Kafka then replay from `earliest`",
        "",
        "## Dataset",
        "",
        "| Field | Type | Notes |",
        "| --- | --- | --- |",
        "| `ts` | `TIMESTAMP(9)` | ISO-8601 event time |",
        "| `auction` | `BIGINT` | From official Nexmark `Bid` events |",
        "| `bidder` | `BIGINT` | From official Nexmark `Bid` events |",
        "| `price` | `BIGINT` | From official Nexmark `Bid` events |",
        "| `channel` | `STRING` | Flattened from official Nexmark `Bid` events |",
        "| `url` | `STRING` | Flattened from official Nexmark `Bid` events |",
        "| `extra` | `STRING` | Flattened from official Nexmark `Bid` events |",
        "",
        "## Results",
        "",
        "| Query | Input Rows | Expected Rows | Inserted Rows | Replay Seconds | Throughput (input records/s) | Avg CPU (%) | Avg Mem (GiB) | Kafka Preload Seconds | State |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in results:
        lines.append(
            "| {query} | {input_rows} | {expected_rows} | {inserted_rows} | {replay_sec:.3f} | {throughput_rps:.1f} | {avg_cpu_percent:.2f} | {avg_mem_gib:.3f} | {kafka_preload_sec:.3f} | {state} |".format(
                **item
            )
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- `q0/q1/q2/q14/q21/q22` are treated as supported benchmark scenarios.",
            "- `sink=table` writes into a Datalayers table and waits on both `COUNT(*)` and Kafka consumer lag reaching 0.",
            "- `sink=blackhole` writes into a Datalayers blackhole sink and waits on Kafka consumer lag reaching 0.",
            "- `throughput` is computed as input rows divided by replay time.",
            "- `avg cpu` and `avg mem` are sampled from the detected local Datalayers PID when available; otherwise they remain 0.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="运行 Datalayers Nexmark benchmark", add_help=False
    )
    parser.add_argument("-h", "--help", action="help", help="显示本帮助信息并退出。")
    parser._positionals.title = "位置参数"
    parser._optionals.title = "可选参数"
    parser.add_argument(
        "--host", default="127.0.0.1", help="Datalayers HTTP SQL host。"
    )
    parser.add_argument(
        "--port", type=int, default=8361, help="Datalayers HTTP SQL 端口。"
    )
    parser.add_argument("--user", default="admin", help="Datalayers 用户名。")
    parser.add_argument("--password", default="public", help="Datalayers 密码。")
    parser.add_argument(
        "--kafka-brokers",
        default="127.0.0.1:9092",
        help="Datalayers source 中使用的 Kafka bootstrap servers。",
    )
    parser.add_argument(
        "--kafka-container",
        default="datalayers-nexmark-kafka",
        help="用于管理 topic 和 preload 数据的 Kafka 容器名。",
    )
    parser.add_argument(
        "--workdir",
        default=".datalayers-nexmark",
        help="报告和中间文件的工作目录。",
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
        "--sink",
        choices=[mode.value for mode in SinkMode],
        default=SinkMode.TABLE.value,
        help="sink 类型：table 通过行数判定完成，blackhole 通过 lag 判定完成。",
    )
    parser.add_argument(
        "--no-cleanup",
        type=int,
        choices=[0, 1],
        default=0,
        help="设为 1 时保留 benchmark 创建的 SQL 对象和 Kafka topic 数据。",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="每个 query 的完成等待超时时间，单位秒。",
    )
    parser.add_argument(
        "--engine-pid",
        type=int,
        help="待采样的 Datalayers 进程 PID；未提供时 CPU 和内存统计保持为 0。",
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
        f"Starting Datalayers Nexmark benchmark: dataset={args.dataset} queries={args.queries} "
        f"sink={args.sink} host={args.host} port={args.port} workdir={args.workdir}"
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
            "docker is required because the topic loader uses kafka tooling inside the Kafka container"
        )
    sink_mode = SinkMode(args.sink)

    sql = DatalayersHttpSql(args.host, args.port, args.user, args.password)
    workdir.mkdir(parents=True, exist_ok=True)

    dataset_path = Path(args.dataset).resolve()
    dataset_stats = load_bid_dataset_stats(dataset_path)
    topic_partitions = dataset_stats["partitions"]
    fixture_metadata = {"dataset_path": str(dataset_path)}
    preclean_benchmark_databases(sql, queries, sink_mode)

    results: list[dict[str, object]] = []
    for query in queries:
        if query.unsupported:
            log(f"Skipping unsupported query {query.name} for Datalayers", color=GREEN)
            results.append(
                {
                    "query": query.name,
                    "input_rows": dataset_stats["total_rows"],
                    "expected_rows": "N/A",
                    "inserted_rows": "N/A",
                    "replay_sec": "N/A",
                    "throughput_rps": "N/A",
                    "avg_cpu_percent": "N/A",
                    "avg_mem_gib": "N/A",
                    "kafka_preload_sec": "N/A",
                    "sink_mode": sink_mode.value,
                    "state": "N/A",
                    "sample_csv": "N/A",
                }
            )
            continue
        log(f"Starting benchmark for query {query.name}", color=GREEN)
        topic = f"nexmark_{query.name}"
        database = f"nexmark_{query.name}"
        total_rows = dataset_stats["total_rows"]
        existing = count_kafka_messages(args.kafka_container, topic)
        if existing is not None and existing == total_rows:
            log(f"Topic {topic} already has {existing} messages, skipping preload")
            kafka_preload_sec = 0.0
        else:
            if existing is not None:
                log(
                    f"Topic {topic} has {existing} messages (expected {total_rows}), will reload"
                )
            ensure_topic(args.kafka_container, topic, topic_partitions)
            kafka_preload_sec = load_topic(args.kafka_container, topic, dataset_path)
        source = ""
        sink = ""
        pipeline = ""
        try:
            create_database(sql, database)
            if sink_mode == SinkMode.TABLE:
                sink = create_table_sink(sql, database, query)
            else:
                sink = create_blackhole_sink(sql, database, query)
            expected_rows = query.expected_rows(dataset_stats)
            sample_csv = workdir / f"{query.name}_samples.csv"
            monitor = (
                ProcessMonitor(args.engine_pid, sample_csv, args.sample_interval)
                if args.engine_pid is not None
                else None
            )
            source, pipeline = create_source_and_pipeline(
                sql, database, topic, args.kafka_brokers, query, sink
            )
            replay_t0 = time.time()
            log(f"Starting replay window for query {query.name}")
            progress_stop = threading.Event()
            if args.engine_pid is not None:
                log(f"Monitoring Datalayers PID: {args.engine_pid}")
                pagesize = os.sysconf(os.sysconf_names["SC_PAGESIZE"])
                prev = {"utime": 0, "stime": 0, "ts": 0.0}

                def _log_progress() -> None:
                    while not progress_stop.is_set():
                        try:
                            with open(f"/proc/{args.engine_pid}/stat", "r") as f:
                                content = f.read()
                            close_paren = content.rfind(")")
                            fields = content[close_paren + 2 :].split()
                            utime = int(fields[11])
                            stime = int(fields[12])
                            rss_kib = (int(fields[21]) * pagesize) // 1024
                            now = time.time()
                            cpu_pct = 0.0
                            if prev["ts"] > 0:
                                delta = (utime - prev["utime"]) + (
                                    stime - prev["stime"]
                                )
                                dt = now - prev["ts"]
                                if dt > 0:
                                    cpu_pct = (delta / _CLK_TCK) / dt * 100.0
                            prev["utime"] = utime
                            prev["stime"] = stime
                            prev["ts"] = now
                            log(
                                f"CPU={cpu_pct:.1f}% RES={rss_kib / 1024 / 1024:.2f} GiB"
                            )
                        except Exception:
                            pass
                        progress_stop.wait(1.0)

                progress_thread = threading.Thread(target=_log_progress, daemon=True)
                progress_thread.start()
            if monitor is not None:
                monitor.start()
            try:
                if sink_mode == SinkMode.TABLE:
                    current_pipeline_id = pipeline_id(sql, database, pipeline)
                    group = f"datalayers-{current_pipeline_id}-group"
                    inserted_rows, _ = wait_for_count(
                        sql,
                        database,
                        sink,
                        expected_rows,
                        args.kafka_container,
                        group,
                        topic,
                        args.timeout,
                    )
                else:
                    current_pipeline_id = pipeline_id(sql, database, pipeline)
                    group = f"datalayers-{current_pipeline_id}-group"
                    inserted_rows, _ = wait_for_group_lag_zero(
                        args.kafka_container, group, topic, args.timeout
                    )
                    inserted_rows = expected_rows
            finally:
                progress_stop.set()
                if monitor is not None:
                    monitor.stop()
            replay_t1 = time.time()
            replay_sec = replay_t1 - replay_t0
            if monitor is not None:
                avg_cpu_percent, avg_mem_gib = summarize_samples(sample_csv)
            else:
                avg_cpu_percent, avg_mem_gib = 0.0, 0.0
            state = pipeline_state(sql, database, pipeline)
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
                    "state": state,
                    "sample_csv": str(sample_csv),
                }
            )
        finally:
            if not args.no_cleanup and source and sink and pipeline:
                cleanup_objects(sql, database, source, sink, pipeline, sink_mode)
            if not args.no_cleanup:
                try:
                    ensure_topic(args.kafka_container, topic, topic_partitions)
                except Exception:
                    pass

    report_md.write_text(
        markdown_report(args, dataset_stats, results), encoding="utf-8"
    )
    report_json.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "engine": "datalayers",
                "mode": "preload_then_earliest_replay",
                "fixture": "official keyed bid dataset",
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
        log("No cleanup requested, keeping Kafka container data and Datalayers objects")
    print(report_md)
    print(report_json)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BenchError as err:
        print(f"error: {err}", file=sys.stderr)
        raise SystemExit(1)
