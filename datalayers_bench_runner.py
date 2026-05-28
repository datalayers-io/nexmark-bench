#!/usr/bin/env python3
"""
这个 bench runner 用来在本地 Datalayers 实例上运行一组固定的 Nexmark 风格基准查询。

它消费的是一个已经预先准备好的 keyed bid JSONL dataset。这个 dataset 通常由
`datagen.sh` 调用 `nexmark_fixture.py` 生成，并稳定保存在 `nexmark-bench` 目录下。

执行方式：

1. `bench_datalayers.sh` 负责启动 Kafka，并把 Datalayers HTTP SQL 连接参数传入本文件。
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
  当前固定为 `0.0`，因为远端 HTTP 连接模式下不负责采样 Datalayers 进程。
- `avg_mem_gib`
  当前固定为 `0.0`，因为远端 HTTP 连接模式下不负责采样 Datalayers 进程。
- `kafka_preload_sec`
  把本轮 query 输入数据写入 Kafka topic 的耗时，单独记录，不计入 throughput 分母。

注意：

- 这是一个“preload Kafka -> earliest replay -> wait for sink completion”的 benchmark runner。
- 它统计的是本地 harness 口径下的 replay 性能，不等同于其他系统官方网页上的 blackhole benchmark 口径。
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable

import base64
import urllib.parse
import urllib.request

from nexmark_fixture import load_bid_dataset_stats


RESET = "\033[0m"
GREEN = "\033[32m"


@dataclass
class QuerySpec:
    name: str
    sink_columns: str
    select_sql: str
    expected_rows: Callable[[dict[str, int]], int]


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
    sql: DatalayersHttpSql, database: str, table: str, expected_rows: int, timeout: int
) -> tuple[int, float]:
    # Table sink completion is defined as "the sink table has observed at least the expected
    # number of output rows for the current query semantics".
    log(f"Waiting for sink table {database}.{table} to reach {expected_rows} rows")
    start = time.time()
    while True:
        count = sql.scalar_i64(f"SELECT COUNT(*) AS c FROM {table}", database)
        if count >= expected_rows:
            return count, time.time() - start
        if time.time() - start > timeout:
            raise BenchError(
                f"timeout waiting for {table} to reach {expected_rows} rows, current={count}"
            )
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
    while True:
        lag = kafka_group_lag(container, group, topic)
        if lag == 0:
            return 0, time.time() - start
        if time.time() - start > timeout:
            raise BenchError(
                f"timeout waiting for kafka group `{group}` lag to reach 0 on topic `{topic}`, current={lag}"
            )
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
    sql.run(f"CREATE DATABASE IF NOT EXISTS {database}")


def create_table_sink(
    sql: DatalayersHttpSql,
    database: str,
    query: QuerySpec,
) -> str:
    sink = f"{query.name}_sink"
    sql.run(
        f"""
        CREATE TABLE {sink} (
            {query.sink_columns}
        )
        ENGINE=TimeSeries
        PARTITION BY HASH(auction)
        PARTITIONS 1
        """,
        database=database,
    )
    return sink


def create_blackhole_sink(
    sql: DatalayersHttpSql, database: str, query: QuerySpec
) -> str:
    sink = f"{query.name}_bh"
    sql.run(f"CREATE SINK {sink} WITH (connector='blackhole')", database=database)
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
    sql.run(
        f"""
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
        """,
        database=database,
    )
    sql.run(
        f"""
        CREATE PIPELINE {pipeline}
        SINK TO {sink}
        AS
        {query.select_sql.format(source=source)}
        """,
        database=database,
        timeout=120,
    )
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
        sql.run(f"DROP DATABASE {database}")
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
            sql.run(f"DROP DATABASE {database}")
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
        f"- Topic partitions: `{args.partitions}`",
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
            "- `sink=table` writes into a Datalayers table and waits on `COUNT(*)`.",
            "- `sink=blackhole` writes into a Datalayers blackhole sink and waits on Kafka consumer lag reaching 0.",
            "- `throughput` is computed as input rows divided by replay time.",
            "- `avg cpu` and `avg mem` are sampled from the Datalayers process during the replay window.",
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
        "--partitions", type=int, default=4, help="Kafka topic 分区数。"
    )
    parser.add_argument(
        "--queries",
        default="q0,q1,q2,q14,q21,q22",
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
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
    fixture_metadata = {"dataset_path": str(dataset_path)}
    preclean_benchmark_databases(sql, queries, sink_mode)

    results: list[dict[str, object]] = []
    for query in queries:
        log(f"Starting benchmark for query {query.name}", color=GREEN)
        topic = f"nexmark_{query.name}"
        database = f"nexmark_{query.name}"
        ensure_topic(args.kafka_container, topic, args.partitions)
        source = ""
        sink = ""
        pipeline = ""
        try:
            create_database(sql, database)
            if sink_mode == SinkMode.TABLE:
                sink = create_table_sink(sql, database, query)
            else:
                sink = create_blackhole_sink(sql, database, query)
            kafka_preload_sec = load_topic(args.kafka_container, topic, dataset_path)
            expected_rows = query.expected_rows(dataset_stats)
            replay_t0 = time.time()
            log(f"Starting replay window for query {query.name}")
            # The replay window intentionally includes source and pipeline creation.
            source, pipeline = create_source_and_pipeline(
                sql, database, topic, args.kafka_brokers, query, sink
            )
            if sink_mode == SinkMode.TABLE:
                inserted_rows, _ = wait_for_count(
                    sql, database, sink, expected_rows, args.timeout
                )
            else:
                current_pipeline_id = pipeline_id(sql, database, pipeline)
                group = f"datalayers-{current_pipeline_id}-group"
                inserted_rows, _ = wait_for_group_lag_zero(
                    args.kafka_container, group, topic, args.timeout
                )
                inserted_rows = expected_rows
            replay_t1 = time.time()
            replay_sec = replay_t1 - replay_t0
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
                }
            )
        finally:
            if not args.no_cleanup and source and sink and pipeline:
                cleanup_objects(sql, database, source, sink, pipeline, sink_mode)
            if not args.no_cleanup:
                try:
                    ensure_topic(args.kafka_container, topic, args.partitions)
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
