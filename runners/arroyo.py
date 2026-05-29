#!/usr/bin/env python3
"""
这个 bench runner 用来在本地单节点 Arroyo 上运行一组固定的 Nexmark 风格查询。

它消费的是一个已经预先准备好的 keyed bid JSONL dataset。`benches/arroyo.sh` 负责启动
Kafka 和 Arroyo 单容器，并把 API、Kafka 和工作目录参数传入本文件。

执行方式：

1. 读取与 dataset 关联的 stats JSON，拿到输入行数和几个 query 的理论输出行数。
2. 对每个 query：
   - 重建独立的 Kafka topic
   - 把 keyed dataset preload 到 topic
   - 通过 Arroyo REST API 提交一条包含 Kafka source 和 blackhole sink 的 SQL pipeline
   - 在 replay 窗口内采样 Arroyo 容器主进程的 CPU 和 RSS
   - 通过显式指定的 Kafka source consumer group lag 到 0 判定 source 已消费完输入
3. 每个 query 的结果会写入 `report.md`、`report.json` 和采样 CSV。
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
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
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
    updating: bool = False


QUERY_SPECS: dict[str, QuerySpec] = {
    "q0": QuerySpec(
        name="q0",
        sink_columns="""
            ts TIMESTAMP,
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel TEXT,
            url TEXT
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
            ts TIMESTAMP,
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            price_eur DOUBLE,
            channel TEXT,
            url TEXT
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
            ts TIMESTAMP,
            auction BIGINT,
            price BIGINT
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
            price DOUBLE,
            bid_time_type TEXT,
            ts TIMESTAMP,
            extra TEXT
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
            ts TIMESTAMP,
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel TEXT,
            channel_id TEXT
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
                ELSE regexp_match(url, 'channel_id=([^&]*)')[1]
              END AS channel_id
            FROM {source}
            WHERE regexp_match(url, 'channel_id=([^&]*)')[1] IS NOT NULL
               OR LOWER(channel) IN ('apple', 'google', 'facebook', 'baidu')
        """,
        expected_rows=lambda stats: stats["q21_expected_rows"],
    ),
    "q22": QuerySpec(
        name="q22",
        sink_columns="""
            ts TIMESTAMP,
            auction BIGINT,
            bidder BIGINT,
            price BIGINT,
            channel TEXT,
            dir1 TEXT,
            dir2 TEXT,
            dir3 TEXT
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
        sink_columns="""
            channel TEXT,
            total_bids BIGINT,
            min_price BIGINT,
            max_price BIGINT,
            avg_price DOUBLE,
            distinct_bidders BIGINT,
            distinct_auctions BIGINT
        """,
        select_sql="""
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
        updating=True,
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
        select_sql="""
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
        updating=True,
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


class ArroyoApi:
    def __init__(self, host: str, port: int):
        self.base = f"http://{host}:{port}/api/v1"

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, object] | None = None,
        timeout: int = 60,
    ) -> object:
        data = None
        headers: dict[str, str] = {}
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            url=self.base + path,
            method=method,
            data=data,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = resp.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            raise BenchError(
                f"arroyo api {method} {path} failed: {exc.code} {detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise BenchError(f"arroyo api {method} {path} failed: {exc}") from exc
        if not body:
            return None
        return json.loads(body.decode("utf-8"))


_CLK_TCK = os.sysconf(os.sysconf_names["SC_CLK_TCK"])


class ContainerMonitor:
    def __init__(self, container: str, output_csv: Path, sample_interval: float):
        self.container = container
        self.output_csv = output_csv
        self.sample_interval = sample_interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._prev_utime: int | None = None
        self._prev_stime: int | None = None
        self._prev_ts: float | None = None
        self._cgroup_procs_path: str | None = None

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

    def _resolve_cgroup(self, pid: str) -> None:
        if self._cgroup_procs_path is not None:
            return
        try:
            with open(f"/proc/{pid}/cgroup", "r") as f:
                for line in f:
                    line = line.strip()
                    if ":name=systemd:" in line or line.startswith("0::"):
                        cgroup_path = line.split(":", 2)[-1]
                        procs_path = f"/sys/fs/cgroup{cgroup_path}/cgroup.procs"
                        if os.path.isfile(procs_path):
                            self._cgroup_procs_path = procs_path
                            return
        except (FileNotFoundError, PermissionError):
            pass

    def _read_proc_stats(self) -> tuple[int, int, int] | None:
        if self._cgroup_procs_path is None:
            return None
        pagesize = os.sysconf(os.sysconf_names["SC_PAGESIZE"])
        total_utime = 0
        total_stime = 0
        total_rss = 0
        try:
            with open(self._cgroup_procs_path, "r") as f:
                pids = [line.strip() for line in f if line.strip()]
        except (FileNotFoundError, PermissionError):
            return None
        for pid in pids:
            try:
                with open(f"/proc/{pid}/stat", "r") as f:
                    content = f.read()
            except (FileNotFoundError, PermissionError):
                continue
            close_paren = content.rfind(")")
            if close_paren < 0:
                continue
            fields = content[close_paren + 2 :].split()
            if len(fields) < 22:
                continue
            try:
                total_utime += int(fields[11])
                total_stime += int(fields[12])
                total_rss += int(fields[21])
            except (IndexError, ValueError):
                continue
        rss_kib = (total_rss * pagesize) // 1024
        return total_utime, total_stime, rss_kib

    def _get_pid(self) -> str | None:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Pid}}", self.container],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return None
        pid = result.stdout.strip()
        if not pid or pid == "0":
            return None
        self._resolve_cgroup(pid)
        return pid

    def _read_sample(self) -> tuple[float, float, int] | None:
        pid = self._get_pid()
        if not pid:
            return None
        stat = self._read_proc_stats()
        if stat is None:
            return None
        utime, stime, rss_kib = stat
        now = time.time()
        cpu_percent: float = 0.0
        if (
            self._prev_ts is not None
            and self._prev_utime is not None
            and self._prev_stime is not None
        ):
            delta_cpu = (utime - self._prev_utime) + (stime - self._prev_stime)
            delta_time = now - self._prev_ts
            if delta_time > 0:
                cpu_percent = (delta_cpu / _CLK_TCK) / delta_time * 100.0
        self._prev_utime = utime
        self._prev_stime = stime
        self._prev_ts = now
        return now, cpu_percent, rss_kib


def read_container_sample(container: str) -> tuple[float, float, int] | None:
    pid_result = subprocess.run(
        [
            "docker",
            "inspect",
            "-f",
            "{{.State.Pid}}",
            container,
        ],
        capture_output=True,
        text=True,
        timeout=5,
    )
    if pid_result.returncode != 0:
        return None
    pid = pid_result.stdout.strip()
    if not pid or pid == "0":
        return None
    result = subprocess.run(
        [
            "ps",
            "-p",
            pid,
            "-o",
            "%cpu=",
            "-o",
            "rss=",
        ],
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


def wait_for_group_lag_zero(
    container: str, group: str, topic: str, timeout: int
) -> None:
    log(f"Waiting for Kafka group `{group}` to finish consuming topic `{topic}`")
    start = time.time()
    last_progress = start
    stable_polls = 0
    last_lag: int | None = None
    while True:
        rows = kafka_group_offsets(container, group, topic)
        if rows:
            # Arroyo commits the last processed Kafka offset to the consumer group, while its
            # internal recovery state stores offset + 1. That means a fully drained partition can
            # still appear with lag = 1 in kafka-consumer-groups output.
            if all(
                current + lag == end and lag in (0, 1) for current, end, lag in rows
            ):
                stable_polls += 1
                if stable_polls >= 3:
                    return
            else:
                stable_polls = 0
        total_lag = sum(lag for _, _, lag in rows) if rows else 0
        if last_lag is None or total_lag != last_lag:
            last_progress = time.time()
        if time.time() - last_progress > timeout:
            raise BenchError(
                f"timeout waiting for kafka group `{group}` to drain topic `{topic}`, current={rows}"
            )
        last_lag = total_lag
        time.sleep(1)


def wait_for_job_state(
    api: ArroyoApi,
    pipeline_id: str,
    terminal_only: bool = False,
    timeout: int = 600,
) -> dict[str, object]:
    start = time.time()
    while True:
        payload = api.request("GET", f"/pipelines/{pipeline_id}/jobs", timeout=30)
        jobs = payload["data"]
        if jobs:
            job = jobs[0]
            state = str(job["state"])
            if terminal_only:
                if state in {"Stopped", "Finished", "Failed"}:
                    return job
            else:
                if state not in {"Created", "Compiling", "Scheduling"}:
                    return job
        if time.time() - start > timeout:
            raise BenchError(
                f"timeout waiting for job state for pipeline {pipeline_id}"
            )
        time.sleep(1)


def stop_and_delete_pipeline(
    api: ArroyoApi, pipeline_id: str, *, timeout: int = 300
) -> None:
    api.request("PATCH", f"/pipelines/{pipeline_id}", {"stop": "immediate"}, timeout=30)
    wait_for_job_state(api, pipeline_id, terminal_only=True, timeout=timeout)
    api.request("DELETE", f"/pipelines/{pipeline_id}", timeout=30)


def render_sql(
    topic: str,
    query: QuerySpec,
    source_name: str,
    sink_name: str,
    kafka_brokers: str,
    group_id: str,
    sink_mode: str,
) -> str:
    effective_sink = "table" if query.updating else sink_mode
    if effective_sink == "blackhole":
        sink_ddl = f"""
CREATE TABLE {sink_name} (
  {query.sink_columns}
) WITH (
  'connector' = 'blackhole'
);
""".strip()
    else:
        sink_ddl = f"""
CREATE TABLE {sink_name} (
  {query.sink_columns}
);
""".strip()

    return f"""
CREATE TABLE {source_name} (
  auction BIGINT,
  bidder BIGINT,
  price BIGINT,
  channel TEXT,
  url TEXT,
  ts TIMESTAMP NOT NULL,
  extra TEXT,
  WATERMARK FOR ts AS ts
) WITH (
  'connector' = 'kafka',
  'format' = 'json',
  'type' = 'source',
  'bootstrap_servers' = '{kafka_brokers}',
  'topic' = '{topic}',
  'source.offset' = 'earliest',
  'source.group_id' = '{group_id}'
);

{sink_ddl}

INSERT INTO {sink_name}
{query.select_sql.format(source=source_name).strip()};
""".strip()


def markdown_report(
    args: argparse.Namespace,
    dataset_stats: dict[str, int],
    results: list[dict[str, object]],
    sink_mode: str,
) -> str:
    lines = [
        "# Arroyo Nexmark Benchmark Report",
        "",
        f"- Generated at: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"- Arroyo container: `{args.arroyo_container}`",
        f"- Kafka brokers in Arroyo: `{args.kafka_brokers}`",
        f"- Topic partitions: `{dataset_stats['partitions']}`",
        f"- Pipeline parallelism: `{args.parallelism}`",
        f"- Fixture: `official keyed bid dataset`",
        f"- Sink mode: `{sink_mode}`",
        f"- Input rows: `{dataset_stats['total_rows']}`",
        f"- Dataset path: `{Path(args.dataset).resolve()}`",
        f"- Measurement mode: preload Kafka then replay from `earliest`",
        "",
        "## Results",
        "",
        "| Query | Input Rows | Expected Rows | Replay Seconds | Throughput (input records/s) | Avg CPU (%) | Avg Mem (GiB) | Kafka Preload Seconds | State |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for item in results:
        lines.append(
            "| {query} | {input_rows} | {expected_rows} | {replay_sec:.3f} | {throughput_rps:.1f} | {avg_cpu_percent:.2f} | {avg_mem_gib:.3f} | {kafka_preload_sec:.3f} | {state} |".format(
                **item
            )
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This benchmark runs Arroyo in a single Docker container and samples the container main process during the replay window.",
            "- Every query creates a dedicated Kafka source with an explicit `source.group_id` and waits for that Kafka consumer group lag to reach 0.",
            "- The sink uses either Arroyo's blackhole connector or an in-memory table, so completion is defined by source consumption rather than sink materialization.",
            "- `throughput` is computed as input rows divided by replay time.",
        ]
    )
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="针对预先生成好的 keyed bid JSONL dataset 运行 Arroyo Nexmark benchmark。",
        add_help=False,
    )
    parser.add_argument("-h", "--help", action="help", help="显示本帮助信息并退出。")
    parser._positionals.title = "位置参数"
    parser._optionals.title = "可选参数"
    parser.add_argument("--host", default="127.0.0.1", help="Arroyo API host。")
    parser.add_argument("--port", type=int, default=5115, help="Arroyo API 端口。")
    parser.add_argument(
        "--arroyo-container",
        required=True,
        help="用于采样 CPU 和内存的 Arroyo 容器名。",
    )
    parser.add_argument(
        "--kafka-brokers",
        default="kafka:9092",
        help="Arroyo SQL source 中使用的 Kafka bootstrap servers。",
    )
    parser.add_argument(
        "--kafka-container",
        default="arroyo-nexmark-kafka",
        help="用于管理 topic 和 preload 数据的 Kafka 容器名。",
    )
    parser.add_argument(
        "--workdir",
        default=".arroyo-nexmark",
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
        help="Arroyo pipeline 并行度。",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=600,
        help="每个 query 的完成等待超时时间，单位秒。",
    )
    parser.add_argument(
        "--sink",
        default="blackhole",
        choices=["table", "blackhole"],
        help="sink 类型：table 写入 in-memory 表，blackhole 写入 blackhole sink。",
    )
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=1.0,
        help="CPU 和 RSS 采样间隔，单位秒。",
    )
    parser.add_argument(
        "--no-cleanup",
        type=int,
        choices=[0, 1],
        default=0,
        help="设为 1 时保留 benchmark 创建的 pipeline 和 Kafka topic 数据。",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_cli_args(args)
    log(
        f"Starting Arroyo Nexmark benchmark: dataset={args.dataset} queries={args.queries} "
        f"host={args.host} port={args.port} workdir={args.workdir}"
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

    api = ArroyoApi(args.host, args.port)
    workdir.mkdir(parents=True, exist_ok=True)
    dataset_path = Path(args.dataset).resolve()
    dataset_stats = load_bid_dataset_stats(dataset_path)
    topic_partitions = dataset_stats["partitions"]
    fixture_metadata = {"dataset_path": str(dataset_path)}
    if args.parallelism < 1:
        raise BenchError("--parallelism must be >= 1")

    sink_mode = args.sink

    results: list[dict[str, object]] = []
    for query in queries:
        log(f"Starting benchmark for query {query.name}", color=GREEN)
        pipeline_id: str | None = None
        suffix = f"{int(time.time() * 1000)}"
        topic = f"nexmark_{query.name}"
        source = f"{query.name}_src_{suffix}"
        sink = f"{query.name}_{'bh' if sink_mode == 'blackhole' else 'tbl'}_{suffix}"
        group = f"nexmark-arroyo-{query.name}-{suffix}"
        pipeline_name = f"nexmark_{query.name}_{suffix}"
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
        try:
            expected_rows = query.expected_rows(dataset_stats)
            sql = render_sql(
                topic, query, source, sink, args.kafka_brokers, group, sink_mode
            )
            log_sql(
                f"Create source/sink and trigger INSERT pipeline for {query.name}",
                sql,
            )
            sample_csv = workdir / f"{query.name}_samples.csv"
            monitor = ContainerMonitor(
                args.arroyo_container, sample_csv, args.sample_interval
            )
            pipeline = api.request(
                "POST",
                "/pipelines",
                {
                    "name": pipeline_name,
                    "query": sql,
                    "parallelism": args.parallelism,
                    "checkpoint_interval_micros": 24 * 60 * 60 * 1_000_000,
                },
                timeout=120,
            )
            pipeline_id = str(pipeline["id"])
            job = wait_for_job_state(
                api, pipeline_id, terminal_only=False, timeout=args.timeout
            )
            state = str(job["state"])
            if state == "Failed":
                raise BenchError(
                    f"arroyo job failed immediately for query {query.name}"
                )
            replay_t0 = time.time()
            log(f"Starting replay window for query {query.name}")
            monitor.start()
            try:
                wait_for_group_lag_zero(
                    args.kafka_container, group, topic, args.timeout
                )
                job = wait_for_job_state(
                    api, pipeline_id, terminal_only=False, timeout=30
                )
                state = str(job["state"])
            finally:
                monitor.stop()
            replay_t1 = time.time()
            replay_sec = replay_t1 - replay_t0
            avg_cpu_percent, avg_mem_gib = summarize_samples(sample_csv)
            log(
                f"Finished benchmark for query {query.name}: "
                f"replay_sec={replay_sec:.3f} throughput={dataset_stats['total_rows'] / replay_sec if replay_sec > 0 else 0.0:.1f}",
                color=GREEN,
            )
            results.append(
                {
                    "query": query.name,
                    "input_rows": dataset_stats["total_rows"],
                    "expected_rows": expected_rows,
                    "replay_sec": round(replay_sec, 3),
                    "throughput_rps": round(dataset_stats["total_rows"] / replay_sec, 1)
                    if replay_sec > 0
                    else 0.0,
                    "avg_cpu_percent": round(avg_cpu_percent, 2),
                    "avg_mem_gib": round(avg_mem_gib, 3),
                    "kafka_preload_sec": round(kafka_preload_sec, 3),
                    "state": state,
                    "sample_csv": str(sample_csv),
                    "pipeline_id": pipeline_id,
                    "job_id": str(job["id"]),
                }
            )
        finally:
            if not args.no_cleanup:
                try:
                    if pipeline_id is not None:
                        stop_and_delete_pipeline(api, pipeline_id, timeout=args.timeout)
                except Exception:
                    pass
                try:
                    run_cmd(
                        [
                            "docker",
                            "exec",
                            args.kafka_container,
                            "kafka-topics",
                            "--bootstrap-server",
                            "127.0.0.1:9092",
                            "--delete",
                            "--if-exists",
                            "--topic",
                            topic,
                        ],
                        timeout=30,
                    )
                except Exception:
                    pass

    report_md.write_text(
        markdown_report(args, dataset_stats, results, sink_mode), encoding="utf-8"
    )
    report_json.write_text(
        json.dumps(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "engine": "arroyo",
                "mode": "preload_then_earliest_replay",
                "fixture": "official keyed bid dataset",
                "fixture_metadata": fixture_metadata,
                "dataset_stats": dataset_stats,
                "parallelism": args.parallelism,
                "sink_mode": sink_mode,
                "results": results,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    log(f"Benchmark completed, report written to {report_md}")
    if args.no_cleanup:
        log("No cleanup requested, keeping Kafka container data and Arroyo pipelines")
    print(report_md)
    print(report_json)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
