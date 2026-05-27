#!/usr/bin/env python3
"""
这个文件负责把官方 Nexmark 数据生成链路适配成当前仓库里三个 benchmark runner
可以直接复用的输入 fixture。

它本身不是一个完整 benchmark runner，不负责执行 query，也不负责统计吞吐量。
它只负责“准备输入数据”，供 Datalayers、RisingWave、Flink 三个 runner 共享。

整体流程：

1. 下载或复用缓存的 `nexmark-flink` 发布包、Flink 发行版、Kafka SQL connector。
2. 启动一个临时本地 Flink standalone 运行环境。
3. 调用官方 Nexmark SQL/datagen，把 combined Nexmark events 写入 Kafka topic。
4. 从 combined events 中抽取 `Bid` 事件，并扁平化为本地 benchmark 使用的 JSON 行格式。
5. 统计这批数据在当前支持 query 下的理论输出行数，例如：
   - `q2_expected_rows`
   - `q14_expected_rows`
   - `q21_expected_rows`
6. 把结果返回给各个 runner，由 runner 再将其 preload 到每个 query 的专用 topic。

这个文件输出的核心结果是：

- `dataset_path`
  扁平化后的 bid fixture 文件路径。
- `stats`
  这批输入数据的统计信息，用于各 runner 做完成判定和结果校验。
- `metadata`
  fixture 的来源和生成细节，写入最终 benchmark report。

注意：

- 这里说的“官方”指的是 `nexmark/nexmark` 仓库里的 `nexmark-flink` 生成链路。
- 本文件做的是“官方 generator 的封装和适配”，不是重新实现一个 Nexmark generator。
"""

from __future__ import annotations

import json
import math
import os
import shutil
import socket
import subprocess
import tarfile
import time
import urllib.request
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Callable


FLINK_VERSION = "1.13.6"
FLINK_DOCKER_IMAGE = "flink:1.13.6-scala_2.11-java8"
FLINK_DIST_URL = (
    f"https://archive.apache.org/dist/flink/flink-{FLINK_VERSION}/"
    f"flink-{FLINK_VERSION}-bin-scala_2.11.tgz"
)
FLINK_KAFKA_CONNECTOR_URL = (
    "https://repo1.maven.org/maven2/org/apache/flink/"
    f"flink-sql-connector-kafka_2.11/{FLINK_VERSION}/"
    f"flink-sql-connector-kafka_2.11-{FLINK_VERSION}.jar"
)
NEXMARK_FLINK_URL = (
    "https://github.com/nexmark/nexmark/releases/download/v0.2.0/nexmark-flink.tgz"
)
OFFICIAL_EVENT_TOPIC = "nexmark"
OFFICIAL_EVENT_RATIOS = (1, 3, 46)
OFFICIAL_BID_FRACTION = OFFICIAL_EVENT_RATIOS[2] / sum(OFFICIAL_EVENT_RATIOS)
DEFAULT_TARGET_BID_ROWS = 1_000_000
FLINK_JAVA_MODULE_OPTS = (
    "--add-exports=java.base/sun.net.util=ALL-UNNAMED "
    "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED "
    "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED "
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
class FixtureResult:
    dataset_path: Path
    stats: dict[str, int]
    metadata: dict[str, object]


def _download_file(url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        return
    tmp = destination.with_suffix(destination.suffix + ".tmp")
    curl_bin = shutil.which("curl")
    if curl_bin is not None:
        result = subprocess.run(
            [curl_bin, "-L", "--fail", "--silent", "--show-error", url, "-o", str(tmp)],
            capture_output=True,
            text=True,
            timeout=3600,
        )
        if result.returncode != 0:
            raise RuntimeError(
                result.stderr.strip()
                or result.stdout.strip()
                or f"failed to download {url}"
            )
    else:
        with urllib.request.urlopen(url) as response, tmp.open("wb") as fh:
            shutil.copyfileobj(response, fh)
    tmp.replace(destination)


def _extract_tgz(archive: Path, destination: Path) -> None:
    # The marker avoids untarring the same archive on every benchmark run.
    marker = destination / ".extracted"
    if marker.exists():
        return
    if destination.exists():
        shutil.rmtree(destination)
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r:gz") as tar:
        tar.extractall(destination)
    marker.write_text("ok\n", encoding="utf-8")


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _run_cmd(
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
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.strip() or result.stdout.strip() or f"command failed: {cmd}"
        )
    return result


def _wait_for_http_port(port: int, timeout: int = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(1)
    raise RuntimeError(f"timeout waiting for Flink REST on 127.0.0.1:{port}")


def _poll_flink_job(rest_port: int, job_id: str, timeout: int = 600) -> None:
    deadline = time.time() + timeout
    job_url = f"http://127.0.0.1:{rest_port}/jobs/{job_id}"
    while time.time() < deadline:
        with urllib.request.urlopen(job_url, timeout=10) as response:
            payload = json.loads(response.read().decode("utf-8"))
        state = payload.get("state", "")
        if state == "FINISHED":
            return
        if state in {"FAILED", "CANCELED", "CANCELING"}:
            raise RuntimeError(f"Flink job {job_id} ended in state {state}")
        time.sleep(1)
    raise RuntimeError(f"timeout waiting for Flink job {job_id} to finish")


def _rewrite_yaml_value(lines: list[str], key: str, value: str) -> list[str]:
    prefix = f"{key}:"
    for idx, line in enumerate(lines):
        if line.startswith(prefix):
            lines[idx] = f"{prefix} {value}"
            return lines
    lines.append(f"{prefix} {value}")
    return lines


def _prepare_cached_toolchain(cache_root: Path) -> tuple[Path, Path]:
    # Cache the Flink distribution, connector jar and nexmark-flink package once so subsequent
    # benchmark runs only pay the download/bootstrap cost when the cache is empty.
    cache_root.mkdir(parents=True, exist_ok=True)
    nexmark_tgz = cache_root / "nexmark-flink-v0.2.0.tgz"
    kafka_jar = cache_root / f"flink-sql-connector-kafka_2.11-{FLINK_VERSION}.jar"

    _download_file(NEXMARK_FLINK_URL, nexmark_tgz)
    _download_file(FLINK_KAFKA_CONNECTOR_URL, kafka_jar)

    flink_home = cache_root / "flink-dist" / f"flink-{FLINK_VERSION}"
    if not flink_home.exists():
        docker_bin = shutil.which("docker")
        if docker_bin is not None:
            _run_cmd([docker_bin, "pull", FLINK_DOCKER_IMAGE], timeout=3600)
            container_name = f"datalayers-nexmark-flink-cache-{int(time.time())}"
            _run_cmd(
                [docker_bin, "create", "--name", container_name, FLINK_DOCKER_IMAGE],
                timeout=120,
            )
            try:
                target_root = cache_root / "flink-dist"
                target_root.mkdir(parents=True, exist_ok=True)
                _run_cmd(
                    [
                        docker_bin,
                        "cp",
                        f"{container_name}:/opt/flink",
                        str(target_root),
                    ],
                    timeout=600,
                )
                copied = target_root / "flink"
                copied.rename(flink_home)
            finally:
                subprocess.run(
                    [docker_bin, "rm", "-f", container_name],
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
        else:
            flink_tgz = cache_root / f"flink-{FLINK_VERSION}.tgz"
            _download_file(FLINK_DIST_URL, flink_tgz)
            _extract_tgz(flink_tgz, cache_root / "flink-dist")
    _extract_tgz(nexmark_tgz, cache_root / "nexmark-dist")

    nexmark_home = cache_root / "nexmark-dist" / "nexmark-flink"
    connector_dest = flink_home / "lib" / kafka_jar.name
    if not connector_dest.exists():
        shutil.copy2(kafka_jar, connector_dest)
    nexmark_jar = nexmark_home / "lib" / "nexmark-flink-0.2-SNAPSHOT.jar"
    nexmark_jar_dest = flink_home / "lib" / nexmark_jar.name
    if not nexmark_jar_dest.exists():
        shutil.copy2(nexmark_jar, nexmark_jar_dest)
    return flink_home, nexmark_home


def prepare_flink_toolchain(cache_root: Path) -> tuple[Path, Path]:
    return _prepare_cached_toolchain(cache_root)


def _render_sql(template_path: Path, variables: dict[str, str]) -> str:
    text = template_path.read_text(encoding="utf-8")
    for key, value in variables.items():
        text = text.replace(f"${{{key}}}", value)
    return text


def _ensure_topic(
    run_cmd: Callable[..., subprocess.CompletedProcess[str]],
    container: str,
    topic: str,
    partitions: int,
) -> None:
    # Recreate the topic to guarantee a clean input stream for fixture generation.
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


def _produce_file_to_topic(
    run_cmd: Callable[..., subprocess.CompletedProcess[str]],
    container: str,
    topic: str,
    dataset_path: Path,
) -> None:
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
                    f"--topic {topic}"
                ),
            ],
            stdin=fh,
            capture_output=True,
            timeout=3600,
        )
    if result.returncode != 0:
        raise RuntimeError(
            result.stderr.decode("utf-8", errors="ignore")
            or result.stdout.decode("utf-8", errors="ignore")
            or f"failed to produce records to topic {topic}"
        )


def _extract_bid_fixture(
    combined_events_path: Path,
    bid_dataset_path: Path,
    bid_rows: int,
) -> dict[str, int]:
    # Keep only bid events and derive per-query expected row counts used by the benchmark runners.
    stats = {
        "total_rows": 0,
        "q2_expected_rows": 0,
        "q14_expected_rows": 0,
        "q21_expected_rows": 0,
    }
    with (
        combined_events_path.open("r", encoding="utf-8") as src,
        bid_dataset_path.open("w", encoding="utf-8") as dst,
    ):
        for line in src:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            if record.get("event_type") != 2:
                continue
            bid = record.get("bid") or {}
            flattened = {
                "ts": bid["dateTime"],
                "auction": bid["auction"],
                "bidder": bid["bidder"],
                "price": bid["price"],
                "channel": bid["channel"],
                "url": bid["url"],
                "extra": bid["extra"],
            }
            dst.write(json.dumps(flattened, separators=(",", ":")) + "\n")
            stats["total_rows"] += 1
            if flattened["auction"] in {1007, 1020, 2001, 2019, 2087}:
                stats["q2_expected_rows"] += 1
            converted_price = float(flattened["price"]) * 0.908
            if 1_000_000 < converted_price < 50_000_000:
                stats["q14_expected_rows"] += 1
            channel = str(flattened["channel"]).lower()
            if channel in {
                "apple",
                "google",
                "facebook",
                "baidu",
            } or "channel_id=" in str(flattened["url"]):
                stats["q21_expected_rows"] += 1
            if stats["total_rows"] >= bid_rows:
                break
    if stats["total_rows"] < bid_rows:
        raise RuntimeError(
            f"official Nexmark generator produced only {stats['total_rows']} bid rows, need {bid_rows}"
        )
    return stats


def prepare_official_bid_fixture(
    *,
    workdir: Path,
    kafka_container: str,
    kafka_brokers: str,
    rows: int,
    partitions: int,
    log: Callable[[str], None],
) -> FixtureResult:
    # Generate one canonical bid fixture that every engine runner can replay independently.
    cache_root = Path.home() / ".cache" / "datalayers-nexmark"
    flink_home, nexmark_home = _prepare_cached_toolchain(cache_root)

    runtime_root = workdir / "nexmark_fixture_runtime"
    if runtime_root.exists():
        shutil.rmtree(runtime_root)
    runtime_root.mkdir(parents=True, exist_ok=True)
    runtime_flink = runtime_root / "flink"
    runtime_nexmark = runtime_root / "nexmark-flink"
    shutil.copytree(flink_home, runtime_flink, symlinks=True)
    shutil.copytree(nexmark_home, runtime_nexmark, symlinks=True)

    rest_port = _find_free_port()
    jobmanager_port = _find_free_port()
    blob_port = _find_free_port()
    flink_conf_path = runtime_flink / "conf" / "flink-conf.yaml"
    flink_lines = flink_conf_path.read_text(encoding="utf-8").splitlines()
    flink_lines = _rewrite_yaml_value(flink_lines, "rest.port", str(rest_port))
    flink_lines = _rewrite_yaml_value(flink_lines, "rest.address", "localhost")
    flink_lines = _rewrite_yaml_value(
        flink_lines, "jobmanager.rpc.address", "localhost"
    )
    flink_lines = _rewrite_yaml_value(
        flink_lines, "jobmanager.rpc.port", str(jobmanager_port)
    )
    flink_lines = _rewrite_yaml_value(flink_lines, "blob.server.port", str(blob_port))
    flink_lines = _rewrite_yaml_value(flink_lines, "parallelism.default", "1")
    flink_lines = _rewrite_yaml_value(flink_lines, "taskmanager.numberOfTaskSlots", "1")
    flink_lines = _rewrite_yaml_value(
        flink_lines, "env.java.opts", FLINK_JAVA_MODULE_OPTS
    )
    flink_lines = _rewrite_yaml_value(
        flink_lines, "env.java.opts.all", FLINK_JAVA_MODULE_OPTS
    )
    flink_lines = _rewrite_yaml_value(
        flink_lines, "env.java.opts.jobmanager", FLINK_JAVA_MODULE_OPTS
    )
    flink_lines = _rewrite_yaml_value(
        flink_lines, "env.java.opts.taskmanager", FLINK_JAVA_MODULE_OPTS
    )
    flink_conf_path.write_text("\n".join(flink_lines) + "\n", encoding="utf-8")
    (runtime_flink / "conf" / "workers").write_text("localhost\n", encoding="utf-8")

    generated_events = max(rows + 1, int(math.ceil(rows / OFFICIAL_BID_FRACTION)) + 512)
    variables = {
        "TPS": str(max(50_000, min(500_000, generated_events))),
        "EVENTS_NUM": str(generated_events),
        "PERSON_PROPORTION": str(OFFICIAL_EVENT_RATIOS[0]),
        "AUCTION_PROPORTION": str(OFFICIAL_EVENT_RATIOS[1]),
        "BID_PROPORTION": str(OFFICIAL_EVENT_RATIOS[2]),
        "BOOTSTRAP_SERVERS": kafka_brokers,
    }

    ddl_gen = _render_sql(runtime_nexmark / "queries" / "ddl_gen.sql", variables)
    ddl_kafka = _render_sql(runtime_nexmark / "queries" / "ddl_kafka.sql", variables)
    ddl_kafka = ddl_kafka.replace(
        "'topic' = 'nexmark'", f"'topic' = '{OFFICIAL_EVENT_TOPIC}'"
    )
    ddl_kafka = ddl_kafka.replace(
        "'properties.group.id' = 'nexmark'",
        "'properties.group.id' = 'nexmark-generator'",
    )
    insert_kafka = (runtime_nexmark / "queries" / "insert_kafka.sql").read_text(
        encoding="utf-8"
    )
    sql_input = "\n".join(
        [
            "SET 'execution.runtime-mode' = 'streaming';",
            ddl_gen,
            ddl_kafka,
            insert_kafka,
        ]
    )
    sql_file = runtime_root / "insert_kafka.sql"
    sql_file.write_text(sql_input, encoding="utf-8")

    log(
        "Generating official Nexmark events with Flink SQL "
        f"(events={generated_events}, target_bid_rows={rows})"
    )
    _ensure_topic(_run_cmd, kafka_container, OFFICIAL_EVENT_TOPIC, partitions)

    env = dict(
        os.environ,
        FLINK_HOME=str(runtime_flink),
        JAVA_TOOL_OPTIONS=FLINK_JAVA_MODULE_OPTS,
        _JAVA_OPTIONS=FLINK_JAVA_MODULE_OPTS,
    )
    _run_cmd(
        [str(runtime_flink / "bin" / "start-cluster.sh")],
        cwd=runtime_flink,
        env=env,
        timeout=120,
    )
    try:
        _wait_for_http_port(rest_port, timeout=60)
        result = _run_cmd(
            [
                str(runtime_flink / "bin" / "sql-client.sh"),
                "embedded",
                "-f",
                str(sql_file),
            ],
            cwd=runtime_root,
            env=env,
            timeout=1800,
        )
        job_id = ""
        for line in result.stdout.splitlines():
            if "Job ID:" in line:
                job_id = line.split("Job ID:", 1)[1].strip()
        if not job_id:
            raise RuntimeError(
                f"failed to parse Flink job id from sql client output:\n{result.stdout}"
            )
        _poll_flink_job(rest_port, job_id, timeout=1800)
    finally:
        _run_cmd(
            [str(runtime_flink / "bin" / "stop-cluster.sh")],
            cwd=runtime_flink,
            env=env,
            timeout=120,
        )

    combined_events_path = workdir / "nexmark_fixture_events.jsonl"
    consumer_cmd = [
        "docker",
        "exec",
        kafka_container,
        "bash",
        "-lc",
        (
            "kafka-console-consumer --bootstrap-server 127.0.0.1:9092 "
            f"--topic {OFFICIAL_EVENT_TOPIC} --from-beginning --timeout-ms 5000"
        ),
    ]
    dataset_path = workdir / "nexmark_fixture_bid.jsonl"
    stats: dict[str, int] | None = None
    last_consumer_output = ""
    for _ in range(12):
        consumer = subprocess.run(
            consumer_cmd,
            capture_output=True,
            text=True,
            timeout=600,
        )
        if consumer.returncode not in (0, 137):
            raise RuntimeError(
                consumer.stderr.strip()
                or consumer.stdout.strip()
                or "failed to consume generated Nexmark events"
            )
        combined_events_path.write_text(consumer.stdout, encoding="utf-8")
        last_consumer_output = consumer.stdout
        try:
            stats = _extract_bid_fixture(combined_events_path, dataset_path, rows)
            break
        except RuntimeError:
            time.sleep(2)
    if stats is None:
        raise RuntimeError(
            "official Nexmark generator did not yield enough bid rows after retries; "
            f"last batch size={len(last_consumer_output.splitlines())}"
        )
    log(
        "Prepared official Nexmark bid fixture: "
        f"rows={stats['total_rows']} q2={stats['q2_expected_rows']} "
        f"q14={stats['q14_expected_rows']} q21={stats['q21_expected_rows']}"
    )
    return FixtureResult(
        dataset_path=dataset_path,
        stats=stats,
        metadata={
            "fixture": "official",
            "combined_topic": OFFICIAL_EVENT_TOPIC,
            "generated_events": generated_events,
            "flink_version": FLINK_VERSION,
        },
    )


def write_keyed_bid_dataset(
    dataset_path: Path, output_path: Path, partitions: int
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with (
        dataset_path.open("r", encoding="utf-8") as src,
        output_path.open("w", encoding="utf-8") as dst,
    ):
        for index, line in enumerate(src):
            dst.write(f"p{index % partitions}\t{line}")


def scan_bid_dataset(dataset_path: Path) -> dict[str, int]:
    stats = {
        "total_rows": 0,
        "q2_expected_rows": 0,
        "q14_expected_rows": 0,
        "q21_expected_rows": 0,
    }
    with dataset_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            payload = line.split("\t", 1)[1] if "\t" in line else line
            record = json.loads(payload)
            stats["total_rows"] += 1
            if record["auction"] in {1007, 1020, 2001, 2019, 2087}:
                stats["q2_expected_rows"] += 1
            converted_price = float(record["price"]) * 0.908
            if 1_000_000 < converted_price < 50_000_000:
                stats["q14_expected_rows"] += 1
            channel = str(record["channel"]).lower()
            if channel in {
                "apple",
                "google",
                "facebook",
                "baidu",
            } or "channel_id=" in str(record["url"]):
                stats["q21_expected_rows"] += 1
    return stats


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare or inspect the shared Nexmark bid dataset"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser(
        "prepare",
        help="Generate a stable keyed Nexmark bid dataset from the official nexmark-flink generator",
    )
    prepare.add_argument(
        "--workdir",
        default=".nexmark-datagen",
        help="Temporary work directory used while running Flink datagen.",
    )
    prepare.add_argument(
        "--output",
        required=True,
        help="Output path of the final keyed JSONL dataset.",
    )
    prepare.add_argument(
        "--stats-output",
        required=True,
        help="Output path of the dataset stats JSON sidecar.",
    )
    prepare.add_argument(
        "--kafka-container",
        required=True,
        help="Kafka container name used during official Nexmark event generation.",
    )
    prepare.add_argument(
        "--kafka-brokers",
        default="127.0.0.1:9092",
        help="Kafka bootstrap servers visible to the local Flink datagen runtime.",
    )
    prepare.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_TARGET_BID_ROWS,
        help="Target number of bid rows to keep in the generated dataset.",
    )
    prepare.add_argument(
        "--partitions",
        type=int,
        default=4,
        help="Number of logical keys used when writing the keyed JSONL output.",
    )

    scan = subparsers.add_parser(
        "scan",
        help="Scan an existing keyed or plain bid dataset and print dataset stats as JSON",
    )
    scan.add_argument("dataset", help="Path to the keyed or plain bid dataset.")
    return parser.parse_args()


def _cli_log(message: str) -> None:
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
    print(f"[{now} UTC] {message}", flush=True)


def main() -> int:
    args = _parse_args()
    if args.command == "scan":
        stats = scan_bid_dataset(Path(args.dataset).resolve())
        print(json.dumps(stats, indent=2))
        return 0

    workdir = Path(args.workdir).resolve()
    output_path = Path(args.output).resolve()
    stats_output = Path(args.stats_output).resolve()
    result = prepare_official_bid_fixture(
        workdir=workdir,
        kafka_container=args.kafka_container,
        kafka_brokers=args.kafka_brokers,
        rows=args.rows,
        partitions=args.partitions,
        log=_cli_log,
    )
    write_keyed_bid_dataset(result.dataset_path, output_path, args.partitions)
    stats_output.parent.mkdir(parents=True, exist_ok=True)
    stats_output.write_text(json.dumps(result.stats, indent=2) + "\n", encoding="utf-8")
    _cli_log(f"Wrote keyed dataset to {output_path}")
    _cli_log(f"Wrote dataset stats to {stats_output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
