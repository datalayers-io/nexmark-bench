# Nexmark Bench

这个目录包含 Datalayers、RisingWave、Flink、Arroyo 的本地 Nexmark benchmark harness。

## 设计

- benchmark 消费一个预先生成好的 keyed bid JSONL dataset。
- dataset 由 [datagen.sh](/home/nsc/nexmark-bench/datagen.sh:1) 调用 [nexmark_fixture.py](/home/nsc/nexmark-bench/nexmark_fixture.py:1) 生成。
- `nexmark_fixture.py` 使用官方 `nexmark-flink` datagen 生成 combined Nexmark events，然后抽取 `Bid` 事件。
- 抽取出的 bid 被扁平化，写入稳定命名的 keyed JSONL 文件，默认是 [nexmark_bid.keyed.jsonl](/home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl)。
- datagen 同时写出同名关联的 stats JSON；默认 stats 文件是 [nexmark_bid.keyed.stats.json](/home/nsc/nexmark-bench/nexmark_bid.keyed.stats.json)。
- stats JSON 记录该 dataset 生成时使用的 `partitions`，各 runner 直接从这里读取 Kafka topic 分区数。
- benchmark 脚本不在运行时临时生成 fixture。它们只接收 `--dataset`，默认指向仓库根目录下的稳定 dataset 文件。
- 默认 query 列表是 `q0,q1,q2,q14,q21,q22,q16,q17`。
- 所有 runner 共用 `input_rows / replay_sec` 作为 throughput 口径。

## 数据生成

先生成可复用 dataset：

```bash
bash ./datagen.sh
```

默认输出：

- dataset: [nexmark_bid.keyed.jsonl](/home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl)
- stats: [nexmark_bid.keyed.stats.json](/home/nsc/nexmark-bench/nexmark_bid.keyed.stats.json)

可选参数：

- `--dataset PATH`
  指定输出 keyed JSONL 文件路径。
- `--rows N`
  目标 bid 行数，默认 `10000000`（1000 万行）。`nexmark-flink` 先生成足够多的 combined events，再从中截取前 `N` 条 bid。
- `--partitions N`
  写 keyed dataset 时使用的逻辑 key 数量。默认 `1`。该值也会写入同名 stats JSON，后续 runner 用它重建 Kafka topic。
- `--bench-root DIR`
  datagen 临时工作目录根路径。
- `--no-cleanup`
  保留 datagen 临时 Kafka 容器和工作目录。

## Benchmark 入口

- [benches/datalayers.sh](/home/nsc/nexmark-bench/benches/datalayers.sh:1)
- [benches/risingwave.sh](/home/nsc/nexmark-bench/benches/risingwave.sh:1)
- [benches/flink.sh](/home/nsc/nexmark-bench/benches/flink.sh:1)
- [benches/arroyo.sh](/home/nsc/nexmark-bench/benches/arroyo.sh:1)

核心实现：

- [nexmark_fixture.py](/home/nsc/nexmark-bench/nexmark_fixture.py:1)
- [runners/datalayers.py](/home/nsc/nexmark-bench/runners/datalayers.py:1)
- [runners/risingwave.py](/home/nsc/nexmark-bench/runners/risingwave.py:1)
- [runners/flink.py](/home/nsc/nexmark-bench/runners/flink.py:1)
- [runners/arroyo.py](/home/nsc/nexmark-bench/runners/arroyo.py:1)

## 推荐用法

先生成 dataset：

```bash
bash ./datagen.sh
```

跑 Datalayers：

```bash
bash ./benches/datalayers.sh \
  --dataset /home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl \
  --queries q0,q1,q2,q14,q21,q22,q16,q17 \
  --sink table \
  --host 127.0.0.1 \
  --port 8361
```

如果 Datalayers 运行在默认地址 `127.0.0.1:8361`，可以省略 `--host/--port`。

跑 RisingWave：

```bash
bash ./benches/risingwave.sh \
  --dataset /home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl \
  --queries q0,q1,q2,q14,q21,q22,q16,q17 \
  --parallelism 1 \
  --sink table
```

跑 Flink：

```bash
bash ./benches/flink.sh \
  --dataset /home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl \
  --queries q0,q1,q2,q14,q21,q22,q16,q17
```

跑 Arroyo：

```bash
bash ./benches/arroyo.sh \
  --dataset /home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl \
  --queries q0,q1,q2,q14,q21,q22,q16,q17 \
  --sink blackhole
```

## 脚本参数

### `benches/datalayers.sh`

连接一个已启动的 Datalayers HTTP SQL endpoint。脚本负责启动 Kafka、preload dataset、执行 benchmark、写 report，并在正式开始前清理目标 `nexmark_*` benchmark database 的残留对象。

主要参数：

- `-h, --host HOST`
  已启动 Datalayers 的 HTTP host。默认 `127.0.0.1`。
- `-P, --port HTTP_PORT`
  已启动 Datalayers 的 HTTP port。默认 `8361`。
- `--dataset PATH`
  keyed JSONL dataset 路径。runner 自动读取同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。Datalayers 暂不支持 q16/q17，runner 会自动跳过。
- `--sink table|blackhole`
  `table` 创建表 sink，通过 sink table 的 `COUNT(*) >= expected_rows` 且 Kafka consumer group lag 归零共同判定完成。
  `blackhole` 创建 blackhole sink，通过 Kafka consumer group lag 判定完成。
  默认: `table`。
- `--timeout SEC`
  每个 query 的完成等待超时时间，单位秒。默认 `600`。
- `--kafka-container KAFKA_CONTAINER`
  复用已有的 Kafka 容器。传入一个正在运行的 Docker 容器名。runner 会检查 topic 中已有 message 数量是否与 dataset 行数匹配，若匹配则跳过 preload。未传时自动创建并启动新的 Kafka 容器。
- `--profile`
  使用 perf 采集 Datalayers 进程的 CPU profile，benchmark 结束后生成 `flamegraph.svg`。要求 Datalayers 以 `RUSTFLAGS="-C force-frame-pointers"` 编译。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka 和 benchmark 创建的对象；未传时自动 cleanup。无论是否传这个参数，runner 在正式开始前都会先做一轮预清理。

### `benches/risingwave.sh`

脚本启动 Kafka 和 RisingWave standalone 容器，通过 Docker network 连接，然后由 Python runner 执行 query 并生成 report。

主要参数：

- `--dataset PATH`
  keyed JSONL dataset 路径。runner 自动读取同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。
- `--parallelism N`
  RisingWave `single_node` 的并行度。默认 `1`。
- `--sink table|blackhole`
  `table` 创建 materialized view，通过 `COUNT(*) >= expected_rows` 判定完成；runner 额外尝试用 Kafka source group 作为兜底信号，如果不可用则回退到稳定的 `COUNT(*)`。
  `blackhole` 创建 blackhole sink，通过 RisingWave metrics 中该 sink 的 `stream_sink_input_row_count >= expected_rows` 判定完成。
  默认: `table`。
- `--timeout SEC`
  每个 query 的完成等待超时时间，单位秒。默认 `600`。
- `--kafka-container KAFKA_CONTAINER`
  复用已有的 Kafka 容器。runner 会检查 topic 中已有 message 数量是否与 dataset 行数匹配，若匹配则跳过 preload。未传时自动创建并启动新的 Kafka 容器。
- `--image IMAGE`
  覆盖默认 RisingWave 镜像。默认 `risingwavelabs/risingwave:v2.8.3`。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka、RisingWave 容器和 benchmark 创建的 source/MV/sink；未传时自动 cleanup。

### `benches/flink.sh`

脚本启动 Kafka 容器，Python runner 在本地启动 Flink standalone cluster 并提交 SQL job。Flink toolchain 自动下载缓存。

主要参数：

- `--dataset PATH`
  keyed JSONL dataset 路径。runner 自动读取同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。
- `--parallelism N`
  Flink 任务并行度。默认 `1`。
- `--timeout SEC`
  每个 query 的完成等待超时时间，单位秒。默认 `600`。
- `--kafka-container KAFKA_CONTAINER`
  复用已有的 Kafka 容器。runner 会检查 topic 中已有 message 数量是否与 dataset 行数匹配，若匹配则跳过 preload。未传时自动创建并启动新的 Kafka 容器。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka 容器；未传时自动 cleanup。

Flink runner 固定使用 blackhole sink。完成判定是 Kafka consumer group lag 到 0 后主动 cancel job。

### `benches/arroyo.sh`

脚本启动本地 Kafka 容器和一个单容器 Arroyo 实例，在 Docker network 内互通，然后通过 Arroyo REST API 提交 pipeline。

主要参数：

- `--dataset PATH`
  keyed JSONL dataset 路径。runner 自动读取同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。
- `--parallelism N`
  Arroyo pipeline 并行度。默认 `1`。
- `--sink table|blackhole`
  `table` 写入 in-memory 表，数据保留在内存中。
  `blackhole` 写入 blackhole sink，数据直接丢弃。
  默认: `blackhole`。
  注意：q16 和 q17 是聚合查询，内部强制使用 `table` sink（`updating=True`），不受此参数影响。
- `--timeout SEC`
  每个 query 的完成等待超时时间，单位秒。默认 `600`。
- `--kafka-container KAFKA_CONTAINER`
  复用已有的 Kafka 容器。runner 会检查 topic 中已有 message 数量是否与 dataset 行数匹配，若匹配则跳过 preload。未传时自动创建并启动新的 Kafka 容器。
- `--image IMAGE`
  覆盖默认 Arroyo 镜像。默认 `ghcr.io/arroyosystems/arroyo:0.14.1`。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka、Arroyo 容器、network 和 benchmark 创建的 pipeline；未传时自动 cleanup。

Arroyo 的完成判定基于每个 query 显式指定的 Kafka source consumer group。由于 Arroyo 对 Kafka group 提交的是"最后已处理 offset"，完成时在 `kafka-consumer-groups` 里可能表现为每个非空分区残留 `lag=1`，runner 已按这个语义做了兜底判定。

## 指标口径

四个 runner 都统计这些核心指标：

- `input_rows`
  dataset 中的总输入行数。
- `expected_rows`
  同一份 bid 输入在该 query 语义下的理论输出行数。
- `inserted_rows`
  仅 Datalayers 和 RisingWave 的 `table` 模式下记录实际观察到的 sink/table 行数。
  对 blackhole 场景，runner 保留这个字段但值等于 `expected_rows`。
- `replay_sec`
  从创建 source/query 并开始 replay，到完成判定满足为止的耗时。
- `throughput_rps`
  `input_rows / replay_sec`。
- `avg_cpu_percent`
  replay 窗口内的平均 CPU。
- `avg_mem_gib`
  replay 窗口内的平均 RSS，单位 GiB。

  Datalayers runner 会根据 `benches/datalayers.sh` 连接的 HTTP 端口自动探测本机监听 PID 并采样；探测失败时这两个字段回退为 `0`。
- `kafka_preload_sec`
  本轮 query 输入 preload 到 Kafka 的耗时，不计入 `throughput_rps` 分母。

Flink 和 Arroyo 的结果不包含 `inserted_rows`，因为完成判定基于 Kafka consumer group lag 而非 sink 行数轮询。两者结果保留 `expected_rows` 和 `state`。

dataset stats 中预计算了以下字段：

- `total_rows`
- `partitions`
- `q2_expected_rows`
- `q14_expected_rows`
- `q21_expected_rows`
- `q16_expected_rows`
- `q17_expected_rows`

四个 runner 直接读取与 `--dataset` 同名关联的 stats JSON，不在 benchmark 启动时重新全量扫描 dataset。命名规则是将 dataset 文件名末尾的 `.jsonl` 替换为 `.stats.json`；例如 `foo.keyed.jsonl` 对应 `foo.keyed.stats.json`。

## 结果文件

每次 benchmark 的输出写入临时根目录。

Datalayers：

- `<bench_root>/datalayers/report.md`
- `<bench_root>/datalayers/report.json`

RisingWave：

- `<bench_root>/risingwave/report.md`
- `<bench_root>/risingwave/report.json`
- `<bench_root>/risingwave/<query>_samples.csv`

Flink：

- `<bench_root>/flink/report.md`
- `<bench_root>/flink/report.json`
- `<bench_root>/flink/<query>_samples.csv`

Arroyo：

- `<bench_root>/arroyo/report.md`
- `<bench_root>/arroyo/report.json`
- `<bench_root>/arroyo/<query>_samples.csv`

所有 `report.json` 使用同一组顶层字段：

- `generated_at`
- `engine`
- `mode`
- `fixture`
- `fixture_metadata`
- `dataset_stats`
- `sink_mode`
- `results`
