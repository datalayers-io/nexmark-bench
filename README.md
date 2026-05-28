# Nexmark Bench

这个目录包含 Datalayers、RisingWave、Flink、Arroyo 的本地 Nexmark benchmark harness。

## 当前设计

- benchmark 只消费一个已经预先生成好的 keyed bid JSONL dataset。
- dataset 由 [datagen.sh](/home/nsc/nexmark-bench/datagen.sh:1) 调用 [nexmark_fixture.py](/home/nsc/nexmark-bench/nexmark_fixture.py:1) 生成。
- `nexmark_fixture.py` 会使用官方 `nexmark-flink` datagen 生成 combined Nexmark events，然后抽取 `Bid` 事件。
- 抽取出的 bid 会被扁平化，并写入稳定命名的 keyed JSONL 文件，默认是 [nexmark_bid.keyed.jsonl](/home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl)。
- datagen 会同时写出与 dataset 同名关联的 stats JSON；默认 stats 文件是 [nexmark_bid.keyed.stats.json](/home/nsc/nexmark-bench/nexmark_bid.keyed.stats.json)。
- stats JSON 会记录该 dataset 生成时使用的 `partitions`，各 runner 会直接从这里读取 Kafka topic 分区数。
- benchmark 脚本不再在运行时临时生成 fixture。它们只接收 `--dataset`，默认指向仓库根目录下的稳定 dataset 文件。
- 当前默认 query 是 `q0,q1,q2,q14,q21,q22,q16,q17`。
- 当前 throughput 统一定义为 `input_rows / replay_sec`。

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
  目标 bid 行数，默认 `1000000`（100 万行）。`nexmark-flink` 先生成足够多的 combined events，再从中截取前 `N` 条 bid。
- `--partitions N`
  写 keyed dataset 时使用的逻辑 key 数量。默认 `4`。该值也会写入同名 stats JSON，后续 runner 会用它来重建 Kafka topic。
- `--bench-root DIR`
  datagen 临时工作目录根路径。
- `--no-cleanup`
  保留 datagen 临时 Kafka 容器和工作目录，便于排查。

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

如果 Datalayers 就运行在默认地址 `127.0.0.1:8361`，可以省略 `--host/--port`：

```bash
bash ./benches/datalayers.sh \
  --dataset /home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl \
  --queries q0,q1,q2,q14,q21,q22,q16,q17 \
  --sink table
```

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
  --queries q0,q1,q2,q14,q21,q22,q16,q17
```

## 脚本参数

### `benches/datalayers.sh`

当前只支持连接一个已经启动好的 Datalayers HTTP SQL endpoint。脚本会负责启动 Kafka、preload dataset、执行 benchmark、写 report，并在正式开始前先清理目标 `nexmark_*` benchmark database 的残留对象。

主要参数：

- `-h, --host HOST`
  已启动 Datalayers 的 HTTP host。默认 `127.0.0.1`。
- `-P, --port HTTP_PORT`
  已启动 Datalayers 的 HTTP port。默认 `8361`。
- `--dataset PATH`
  keyed JSONL dataset 路径。默认指向仓库根目录下的稳定 dataset。runner 会自动读取与其同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。
- `--sink table|blackhole`
  `table` 通过 sink table 的 `COUNT(*) >= expected_rows` 且 Kafka consumer group lag 归零共同判定完成；
  `blackhole` 通过 Kafka consumer group lag 判定完成。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka 和 benchmark 创建的对象；未传时会自动 cleanup。无论是否传这个参数，runner 在正式开始前都会先做一轮预清理；cleanup 顺序会先 drop pipeline，再 drop source/sink/table，最后 drop database。

### `benches/risingwave.sh`

主要参数：

- `--dataset PATH`
  keyed JSONL dataset 路径。默认指向仓库根目录下的稳定 dataset。runner 会自动读取与其同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。
- `--parallelism N`
  RisingWave `single_node` 的并行度。
- `--sink table|blackhole`
  `table` 创建 materialized view，并通过 `COUNT(*) >= expected_rows` 判定完成；runner 会额外尝试用 Kafka source group 作为兜底信号，如果该信号不可用，则回退到稳定的 `COUNT(*)`。
  `blackhole` 创建 blackhole sink，并通过 RisingWave metrics 中该 sink 的 `stream_sink_input_row_count >= expected_rows` 判定完成。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka、RisingWave 容器和 benchmark 创建的 source/MV/sink；未传时会自动 cleanup。
- `--image IMAGE`
  覆盖默认 RisingWave 镜像。

### `benches/flink.sh`

主要参数：

- `--dataset PATH`
  keyed JSONL dataset 路径。默认指向仓库根目录下的稳定 dataset。runner 会自动读取与其同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。
- `--parallelism N`
  Flink 任务并行度。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka 容器；未传时会自动 cleanup。

Flink runner 固定使用 blackhole sink。完成判定是 Kafka consumer group lag 到 0 后主动 cancel job。

### `benches/arroyo.sh`

脚本会启动本地 Kafka 容器和一个单容器 Arroyo 实例，然后通过 Arroyo REST API 提交 pipeline。

主要参数：

- `--dataset PATH`
  keyed JSONL dataset 路径。默认指向仓库根目录下的稳定 dataset。runner 会自动读取与其同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka、Arroyo 容器、network 和 benchmark 创建的 pipeline；未传时会自动 cleanup。
- `--image IMAGE`
  覆盖默认 Arroyo 镜像。当前默认值是 `ghcr.io/arroyosystems/arroyo:0.14.1`。

Arroyo runner 当前支持 `--parallelism`，会直接透传到 `/pipelines` API 的 `parallelism` 字段。sink 仍固定为 blackhole。原因是 Arroyo 目前虽然支持 `CREATE TABLE ... WITH (...)`、`INSERT INTO ... SELECT ...` 和 preview pipeline，但没有像 Datalayers/RisingWave 那样适合 benchmark 轮询 `SELECT COUNT(*)` 的内建结果表；preview sink 会把完整输出流推给 API，更适合调试，不适合 100 万行 replay benchmark。

Arroyo 的完成判定仍基于每个 query 显式指定的 Kafka source consumer group；由于 Arroyo 对 Kafka group 提交的是“最后已处理 offset”，完成时在 `kafka-consumer-groups` 里可能表现为每个非空分区残留 `lag=1`，runner 已按这个语义做了兜底判定。

## 指标口径

四个 runner 都会统计这些核心指标：

- `input_rows`
  dataset 中的总输入行数。
- `expected_rows`
  同一份 bid 输入在该 query 语义下的理论输出行数。
- `inserted_rows`
  只出现在 Datalayers 和 RisingWave 的结果里。
  对 `table` 模式，表示实际观察到的 sink/table 行数。
  对 blackhole 场景，runner 仍然会保留这个字段，但值等于 `expected_rows`。
- `replay_sec`
  从创建 source/query 并开始 replay，到完成判定满足为止的耗时。
- `throughput_rps`
  `input_rows / replay_sec`。
- `avg_cpu_percent`
  replay 窗口内的平均 CPU。
- `avg_mem_gib`
  replay 窗口内的平均 RSS，单位 GiB。

当前 Datalayers runner 会尝试根据 `benches/datalayers.sh` 连接的 HTTP 端口自动探测本机监听 PID，并对该 PID 做采样；如果探测失败，这两个字段会回退为 `0`。
- `kafka_preload_sec`
  本轮 query 输入 preload 到 Kafka 的耗时，不计入 `throughput_rps` 分母。

Flink 的结果没有 `inserted_rows`，因为当前完成判定是 source 消费完所有输入后主动 cancel job，结果里保留的是 `expected_rows` 和 `state`。

Arroyo 的结果目前也没有 `inserted_rows`，因为当前只支持 blackhole sink；结果里保留的是 `expected_rows` 和 pipeline 在完成判定时观察到的 `state`。

dataset stats 里的这些字段是提前从 dataset 扫描得到的：

- `total_rows`
- `partitions`
- `q2_expected_rows`
- `q14_expected_rows`
- `q21_expected_rows`
- `q16_expected_rows`
- `q17_expected_rows`

四个 runner 会直接读取与 `--dataset` 同名关联的 stats JSON，而不是在 benchmark 启动时重新全量扫描 dataset。当前命名规则是把 dataset 文件名末尾的 `.jsonl` 替换成 `.stats.json`；例如 `foo.keyed.jsonl` 对应 `foo.keyed.stats.json`。

## 结果文件

每次 benchmark 的输出都会写到一个临时根目录。

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

三个 `report.json` 现在都使用同一组顶层字段：

- `generated_at`
- `engine`
- `mode`
- `fixture`
- `fixture_metadata`
- `dataset_stats`
- `sink_mode`
- `results`
