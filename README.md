# Nexmark Bench

这个目录包含 Datalayers、RisingWave、Flink 的本地 Nexmark benchmark harness。

## 当前设计

- benchmark 只消费一个已经预先生成好的 keyed bid JSONL dataset。
- dataset 由 [datagen.sh](/home/nsc/nexmark-bench/datagen.sh:1) 调用 [nexmark_fixture.py](/home/nsc/nexmark-bench/nexmark_fixture.py:1) 生成。
- `nexmark_fixture.py` 会使用官方 `nexmark-flink` datagen 生成 combined Nexmark events，然后抽取 `Bid` 事件。
- 抽取出的 bid 会被扁平化，并写入稳定命名的 keyed JSONL 文件，默认是 [nexmark_bid.keyed.jsonl](/home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl)。
- datagen 会同时写出与 dataset 同名关联的 stats JSON；默认 stats 文件是 [nexmark_bid.keyed.stats.json](/home/nsc/nexmark-bench/nexmark_bid.keyed.stats.json)。
- benchmark 脚本不再在运行时临时生成 fixture。它们只接收 `--dataset`，默认指向仓库根目录下的稳定 dataset 文件。
- 当前默认 query 是 `q0,q1,q2,q14,q21,q22`。
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
  写 keyed dataset 时使用的逻辑 key 数量。默认 `4`。
- `--bench-root DIR`
  datagen 临时工作目录根路径。
- `--no-cleanup`
  保留 datagen 临时 Kafka 容器和工作目录，便于排查。

## Benchmark 入口

- [bench_datalayers.sh](/home/nsc/nexmark-bench/bench_datalayers.sh:1)
- [bench_risingwave.sh](/home/nsc/nexmark-bench/bench_risingwave.sh:1)
- [bench_flink.sh](/home/nsc/nexmark-bench/bench_flink.sh:1)

核心实现：

- [nexmark_fixture.py](/home/nsc/nexmark-bench/nexmark_fixture.py:1)
- [datalayers_bench_runner.py](/home/nsc/nexmark-bench/datalayers_bench_runner.py:1)
- [risingwave_bench_runner.py](/home/nsc/nexmark-bench/risingwave_bench_runner.py:1)
- [flink_bench_runner.py](/home/nsc/nexmark-bench/flink_bench_runner.py:1)

## 推荐用法

先生成 dataset：

```bash
bash ./datagen.sh
```

跑 Datalayers：

```bash
bash ./bench_datalayers.sh \
  --dataset /home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl \
  --queries q0,q1,q2,q14,q21,q22 \
  --sink table \
  --host 127.0.0.1 \
  --port 8361
```

如果 Datalayers 就运行在默认地址 `127.0.0.1:8361`，可以省略 `--host/--port`：

```bash
bash ./bench_datalayers.sh \
  --dataset /home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl \
  --queries q0,q1,q2,q14,q21,q22 \
  --sink table
```

跑 RisingWave：

```bash
bash ./bench_risingwave.sh \
  --dataset /home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl \
  --queries q0,q1,q2,q14,q21,q22 \
  --parallelism 1 \
  --sink table
```

跑 Flink：

```bash
bash ./bench_flink.sh \
  --dataset /home/nsc/nexmark-bench/nexmark_bid.keyed.jsonl \
  --queries q0,q1,q2,q14,q21,q22
```

## 脚本参数

### `bench_datalayers.sh`

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
  `table` 通过 sink table 的 `COUNT(*)` 判定完成；
  `blackhole` 通过 Kafka consumer group lag 判定完成。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka 和 benchmark 创建的对象；未传时会自动 cleanup。无论是否传这个参数，runner 在正式开始前都会先做一轮预清理；cleanup 顺序会先 drop pipeline，再 drop source/sink/table，最后 drop database。

### `bench_risingwave.sh`

主要参数：

- `--dataset PATH`
  keyed JSONL dataset 路径。默认指向仓库根目录下的稳定 dataset。runner 会自动读取与其同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。
- `--parallelism N`
  RisingWave `single_node` 的并行度。
- `--sink table|blackhole`
  `table` 创建 materialized view，并通过 `COUNT(*)` 判定完成。
  `blackhole` 创建 blackhole sink，并通过 `rw_catalog.rw_kafka_job_lag` 判定完成。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka、RisingWave 容器和 benchmark 创建的 source/MV/sink；未传时会自动 cleanup。
- `--image IMAGE`
  覆盖默认 RisingWave 镜像。

### `bench_flink.sh`

主要参数：

- `--dataset PATH`
  keyed JSONL dataset 路径。默认指向仓库根目录下的稳定 dataset。runner 会自动读取与其同名关联的 stats JSON。
- `--queries LIST`
  逗号分隔 query 列表。
- `--bench-root DIR`
  本次 benchmark 的临时根目录。
- `--no-cleanup`
  保留 Kafka 容器；未传时会自动 cleanup。

Flink runner 固定使用 blackhole sink。完成判定是 Kafka consumer group lag 到 0 后主动 cancel job。

## 指标口径

三个 runner 都会统计这些核心指标：

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
- `kafka_preload_sec`
  本轮 query 输入 preload 到 Kafka 的耗时，不计入 `throughput_rps` 分母。

Flink 的结果没有 `inserted_rows`，因为当前完成判定是 source 消费完所有输入后主动 cancel job，结果里保留的是 `expected_rows` 和 `state`。

dataset stats 里的这些字段是提前从 dataset 扫描得到的：

- `total_rows`
- `q2_expected_rows`
- `q14_expected_rows`
- `q21_expected_rows`

三个 runner 会直接读取与 `--dataset` 同名关联的 stats JSON，而不是在 benchmark 启动时重新全量扫描 dataset。当前命名规则是把 dataset 文件名末尾的 `.jsonl` 替换成 `.stats.json`；例如 `foo.keyed.jsonl` 对应 `foo.keyed.stats.json`。

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

三个 `report.json` 现在都使用同一组顶层字段：

- `generated_at`
- `engine`
- `mode`
- `fixture`
- `fixture_metadata`
- `dataset_stats`
- `sink_mode`
- `results`
