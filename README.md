# Nexmark Benchmark

这个目录包含 Datalayers、RisingWave、Flink 的 Nexmark benchmark 脚本。

## 当前设计

- 唯一支持的 fixture 是官方 Nexmark 生成器。
- benchmark 先用官方 `nexmark-flink` datagen 生成 combined Nexmark events 到 Kafka topic `nexmark`。
- 再从 combined event 中抽取 `bid` 事件，扁平化为本地 `jsonl` fixture。
- benchmark 期间把这份 bid fixture preload 到 query topic，再从 `earliest` 回放。
- 当前默认 query 是 `q0,q1,q2,q14,q21,q22`。
- 当前默认输入行数是 `1000000`。
- `throughput = 输入行数 / replay 耗时`。

这套 benchmark 当前衡量的是：

- preload 完成后，从 source/query 创建开始，到回放完成为止的 replay 窗口
- replay 窗口内的平均 CPU 与内存采样

这套 benchmark 当前不再支持手工 `synthetic` fixture。

## 输入数据

benchmark 消费的 topic message 是扁平化后的 bid JSON，不是完整的 Nexmark union event。

这份 bid fixture 来自官方 combined Nexmark events 的抽取，而不是手工构造。

字段：

- `ts`
- `auction`
- `bidder`
- `price`
- `channel`
- `url`
- `extra`

## 入口脚本

- [bench_datalayers.sh](/home/nsc/nexmark-bench/bench_datalayers.sh:1)
- [bench_risingwave.sh](/home/nsc/nexmark-bench/bench_risingwave.sh:1)
- [bench_flink.sh](/home/nsc/nexmark-bench/bench_flink.sh:1)

核心实现：

- [nexmark_fixture.py](/home/nsc/nexmark-bench/nexmark_fixture.py:1)
- [datalayers_bench_runner.py](/home/nsc/nexmark-bench/datalayers_bench_runner.py:1)
- [risingwave_bench_runner.py](/home/nsc/nexmark-bench/risingwave_bench_runner.py:1)
- [flink_bench_runner.py](/home/nsc/nexmark-bench/flink_bench_runner.py:1)

## 推荐用法

Datalayers:

```bash
bash ./bench_datalayers.sh \
  --datalayers-path /home/nsc/datalayers \
  --rows 1000000 \
  --queries q0,q1,q2,q14,q21,q22 \
  --sink table
```

RisingWave:

```bash
bash ./bench_risingwave.sh \
  --rows 1000000 \
  --queries q0,q1,q2,q14,q21,q22 \
  --parallelism 1 \
  --sink table
```

Flink:

```bash
bash ./bench_flink.sh \
  --rows 1000000 \
  --queries q0,q1,q2,q14,q21,q22
```

## 参数

### `bench_datalayers.sh`

支持：

- `--datalayers-path ABS_PATH`
- `--rows`
- `--queries`
- `--sink`
- `--skip-build`
- `--bench-root DIR`
- `--no-cleanup`

说明：

- `--datalayers-path ABS_PATH`
  - 必填
  - 指向本地 Datalayers 仓库的绝对路径
  - 脚本会优先复用 `<ABS_PATH>/target/reldev/datalayers` 和 `<ABS_PATH>/target/reldev/dlsql`
  - 如果二进制不存在，且没有传 `--skip-build`，脚本会在该仓库下执行 `cargo build --profile reldev --bin datalayers --bin dlsql`
- `--sink`
  - `table`: 写 Datalayers table，通过 `COUNT(*)` 判定完成
  - `blackhole`: 写 Datalayers blackhole sink，通过 Kafka consumer group lag 判定完成
- `--skip-build`
  - 跳过 `reldev` 编译，复用已有二进制
- `--bench-root DIR`
  - 指定本次 benchmark 的临时根目录
- `--no-cleanup`
  - 保留 Kafka 容器
  - 保留 Datalayers 进程
  - 保留 benchmark 创建的 database/source/sink/pipeline/table

### `bench_risingwave.sh`

支持：

- `--rows`
- `--queries`
- `--parallelism`
- `--sink`
- `--bench-root DIR`
- `--no-cleanup`
- `--image IMAGE`

说明：

- `--parallelism`
  - 控制 RisingWave `single_node` 并行度
- `--sink`
  - `table`: 创建 materialized view，通过 `COUNT(*)` 判定完成
  - `blackhole`: 创建 blackhole sink，通过 `rw_catalog.rw_kafka_job_lag` 判定完成
- `--bench-root DIR`
  - 指定本次 benchmark 的临时根目录
- `--no-cleanup`
  - 保留 Kafka 容器
  - 保留 RisingWave 容器和 network
  - 保留 benchmark 创建的 source/materialized view/sink
- `--image IMAGE`
  - 覆盖默认镜像

注意：

- RisingWave 官方 benchmark 页面使用 `blackhole` 口径。
- 当前本地环境更稳定的路径仍然是 `--sink table`，因为不同 RisingWave 版本对 `rw_catalog.rw_kafka_job_lag` 的可用性不完全一致。
- 因此本地 `table` 结果不能直接和官方 `blackhole` 吞吐量绝对值或 query 间比值等同。

### `bench_flink.sh`

支持：

- `--rows`
- `--queries`
- `--bench-root DIR`
- `--no-cleanup`

说明：

- Flink runner 固定使用官方 fixture。
- Flink runner 固定使用 `blackhole` sink。
- 完成判定通过 Kafka consumer group lag 到 0 后主动 cancel Flink job。

## 结果文件

每次 benchmark 的输出都会写到一个临时根目录。

Datalayers:

- `<bench_root>/datalayers/report.md`
- `<bench_root>/datalayers/report.json`
- `<bench_root>/datalayers/<query>_samples.csv`

RisingWave:

- `<bench_root>/risingwave/report.md`
- `<bench_root>/risingwave/report.json`
- `<bench_root>/risingwave/<query>_samples.csv`

Flink:

- `<bench_root>/flink/report.md`
- `<bench_root>/flink/report.json`
- `<bench_root>/flink/<query>_samples.csv`

## 对象命名

Datalayers:

- database: `nexmark_q21`
- source: `q21_src`
- pipeline: `q21_pipeline`
- table sink: `q21_sink`
- blackhole sink: `q21_bh`

RisingWave:

- source: `q21_src_<timestamp>`
- materialized view: `q21_mv`
- blackhole sink: `q21_bh`

Flink:

- source table: `q21_src`
- sink table: `q21_sink`
- statement set / insert job: `q21`

## 保留现场后如何观察

如果传了 `--no-cleanup`，benchmark 结束后仍然可以继续查看对象和数据。

### Datalayers

```bash
bash ./bench_datalayers.sh --datalayers-path /home/nsc/datalayers --sink table --no-cleanup
```

查看 database：

```bash
./target/reldev/dlsql -h 127.0.0.1 -P <PORT> -u admin -p public -e "SHOW DATABASES"
```

查看 pipeline：

```bash
./target/reldev/dlsql -h 127.0.0.1 -P <PORT> -u admin -p public -d nexmark_q21 -e "SHOW PIPELINES"
```

查看 source：

```bash
./target/reldev/dlsql -h 127.0.0.1 -P <PORT> -u admin -p public -d nexmark_q21 -e "SHOW SOURCES"
```

查看 sink table 行数：

```bash
./target/reldev/dlsql -h 127.0.0.1 -P <PORT> -u admin -p public -d nexmark_q21 -e "SELECT COUNT(*) AS c FROM q21_sink"
```

如果是 `--sink blackhole`，可以查看 Kafka consumer group：

```bash
docker exec <KAFKA_CONTAINER> kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --describe --group <GROUP_ID>
```

### RisingWave

```bash
bash ./bench_risingwave.sh --parallelism 1 --sink table --no-cleanup
```

查看 source：

```bash
psql -h 127.0.0.1 -p <PORT> -U root -d dev -c "SELECT id, name FROM rw_catalog.rw_sources ORDER BY id"
```

查看 materialized view：

```bash
psql -h 127.0.0.1 -p <PORT> -U root -d dev -c "SELECT id, name FROM rw_catalog.rw_materialized_views ORDER BY id"
```

查看 sink：

```bash
psql -h 127.0.0.1 -p <PORT> -U root -d dev -c "SELECT id, name, connector, sink_type FROM rw_catalog.rw_sinks ORDER BY id"
```

查看 MV 行数：

```bash
psql -h 127.0.0.1 -p <PORT> -U root -d dev -c "SELECT COUNT(*) FROM q21_mv"
```

### Flink

```bash
bash ./bench_flink.sh --no-cleanup
```

查看 job：

```bash
curl -s http://127.0.0.1:<FLINK_REST_PORT>/jobs | jq
```

查看 Kafka consumer group：

```bash
docker exec <KAFKA_CONTAINER> kafka-consumer-groups --bootstrap-server 127.0.0.1:9092 --describe --group <GROUP_ID>
```

## 其他说明

- 三个入口脚本都会为本轮 benchmark 生成唯一的临时目录、容器名和端口。
- Datalayers 使用 host 可达的 Kafka broker。
- RisingWave 使用容器网络内可达的 Kafka broker。
- Flink 同时使用 host 侧和容器网络内 broker 地址。
- dataset stats 里的 `q2_expected_rows`、`q14_expected_rows`、`q21_expected_rows` 表示同一批官方 bid 输入在对应 query 下的理论输出行数。
