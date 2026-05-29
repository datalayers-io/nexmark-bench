# Nexmark 介绍

Nexmark 是流计算领域被广泛使用的压测套件，最初由 Apache Beam 团队开发，后来被 Apache Flink、Apache Spark 等项目采用。Nexmark 模拟了一个在线拍卖系统的场景，包含了用户注册、拍卖创建、出价等一系列事件。通过执行一系列预定义的查询，Nexmark 可以评估流处理引擎在不同查询类型下的性能表现。

Nexmark 的标准事件模型通常是一个统一流，里面有三类事件：

- `person`
- `auction`
- `bid`

每个事件的结构如下：

```text
person: struct<
  id, name, email_address, credit_card, city, state, datetime, extra
>

auction: struct<
  id, description, item_name, initial_bid, reserve, datetime, expires, seller, category, extra
>

bid: struct<
  auction, bidder, price, channel, url, datetime, extra
>
```

## Nexmark 查询列表

| Query | 语义 | 核心特征 | Datalayers 是否支持 |
| --- | --- | --- | --- |
| q0 | 直接输出输入事件 | 直接透传 | 是 |
| q1 | 对出价价格做货币换算 | 投影、算术表达式 | 是 |
| q2 | 过滤特定 auction 的出价 | 过滤、投影 | 是 |
| q3 | 将 auction 和 person 关联，找出某些区域里的拍卖 | 两流 join、过滤 | 否 |
| q4 | 统计某个 category 的平均成交价 | join、聚合 | 否 |
| q5 | 在滑动窗口中找最热 auction | 滑动窗口聚合 | 否 |
| q6 | 统计 seller 的平均成交价 | join、Top-N、聚合 | 否 |
| q7 | 找出最高出价 | 窗口聚合 | 否 |
| q8 | 监控新用户行为 | 两流窗口 join | 否 |
| q9 | 求每个 auction 的 winning bid | 多流 join | 否 |
| q10 | 把结果写入文件系统 | 外部 sink 写文件 | 否 |
| q11 | 基于 session window 统计用户会话 | session window | 否 |
| q12 | 基于 processing time 做窗口统计 | processing-time window | 否 |
| q13 | 流和 side input 做 join | 流和有界表 join | 否 |
| q14 | 对事件做复杂投影和计算 | 复杂投影、类型转换、过滤 | 是 |
| q15 | 做多种 distinct 统计报表 | 多个 distinct 聚合 | 否 |
| q16 | 做按 channel 的统计报表 | 多 key、多 distinct 聚合 | 否 |
| q17 | 做 auction 统计报表 | 无界聚合 | 否 |
| q18 | 找最后一条 bid | 去重 / last row | 否 |
| q19 | 找价格最高的前 N 个 auction | Top-N | 否 |
| q20 | 用 auction 信息扩展 bid | join、过滤 | 否 |
| q21 | 从 channel/url 中提取 channel id | `CASE WHEN`、正则提取 | 是 |
| q22 | 从 URL 中提取目录层级 | URL 拆分函数 | 是 |

## Datalayers 不支持的查询

| Query | 主要阻塞 |
| --- | --- |
| q3 | 需要 stream join |
| q4 | 需要 join + 聚合 |
| q5 | 需要滑动窗口聚合 |
| q6 | 需要 join + Top-N + 聚合 |
| q7 | 需要窗口聚合 |
| q8 | 需要窗口 join |
| q9 | 需要多流 join |
| q10 | 官方语义需要 file sink；当前只有 blackhole sink |
| q11 | 需要 session window 聚合 |
| q12 | 需要 processing-time window |
| q13 | 需要 bounded side input join |
| q15 | 需要多个 distinct 聚合 |
| q16 | 需要多 key、多 distinct 聚合 |
| q17 | 需要无界聚合 |
| q18 | 需要 dedup / last-row 语义 |
| q19 | 需要 Top-N |
| q20 | 需要 join |

## RisingWave 的 Nexmark SQL

### RisingWave q0

```sql
CREATE SINK nexmark_q0
AS
SELECT auction, bidder, price, date_time
FROM bid
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q1

```sql
CREATE SINK nexmark_q1
AS
SELECT auction,
       bidder,
       0.908 * price as price,
       date_time
FROM bid
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q2

```sql
CREATE SINK nexmark_q2
AS
SELECT auction, price
FROM bid
WHERE auction = 1007
   OR auction = 1020
   OR auction = 2001
   OR auction = 2019
   OR auction = 2087
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q3

```sql
CREATE SINK nexmark_q3
AS
SELECT P.name,
       P.city,
       P.state,
       A.id
FROM auction AS A
         INNER JOIN person AS P on A.seller = P.id
WHERE A.category = 10
  and (P.state = 'or' OR P.state = 'id' OR P.state = 'ca')
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q4

```sql
CREATE SINK nexmark_q4
AS
SELECT Q.category,
       AVG(Q.final) as avg
FROM (SELECT MAX(B.price) AS final,
             A.category
      FROM auction A,
           bid B
      WHERE A.id = B.auction
        AND B.date_time BETWEEN A.date_time AND A.expires
      GROUP BY A.id, A.category) Q
GROUP BY Q.category
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q5

```sql
CREATE SINK nexmark_q5
AS
SELECT
    AuctionBids.auction, AuctionBids.num
FROM (
    SELECT
        bid.auction,
        count(*) AS num,
        window_start AS starttime
    FROM
        HOP(bid, date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
    GROUP BY
        bid.auction,
        window_start
) AS AuctionBids
JOIN (
    SELECT
        max(CountBids.num) AS maxn,
        CountBids.starttime_c
    FROM (
        SELECT
            count(*) AS num,
            window_start AS starttime_c
        FROM
            HOP(bid, date_time, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
        GROUP BY
            bid.auction,
            window_start
        ) AS CountBids
    GROUP BY
        CountBids.starttime_c
    ) AS MaxBids
ON
    AuctionBids.starttime = MaxBids.starttime_c AND
    AuctionBids.num >= MaxBids.maxn
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q6

RisingWave 主目录中对应文件是 `q6-group-top1.sql`：

```sql
CREATE SINK nexmark_q6_group_top1
AS
SELECT
    Q.seller,
    AVG(Q.final) OVER
        (PARTITION BY Q.seller ORDER BY Q.date_time ROWS BETWEEN 10 PRECEDING AND CURRENT ROW)
    as avg
FROM (
    SELECT ROW_NUMBER() OVER (PARTITION BY A.id, A.seller ORDER BY B.price) as rank, A.seller, B.price as final,  B.date_time
    FROM auction AS A, bid AS B
    WHERE A.id = B.auction and B.date_time between A.date_time and A.expires
) AS Q
WHERE Q.rank <= 1
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q7

```sql
CREATE SINK nexmark_q7
AS
SELECT B.auction,
       B.price,
       B.bidder,
       B.date_time
from bid B
         JOIN (SELECT MAX(price) AS maxprice,
                      window_end as date_time
               FROM
                   TUMBLE(bid, date_time, INTERVAL '10' SECOND)
               GROUP BY window_end) B1 ON B.price = B1.maxprice
WHERE B.date_time BETWEEN B1.date_time - INTERVAL '10' SECOND
          AND B1.date_time
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q8

```sql
CREATE SINK nexmark_q8
AS
SELECT P.id,
       P.name,
       P.starttime
FROM (SELECT id,
             name,
             window_start AS starttime,
             window_end   AS endtime
      FROM
          TUMBLE(person, date_time, INTERVAL '10' SECOND)
      GROUP BY id,
               name,
               window_start,
               window_end) P
         JOIN (SELECT seller,
                      window_start AS starttime,
                      window_end   AS endtime
               FROM
                   TUMBLE(auction, date_time, INTERVAL '10' SECOND)
               GROUP BY seller,
                        window_start,
                        window_end) A ON P.id = A.seller
    AND P.starttime = A.starttime
    AND P.endtime = A.endtime
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q9

```sql
CREATE SINK nexmark_q9
AS
SELECT id,
       item_name,
       description,
       initial_bid,
       reserve,
       date_time,
       expires,
       seller,
       category,
       auction,
       bidder,
       price,
       bid_date_time
FROM (SELECT A.*,
             B.auction,
             B.bidder,
             B.price,
             B.date_time                                                                  AS bid_date_time,
             ROW_NUMBER() OVER (PARTITION BY A.id ORDER BY B.price DESC, B.date_time ASC) AS rownum
      FROM auction A,
           bid B
      WHERE A.id = B.auction
        AND B.date_time BETWEEN A.date_time AND A.expires) tmp
WHERE rownum <= 1
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q10

```sql
CREATE SINK nexmark_q10 AS
SELECT auction,
       bidder,
       price,
       date_time,
       TO_CHAR(date_time, 'YYYY-MM-DD') as date,
       TO_CHAR(date_time, 'HH:MI')      as time
FROM bid
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q11

RisingWave 在 `~/risingwave/ci/scripts/sql/nexmark/` 目录下没有提供 `q11.sql` 主版本文件。

### RisingWave q12

```sql
CREATE SINK nexmark_q12 AS
SELECT bidder, count(*) as bid_count, window_start, window_end
FROM TUMBLE(bid, p_time, INTERVAL '10' SECOND)
GROUP BY bidder, window_start, window_end
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q13

```sql
CREATE TABLE side_input(
    key BIGINT PRIMARY KEY,
    value VARCHAR
);
INSERT INTO side_input SELECT v, v::varchar FROM generate_series(0, ${BENCHMARK_NEXMARK_RISINGWAVE_Q13_SIDE_INPUT_ROW_COUNT} - 1) AS s(v);

CREATE SINK nexmark_q13 AS
SELECT B.auction, B.bidder, B.price, B.date_time, S.value
FROM bid B join side_input FOR SYSTEM_TIME AS OF PROCTIME() S on mod(B.auction, ${BENCHMARK_NEXMARK_RISINGWAVE_Q13_SIDE_INPUT_ROW_COUNT}) = S.key
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q14

```sql
CREATE FUNCTION count_char(s varchar, c varchar) RETURNS int LANGUAGE SQL AS
  $$SELECT LENGTH(s) - LENGTH(REPLACE(s, c, ''))$$;

CREATE SINK nexmark_q14 AS
SELECT auction,
       bidder,
       0.908 * price as price,
       CASE
           WHEN
                       extract(hour from date_time) >= 8 AND
                       extract(hour from date_time) <= 18
               THEN 'dayTime'
           WHEN
                       extract(hour from date_time) <= 6 OR
                       extract(hour from date_time) >= 20
               THEN 'nightTime'
           ELSE 'otherTime'
           END       AS bidTimeType,
       date_time,
       count_char(extra, 'c') AS c_counts
FROM bid
WHERE 0.908 * price > 1000000
  AND 0.908 * price < 50000000
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q15

```sql
SET rw_force_split_distinct_agg = ${BENCHMARK_NEXMARK_RISINGWAVE_Q15_RW_FORCE_SPLIT_DISTINCT_AGG};
SET rw_force_two_phase_agg = ${BENCHMARK_NEXMARK_RISINGWAVE_Q15_RW_FORCE_TWO_PHASE_AGG};
CREATE SINK nexmark_q15 AS
SELECT to_char(date_time, 'YYYY-MM-DD')                                          as "day",
       count(*)                                                                  AS total_bids,
       count(*) filter (where price < 10000)                                     AS rank1_bids,
       count(*) filter (where price >= 10000 and price < 1000000)                AS rank2_bids,
       count(*) filter (where price >= 1000000)                                  AS rank3_bids,
       count(distinct bidder)                                                    AS total_bidders,
       count(distinct bidder) filter (where price < 10000)                       AS rank1_bidders,
       count(distinct bidder) filter (where price >= 10000 and price < 1000000)  AS rank2_bidders,
       count(distinct bidder) filter (where price >= 1000000)                    AS rank3_bidders,
       count(distinct auction)                                                   AS total_auctions,
       count(distinct auction) filter (where price < 10000)                      AS rank1_auctions,
       count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
       count(distinct auction) filter (where price >= 1000000)                   AS rank3_auctions
FROM bid
GROUP BY to_char(date_time, 'YYYY-MM-DD')
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q16

```sql
SET rw_force_split_distinct_agg = ${BENCHMARK_NEXMARK_RISINGWAVE_Q16_RW_FORCE_SPLIT_DISTINCT_AGG};
SET rw_force_two_phase_agg = ${BENCHMARK_NEXMARK_RISINGWAVE_Q16_RW_FORCE_TWO_PHASE_AGG};
CREATE SINK nexmark_q16 AS
SELECT channel,
       to_char(date_time, 'YYYY-MM-DD')                                          as "day",
       max(to_char(date_time, 'HH:mm'))                                          as "minute",
       count(*)                                                                  AS total_bids,
       count(*) filter (where price < 10000)                                     AS rank1_bids,
       count(*) filter (where price >= 10000 and price < 1000000)                AS rank2_bids,
       count(*) filter (where price >= 1000000)                                  AS rank3_bids,
       count(distinct bidder)                                                    AS total_bidders,
       count(distinct bidder) filter (where price < 10000)                       AS rank1_bidders,
       count(distinct bidder) filter (where price >= 10000 and price < 1000000)  AS rank2_bidders,
       count(distinct bidder) filter (where price >= 1000000)                    AS rank3_bidders,
       count(distinct auction)                                                   AS total_auctions,
       count(distinct auction) filter (where price < 10000)                      AS rank1_auctions,
       count(distinct auction) filter (where price >= 10000 and price < 1000000) AS rank2_auctions,
       count(distinct auction) filter (where price >= 1000000)                   AS rank3_auctions
FROM bid
GROUP BY to_char(date_time, 'YYYY-MM-DD'), channel
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q17

```sql
CREATE SINK nexmark_q17 AS
SELECT auction,
       to_char(date_time, 'YYYY-MM-DD')                           AS day,
       count(*)                                                   AS total_bids,
       count(*) filter (where price < 10000)                      AS rank1_bids,
       count(*) filter (where price >= 10000 and price < 1000000) AS rank2_bids,
       count(*) filter (where price >= 1000000)                   AS rank3_bids,
       min(price)                                                 AS min_price,
       max(price)                                                 AS max_price,
       avg(price)                                                 AS avg_price,
       sum(price)                                                 AS sum_price
FROM bid
GROUP BY to_char(date_time, 'YYYY-MM-DD'), auction
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q18

```sql
CREATE SINK nexmark_q18 AS
SELECT auction, bidder, price, channel, url, date_time
FROM (SELECT *,
             ROW_NUMBER() OVER (
                 PARTITION BY bidder, auction
                 ORDER BY date_time DESC
                 ) AS rank_number
      FROM bid)
WHERE rank_number <= 1
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q19

```sql
CREATE SINK nexmark_q19 AS
SELECT *
FROM (SELECT *,
             ROW_NUMBER() OVER (
                 PARTITION BY auction
                 ORDER BY price DESC
             ) AS rank_number
      FROM bid)
WHERE rank_number <= 10
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q20

```sql
CREATE SINK nexmark_q20 AS
SELECT auction,
       bidder,
       price,
       channel,
       url,
       B.date_time as bid_date_time,
       B.extra     as bid_extra,
       item_name,
       description,
       initial_bid,
       reserve,
       A.date_time as auction_date_time,
       expires,
       seller,
       category,
       A.extra     as auction_extra
FROM bid AS B
         INNER JOIN auction AS A on B.auction = A.id
WHERE A.category = 10
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q21

```sql
CREATE SINK nexmark_q21 AS
SELECT auction,
       bidder,
       price,
       channel,
       CASE
           WHEN LOWER(channel) = 'apple' THEN '0'
           WHEN LOWER(channel) = 'google' THEN '1'
           WHEN LOWER(channel) = 'facebook' THEN '2'
           WHEN LOWER(channel) = 'baidu' THEN '3'
           ELSE (regexp_match(url, '(&|^)channel_id=([^&]*)'))[2]
           END
           AS channel_id
FROM bid
WHERE (regexp_match(url, '(&|^)channel_id=([^&]*)'))[2] is not null
   or LOWER(channel) in ('apple', 'google', 'facebook', 'baidu')
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```

### RisingWave q22

```sql
CREATE SINK nexmark_q22 AS
SELECT auction,
       bidder,
       price,
       channel,
       split_part(url, '/', 4) as dir1,
       split_part(url, '/', 5) as dir2,
       split_part(url, '/', 6) as dir3
FROM bid
WITH ( connector = 'blackhole', type = 'append-only', force_append_only = 'true');
```
