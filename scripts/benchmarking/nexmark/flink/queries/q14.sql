-- Local variant of upstream q14: upstream counts 'c' in `extra` with the Java UDF
-- com.github.nexmark.flink.udf.CountChar, which the stock Flink image does not ship.
-- CHAR_LENGTH(extra) - CHAR_LENGTH(REPLACE(extra, 'c', '')) computes the same count.

CREATE TABLE nexmark_q14 (
    auction BIGINT,
    bidder BIGINT,
    price  DECIMAL(23, 3),
    bidTimeType VARCHAR,
    `dateTime` TIMESTAMP(3),
    extra VARCHAR,
    c_counts BIGINT
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q14
SELECT
    auction,
    bidder,
    0.908 * price as price,
    CASE
        WHEN HOUR(`dateTime`) >= 8 AND HOUR(`dateTime`) <= 18 THEN 'dayTime'
        WHEN HOUR(`dateTime`) <= 6 OR HOUR(`dateTime`) >= 20 THEN 'nightTime'
        ELSE 'otherTime'
    END AS bidTimeType,
    `dateTime`,
    extra,
    CAST(CHAR_LENGTH(extra) - CHAR_LENGTH(REPLACE(extra, 'c', '')) AS BIGINT) AS c_counts
FROM bid
WHERE 0.908 * price > 1000000 AND 0.908 * price < 50000000;
