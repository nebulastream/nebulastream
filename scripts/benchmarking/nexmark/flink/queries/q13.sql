-- Local variant of upstream q13: upstream joins `FOR SYSTEM_TIME AS OF PROCTIME()` against a
-- static side input file. With a static table that is the same as a regular join, which also
-- runs in batch mode and is deterministic. The side input comes from ../datagen (side_input.csv).

CREATE TABLE side_input (
  key BIGINT,
  `value` VARCHAR
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///data/side_input.csv',
  'format' = 'csv'
);

CREATE TABLE nexmark_q13 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  `value`  VARCHAR
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q13
SELECT
    B.auction,
    B.bidder,
    B.price,
    B.`dateTime`,
    S.`value`
FROM bid B
JOIN side_input S
ON mod(B.auction, 10000) = S.key;
