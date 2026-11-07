-- Local variant of upstream q10: the same projection, but an unpartitioned sink so the
-- reference run can collect one CSV (upstream: filesystem sink PARTITIONED BY (dt, hm)).

CREATE TABLE nexmark_q10 (
  auction  BIGINT,
  bidder  BIGINT,
  price  BIGINT,
  `dateTime`  TIMESTAMP(3),
  extra  VARCHAR,
  dt STRING,
  hm STRING
) WITH (
  'connector' = 'blackhole'
);

INSERT INTO nexmark_q10
SELECT auction, bidder, price, `dateTime`, extra, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm')
FROM bid;
