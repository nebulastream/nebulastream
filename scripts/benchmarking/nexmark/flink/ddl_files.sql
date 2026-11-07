-- Flink sources over the CSV files written by ../datagen.
-- Replaces upstream ddl_gen.sql + ddl_views.sql: the views below expose the exact column names, order and
-- types of the upstream views, so the upstream query files run unchanged.

SET 'table.dml-sync' = 'true';
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '1s';
-- Join state (q7) exceeds the 5 MB limit of the default in-memory checkpoint storage; a failing checkpoint
-- would otherwise restart the job forever.
SET 'state.checkpoint-storage' = 'filesystem';
SET 'state.checkpoints.dir' = 'file:///tmp/flink-checkpoints';
SET 'restart-strategy.type' = 'none';

CREATE TABLE person_file (
    id BIGINT, name VARCHAR, emailAddress VARCHAR, creditCard VARCHAR, city VARCHAR, state VARCHAR,
    dateTimeMs BIGINT, extra VARCHAR,
    `dateTime` AS CAST(TO_TIMESTAMP_LTZ(dateTimeMs, 3) AS TIMESTAMP(3)),
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
) WITH ('connector' = 'filesystem', 'path' = 'file:///data/person.csv', 'format' = 'csv');

CREATE TABLE auction_file (
    id BIGINT, itemName VARCHAR, description VARCHAR, initialBid BIGINT, reserve BIGINT,
    dateTimeMs BIGINT, expiresMs BIGINT, seller BIGINT, category BIGINT, extra VARCHAR,
    `dateTime` AS CAST(TO_TIMESTAMP_LTZ(dateTimeMs, 3) AS TIMESTAMP(3)),
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
) WITH ('connector' = 'filesystem', 'path' = 'file:///data/auction.csv', 'format' = 'csv');

CREATE TABLE bid_file (
    auction BIGINT, bidder BIGINT, price BIGINT, channel VARCHAR, url VARCHAR, dateTimeMs BIGINT, extra VARCHAR,
    `dateTime` AS CAST(TO_TIMESTAMP_LTZ(dateTimeMs, 3) AS TIMESTAMP(3)),
    WATERMARK FOR `dateTime` AS `dateTime` - INTERVAL '4' SECOND
) WITH ('connector' = 'filesystem', 'path' = 'file:///data/bid.csv', 'format' = 'csv');

CREATE VIEW person AS
SELECT id, name, emailAddress, creditCard, city, state, `dateTime`, extra FROM person_file;

CREATE VIEW auction AS
SELECT id, itemName, description, initialBid, reserve, `dateTime`,
       CAST(TO_TIMESTAMP_LTZ(expiresMs, 3) AS TIMESTAMP(3)) AS expires, seller, category, extra
FROM auction_file;

CREATE VIEW bid AS
SELECT auction, bidder, price, channel, url, `dateTime`, extra FROM bid_file;
