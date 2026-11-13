-- The generator emits 20 rows per second and flushes each row in a buffer of its own; every row waits two seconds.
CREATE LOGICAL SOURCE ids(id UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR ids TYPE Generator SET(
       'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as INPUT_FORMATTER."TYPE",
       60000 AS "SOURCE".MAX_RUNTIME_MS,
       'emit_rate 20' AS "SOURCE".GENERATOR_RATE_CONFIG,
       1 AS "SOURCE".SEED,
       'SEQUENCE UINT64 0 20 1' AS "SOURCE".GENERATOR_SCHEMA);
CREATE SINK delayed(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('delayed.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, ASYNC_DELAY('waited', 2000) AS response FROM ids INTO delayed;
