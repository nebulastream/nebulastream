-- The service answers the first 300 calls and fails every later one. By then the source has produced far more rows
-- than the calls finished, so the channel to the async stage is full and holds the producer back.
-- HTTP_SERVER is replaced with the url of tests/http_path_server.py, which answers with "<path>:<body>".
CREATE LOGICAL SOURCE ids(id UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR ids TYPE Generator SET(
       'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as INPUT_FORMATTER."TYPE",
       60000 AS "SOURCE".MAX_RUNTIME_MS,
       'emit_rate 2000' AS "SOURCE".GENERATOR_RATE_CONFIG,
       1 AS "SOURCE".SEED,
       'SEQUENCE UINT64 0 20000 1' AS "SOURCE".GENERATOR_SCHEMA);
CREATE SINK responses(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('responses.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, HTTP_CALL('ping', 'HTTP_SERVER/slow?delay_ms=50&fail_after=300') AS response FROM ids INTO responses;
