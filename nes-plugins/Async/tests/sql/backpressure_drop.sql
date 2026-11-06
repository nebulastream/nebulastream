-- Every call takes 50 ms while the source emits 2000 rows per second, so the channel fills and the producer is held back
-- for as long as the query runs.
-- HTTP_SERVER is replaced with the url of tests/http_path_server.py, which answers with "<path>:<body>".
CREATE LOGICAL SOURCE ids(id UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR ids TYPE Generator SET(
       'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as INPUT_FORMATTER."TYPE",
       600000 AS "SOURCE".MAX_RUNTIME_MS,
       'emit_rate 2000' AS "SOURCE".GENERATOR_RATE_CONFIG,
       1 AS "SOURCE".SEED,
       'SEQUENCE UINT64 0 1000000 1' AS "SOURCE".GENERATOR_SCHEMA);
CREATE SINK responses(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('responses.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, HTTP_CALL('ping', 'HTTP_SERVER/slow?delay_ms=50') AS response FROM ids INTO responses SET ('5a1c0de0-0000-4000-8000-000000000002' AS "QUERY"."ID");
