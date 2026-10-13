-- Generated ids select one of three urls; HTTP_SERVER is replaced with the url of tests/http_path_server.py.
CREATE LOGICAL SOURCE ids(id UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR ids TYPE Generator SET(
       'ALL' as "SOURCE".STOP_GENERATOR_WHEN_SEQUENCE_FINISHES,
       'CSV' as INPUT_FORMATTER."TYPE",
       10000 AS "SOURCE".MAX_RUNTIME_MS,
       'emit_rate 100' AS "SOURCE".GENERATOR_RATE_CONFIG,
       1 AS "SOURCE".SEED,
       'SEQUENCE UINT64 0 30 1' AS "SOURCE".GENERATOR_SCHEMA);
CREATE SINK responses(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('responses.csv' as "SINK".FILE_PATH, 'CSV' as "SINK".OUTPUT_FORMAT);
SELECT id, HTTP_CALL('ping', Conditional(id % 3 = 0, 'HTTP_SERVER/a', id % 3 = 1, 'HTTP_SERVER/b', 'HTTP_SERVER/c')) AS response FROM ids INTO responses;
