-- HTTP_SERVER is replaced with the url of tests/http_path_server.py, which answers with "<path>:<body>".
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK passThrough(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('pass_through.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK computed(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('computed.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK nested(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('nested.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK mixed(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('mixed.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
-- A field forwarded next to the call result.
SELECT id, HTTP_CALL(data, 'HTTP_SERVER/echo') AS response FROM texts INTO passThrough;
-- A computed argument, and sync work on the result.
SELECT id, CONCAT(HTTP_CALL(CONCAT(data, '!'), 'HTTP_SERVER/echo'), '?') AS response FROM texts INTO computed;
-- Nested calls: two async stages with sync work in between.
SELECT id, HTTP_CALL(CONCAT(HTTP_CALL(data, 'HTTP_SERVER/a'), '-'), 'HTTP_SERVER/b') AS response FROM texts INTO nested;
-- Two async functions in one stage.
SELECT id, CONCAT(ASYNC_DELAY(data, 10), HTTP_CALL(data, 'HTTP_SERVER/echo')) AS response FROM texts INTO mixed;
