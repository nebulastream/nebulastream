-- Two queries share the event loop: the first fails at once, while the calls of the second are still waiting.
-- HTTP_SERVER is replaced with the url of tests/http_path_server.py, which answers with "<path>:<body>".
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK failed(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('failed.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK healthy(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('healthy.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, HTTP_CALL(data, 'HTTP_SERVER/slow?delay_ms=1000') AS response FROM texts INTO healthy;
SELECT id, HTTP_CALL(data, 'http://127.0.0.1:1/echo') AS response FROM texts INTO failed;
