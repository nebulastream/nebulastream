-- Retries are explicitly requested per HTTP_CALL. The count may be a per-row integer expression.
-- HTTP_SERVER is replaced with the url of tests/http_path_server.py.
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK responses(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('responses.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, HTTP_CALL(data, 'HTTP_SERVER/flaky?fail_first=2', 30000, id - id + 2) AS response FROM texts INTO responses;
