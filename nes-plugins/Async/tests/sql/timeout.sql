-- The service takes five seconds to answer, the call gives it half a second.
-- HTTP_SERVER is replaced with the url of tests/http_path_server.py, which answers with "<path>:<body>".
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK responses(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('responses.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, HTTP_CALL(data, 'HTTP_SERVER/slow?delay_ms=5000', 500) AS response FROM texts INTO responses;
