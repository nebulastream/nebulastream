-- HTTP_SERVER is replaced with the url of tests/http_path_server.py, which answers with "<path>:<body>".
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK chained(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('chained.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK reused(id UINT64 NOT NULL, output VARSIZED NOT NULL) TYPE File SET('reused.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
-- Directly nested calls run in one async stage, the outer call after the inner one of the same row.
SELECT id, HTTP_CALL(HTTP_CALL(data, 'HTTP_SERVER/inner'), 'HTTP_SERVER/outer') AS response FROM texts INTO chained;
-- The SELECT reads the result of the calls the WHERE already made.
SELECT id, HTTP_CALL(HTTP_CALL(data, 'HTTP_SERVER/both'), 'HTTP_SERVER/both') AS output
FROM texts
WHERE HTTP_CALL(HTTP_CALL(data, 'HTTP_SERVER/both'), 'HTTP_SERVER/both') = '/both:/both:Hello'
INTO reused;
