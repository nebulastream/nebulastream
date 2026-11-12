-- The caller explicitly accepts replaying a POST that may have already been processed.
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK responses(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('responses.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, HTTP_CALL(data, 'HTTP_SERVER/echo?drop_on=2', 30000, 1) AS response FROM texts INTO responses;
