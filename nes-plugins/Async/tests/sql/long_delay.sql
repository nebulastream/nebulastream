-- Each row waits ten minutes, far longer than the test runs.
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK delayed(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('delayed.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, ASYNC_DELAY(data, 600000) AS response FROM texts INTO delayed SET ('5a1c0de0-0000-4000-8000-000000000001' AS "QUERY"."ID");
