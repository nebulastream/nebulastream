-- Three rows in one buffer, each waiting one second; with a batch size of three, one wait covers all of them.
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK batched(id UINT64 NOT NULL, response VARSIZED NOT NULL, covered VARSIZED NOT NULL) TYPE File SET('batched.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, ASYNC_DELAY(data, 1000, 3) AS response, ASYNC_OUTPUT(ASYNC_DELAY(data, 1000, 3), 1) AS covered FROM texts INTO batched;
