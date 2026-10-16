-- Three rows in one buffer, each waiting one second, one row per call.
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK unbatched(id UINT64 NOT NULL, covered VARSIZED NOT NULL) TYPE File SET('unbatched.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, ASYNC_OUTPUT(ASYNC_DELAY(data, 1000), 1) AS covered FROM texts INTO unbatched;
