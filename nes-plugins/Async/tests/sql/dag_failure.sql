-- A root failure must cancel the other root, not wait for its ten-minute timer.
CREATE LOGICAL SOURCE textstream(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR textstream TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK results(response VARSIZED NOT NULL, waiting VARSIZED NOT NULL) TYPE File SET('dag_failure.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT HTTP_CALL(data, 'http://127.0.0.1:1/echo') AS response, ASYNC_DELAY(data, 600000) AS waiting
FROM textstream INTO results;
