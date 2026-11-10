-- Native scalars and actual nullable VARSIZED values cross the coroutine boundary.
CREATE LOGICAL SOURCE typed_nullable(id UINT64 NOT NULL, letter CHAR NOT NULL, score FLOAT64 NOT NULL, text VARSIZED);
CREATE PHYSICAL SOURCE FOR typed_nullable TYPE File SET('tests/testdata/typed_nullable.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK results(id UINT64 NOT NULL, letter CHAR NOT NULL, score FLOAT64 NOT NULL, text VARSIZED, missing BOOLEAN NOT NULL) TYPE File SET('typed_nullable.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id,
       ASYNC_DELAY(letter, 0) AS letter,
       ASYNC_DELAY(score, 0) AS score,
       ASYNC_DELAY(text, 0) AS text,
       ASYNC_DELAY(text, 0) IS NULL AS missing
FROM typed_nullable INTO results;
