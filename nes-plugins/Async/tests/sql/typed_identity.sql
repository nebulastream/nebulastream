-- Equal-looking constants of different types are different calls. A field is not a string literal either.
CREATE LOGICAL SOURCE textstream(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR textstream TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK results(id UINT64 NOT NULL, number UINT8 NOT NULL, text VARSIZED NOT NULL, literal VARSIZED NOT NULL) TYPE File SET('typed_identity.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, ASYNC_DELAY(1, 0) AS number, ASYNC_DELAY('1', 0) AS text, ASYNC_DELAY('DATA', 0) AS literal
FROM textstream WHERE ASYNC_DELAY(data, 0) = 'Hello' INTO results;
