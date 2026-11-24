-- Typed delay values, dynamic/computed integer millis, and optional concurrency limit.
CREATE LOGICAL SOURCE typed(id UINT64 NOT NULL, number INT32 NOT NULL, flag BOOLEAN NOT NULL, millis UINT32 NOT NULL, maybe INT16);
CREATE PHYSICAL SOURCE FOR typed TYPE File SET('tests/testdata/typed_values.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK results(id UINT64 NOT NULL, number INT32 NOT NULL, flag BOOLEAN NOT NULL, pass_nullable INT16, wait_ms UINT32 NOT NULL, computed_ms UINT32 NOT NULL, nested_ms UINT32 NOT NULL) TYPE File SET('typed.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id,
       ASYNC_DELAY(number, millis) AS number,
       ASYNC_DELAY(flag, millis, 2) AS flag,
       ASYNC_DELAY(maybe, millis) AS pass_nullable,
       ASYNC_DELAY(millis, millis) AS wait_ms,
       ASYNC_DELAY(millis + 1, millis + 1) AS computed_ms,
       ASYNC_DELAY(millis, ASYNC_DELAY(millis, millis)) AS nested_ms
FROM typed INTO results;
