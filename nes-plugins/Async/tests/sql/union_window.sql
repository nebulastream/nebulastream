-- Multi-origin stream (UNION) through an async stage into a tumbling window flushes every origin's final watermark at EoS.
CREATE LOGICAL SOURCE left_stream(val UINT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR left_stream TYPE File SET('left.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE LOGICAL SOURCE right_stream(val UINT64 NOT NULL, ts UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR right_stream TYPE File SET('right.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK windowed(max_val UINT64 NOT NULL) TYPE File SET('windowed.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT MAX(delayed) AS max_val
FROM (
    SELECT ASYNC_DELAY(val, 1) AS delayed, ts
    FROM (
        SELECT * FROM left_stream UNION SELECT * FROM right_stream
    )
)
WINDOW TUMBLING(ts, size 1 sec)
INTO windowed;
