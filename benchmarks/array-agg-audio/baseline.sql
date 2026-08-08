SELECT OCTET_LENGTH(ARRAY_AGG(CASTTOTYPE(sample AS FLOAT32))) AS bytes
FROM (
    SELECT timestamp / UINT64(1000) AS timestamp, sample
    FROM File(
        '/home/ls/dima/nebulastream/benchmarks/array-agg-audio/audio-44khz-1gb.csv' AS "SOURCE".FILE_PATH,
        'localhost:8080' AS "SOURCE"."HOST",
        'CSV' AS INPUT_FORMATTER."TYPE",
        SCHEMA(sample FLOAT64 NOT NULL, timestamp UINT64 NOT NULL) AS "SOURCE"."SCHEMA")
)
WINDOW SLIDING(timestamp, SIZE 1000000 MS, ADVANCE BY 10000 MS)
INTO Void('localhost:8080' AS "SINK"."HOST");
