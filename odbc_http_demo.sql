CREATE LOGICAL SOURCE readings(id INT32 NOT NULL, ts UINT64 NOT NULL, reading FLOAT64, note VARSIZED);

CREATE PHYSICAL SOURCE FOR readings TYPE ODBC SET(
    'NATIVE' AS INPUT_FORMATTER."TYPE",
    'SELECT id, ts, reading, note FROM dbo.readings WHERE ts > ? ORDER BY ts' AS "SOURCE"."QUERY",
    '172.17.0.2' AS "SOURCE"."DB_HOST",
    '1433' AS "SOURCE"."DB_PORT",
    'master' AS "SOURCE"."DATABASE",
    'sa' AS "SOURCE"."USERNAME",
    'samplePassword1!' AS "SOURCE"."PASSWORD",
    'ODBC Driver 18 for SQL Server' AS "SOURCE"."DRIVER",
    'true' AS "SOURCE"."TRUST_SERVER_CERTIFICATE",
    '500' AS "SOURCE"."POLL_INTERVAL_MS"
);

CREATE SINK alerts(id INT32 NOT NULL, reading FLOAT64) TYPE HTTP SET(
    '172.17.0.3' AS "SINK".IP_ADDRESS,
    '8000' AS "SINK".PORT,
    'alerts' AS "SINK".ENDPOINT,
    '/tmp/httpsink.log' AS "SINK".LOG_FILE_PATH,
    'JSON' AS "SINK".OUTPUT_FORMAT
);

SELECT id, reading FROM readings WHERE reading > FLOAT64(38.0) INTO alerts;
