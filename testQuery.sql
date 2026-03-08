CREATE LOGICAL SOURCE heartRates(creationTS UINT64, patientId UINT32, heartRate FLOAT32);

CREATE PHYSICAL SOURCE FOR heartRates TYPE FILE SET(
       '/tmp/nebulastream/nes-systests/testdata/small/matrix/heartrates.csv' AS `SOURCE`.`FILE_PATH`,
       'CSV' as `PARSER`.`TYPE`
);

CREATE SINK highrates2(HEARTRATES.CREATIONTS UINT64, HEARTRATES.PATIENTID UINT32, HEARTRATES.HEARTRATE FLOAT32) TYPE HTTP SET(
       '3000' as `SINK`.PORT,
       'JSON' as `SINK`.INPUT_FORMAT,
       'host.docker.internal' as `SINK`.`IP_ADDRESS`,
       'nesNotification' as `SINK`.ENDPOINT
);

SELECT * FROM HEARTRATES WHERE HEARTRATE > FLOAT32(100) INTO HIGHRATES;

CREATE LOGICAL SOURCE heartRates(creationTS UINT64, patientId UINT32, heartRate FLOAT32);
CREATE PHYSICAL SOURCE FOR heartRates TYPE TCP SET(
       'host.docker.internal' AS `SOURCE`.`socket_host`,
       5000 AS `SOURCE`.`socket_port`,
        65536 as `SOURCE`.SOCKET_BUFFER_SIZE,
        100 as `SOURCE`.FLUSH_INTERVAL_MS,
        60 as `SOURCE`.CONNECT_TIMEOUT_SECONDS,
        'CSV' as PARSER.`TYPE`,
        '\n' as PARSER.TUPLE_DELIMITER,
        ',' as PARSER.FIELD_DELIMITER
);
CREATE SINK highrates2(HEARTRATES.CREATIONTS UINT64, HEARTRATES.PATIENTID UINT32, HEARTRATES.HEARTRATE FLOAT32) TYPE HTTP SET(
       '3000' as `SINK`.PORT,
       'JSON' as `SINK`.INPUT_FORMAT,
       'host.docker.internal' as `SINK`.`IP_ADDRESS`,
       'nesNotification' as `SINK`.ENDPOINT
);
SELECT * FROM HEARTRATES INTO HIGHRATES2;

CREATE LOGICAL SOURCE heartRates(creationTS UINT64, patientId UINT32, heartRate FLOAT32);
CREATE PHYSICAL SOURCE FOR heartRates TYPE TCP SET(
       'host.docker.internal' AS `SOURCE`.`socket_host`,
       5000 AS `SOURCE`.`socket_port`,
        65536 as `SOURCE`.SOCKET_BUFFER_SIZE,
        100 as `SOURCE`.FLUSH_INTERVAL_MS,
        60 as `SOURCE`.CONNECT_TIMEOUT_SECONDS,
        'JSON' as PARSER.`TYPE`,
       "1" as LLM_QUERY_ID
);
CREATE SINK highrates2(HEARTRATES.CREATIONTS UINT64, HEARTRATES.PATIENTID UINT32, HEARTRATES.HEARTRATE FLOAT32) TYPE HTTP SET(
       '3000' as `SINK`.PORT,
       'JSON' as `SINK`.INPUT_FORMAT,
       'host.docker.internal' as `SINK`.`IP_ADDRESS`,
       'nesNotification' as `SINK`.ENDPOINT
);
SELECT * FROM HEARTRATES INTO HIGHRATES2;

-- -----
CREATE LOGICAL SOURCE heartRates(creationTS UINT64, patientId UINT32, description VARSIZED);

CREATE PHYSICAL SOURCE FOR heartRates TYPE FILE SET(
       '/tmp/nebulastream/nes-systests/testdata/small/matrix/llm.csv' AS `SOURCE`.`FILE_PATH`,
       'CSV' as `PARSER`.`TYPE`
);

CREATE SINK highrates2(UID VARSIZED, CREATIONTS UINT64, PATIENTID UINT32, DESCRIPTION VARSIZED) TYPE HTTP SET(
       '3000' as `SINK`.PORT,
       'JSON' as `SINK`.INPUT_FORMAT,
       'host.docker.internal' as `SINK`.`IP_ADDRESS`,
       'nesNotification' as `SINK`.ENDPOINT
);

SELECT VARSIZED("1") as UID, CREATIONTS, PATIENTID, DESCRIPTION FROM HEARTRATES INTO HIGHRATES2;
CREATE SINK highrates2(VARSIZED UINT64, HEARTRATES.CREATIONTS UINT64, HEARTRATES.PATIENTID UINT32, HEARTRATES.DESCRIPTION VARSIZED) TYPE HTTP SET(
       '3000' as `SINK`.PORT,
       'JSON' as `SINK`.INPUT_FORMAT,
       'host.docker.internal' as `SINK`.`IP_ADDRESS`,
       'operator' as `SINK`.ENDPOINT
);

-- -----
CREATE LOGICAL SOURCE heartRates(creationTS UINT64, patientId UINT32, description VARSIZED);

CREATE PHYSICAL SOURCE FOR heartRates TYPE FILE SET(
       '/tmp/nebulastream/nes-systests/testdata/small/matrix/llm.csv' AS `SOURCE`.`FILE_PATH`,
       'CSV' as `PARSER`.`TYPE`
);

SELECT VARSIZED("1") as query_id, CREATIONTS, PATIENTID, DESCRIPTION FROM HEARTRATES INTO HTTP(
       '3000' as `SINK`.PORT,
       'JSON' as `SINK`.INPUT_FORMAT,
       'host.docker.internal' as `SINK`.`IP_ADDRESS`,
       'operator' as `SINK`.ENDPOINT
);

CREATE LOGICAL SOURCE LLM_ANSWER(ID VARSIZED, ts VARSIZED, SENTIMENT VARSIZED, creationTS UINT64, patientId UINT32, description VARSIZED);

CREATE PHYSICAL SOURCE FOR LLM_ANSWER TYPE TCP SET(
       'host.docker.internal' AS `SOURCE`.`socket_host`,
        5000 AS `SOURCE`.`socket_port`,
        4096 as `SOURCE`.SOCKET_BUFFER_SIZE,
        20 as `SOURCE`.FLUSH_INTERVAL_MS,
        1000 as `SOURCE`.CONNECT_TIMEOUT_SECONDS,
        "1" as `SOURCE`.`LLM_QUERY_ID`,
        'CSV' as PARSER.`TYPE`,
        '\n' as PARSER.TUPLE_DELIMITER,
        ',' as PARSER.FIELD_DELIMITER
);

SELECT * FROM LLM_ANSWER INTO FILE(
'/tmp/nebulastream/nes-systests/testdata/small/matrix/llm_answer2.csv' AS `SINK`.`FILE_PATH`,
'CSV' AS `SINK`.INPUT_FORMAT
);


CREATE LOGICAL SOURCE heartRates(creationTS UINT64, patientId UINT32, description VARSIZED);

CREATE PHYSICAL SOURCE FOR heartRates TYPE FILE SET(
       '/tmp/nebulastream/nes-systests/testdata/small/matrix/llm.csv' AS `SOURCE`.`FILE_PATH`,
       'CSV' as `PARSER`.`TYPE`
);

SELECT VARSIZED("1") as query_id, CREATIONTS, PATIENTID, DESCRIPTION FROM HEARTRATES INTO FILE(
'/tmp/nebulastream/nes-systests/testdata/small/matrix/llm_answer.csv' AS `SINK`.`FILE_PATH`,
'CSV' AS `SINK`.INPUT_FORMAT
);
SHOW QUERIES;