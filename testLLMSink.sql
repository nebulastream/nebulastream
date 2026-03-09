CREATE LOGICAL SOURCE heartRates(creationTS UINT64, patientId UINT32, description VARSIZED);

CREATE PHYSICAL SOURCE FOR heartRates TYPE FILE SET(
       '/tmp/nebulastream/nes-systests/testdata/small/matrix/llm.csv' AS `SOURCE`.`FILE_PATH`,
       'CSV' as `PARSER`.`TYPE`
);

SELECT VARSIZED("3") as query_id, CREATIONTS, PATIENTID, DESCRIPTION FROM HEARTRATES INTO LLM(
       '3000' as `SINK`.PORT,
       'JSON' as `SINK`.INPUT_FORMAT,
       'host.docker.internal' as `SINK`.`IP_ADDRESS`,
       'operator' as `SINK`.`ENDPOINT`,
       '{
          "query_id": "3",
          "type": "SEM_MAP",
          "config": {
            "prompt_template": "Classify sentiment of the following text:{HEARTRATES$DESCRIPTION}. Response only in JSON format. Example:{{\"sentiment\": \"negative\"}}",
            "output_column": "sentiment",
            "depends_on": ["HEARTRATES$DESCRIPTION"]
          },
          "model": "ollama"
        }' as `SINK`.`QUERY`
);
CREATE LOGICAL SOURCE LLM_ANSWER(ID VARSIZED, ts VARSIZED, SENTIMENT VARSIZED, creationTS UINT64, patientId UINT32, description VARSIZED);

CREATE PHYSICAL SOURCE FOR LLM_ANSWER TYPE LLMR SET(
       'host.docker.internal' AS `SOURCE`.`socket_host`,
        5000 AS `SOURCE`.`socket_port`,
        4096 as `SOURCE`.SOCKET_BUFFER_SIZE,
        20 as `SOURCE`.FLUSH_INTERVAL_MS,
        1000 as `SOURCE`.CONNECT_TIMEOUT_SECONDS,
        '3' as `SOURCE`.`LLM_QUERY_ID`,
        'CSV' as PARSER.`TYPE`,
        '\n' as PARSER.TUPLE_DELIMITER,
        ',' as PARSER.FIELD_DELIMITER
);

SELECT * FROM LLM_ANSWER INTO FILE(
'/tmp/nebulastream/nes-systests/testdata/small/matrix/llm_answer2.csv' AS `SINK`.`FILE_PATH`,
'CSV' AS `SINK`.INPUT_FORMAT
);