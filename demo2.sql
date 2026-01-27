CREATE LOGICAL SOURCE single5(id UINT64, status VARSIZED, value UINT64,type VARSIZED);

CREATE PHYSICAL SOURCE FOR single5 TYPE LinuxProcess SET(
  './nes-systests/sources/bashjson.sh' AS `SOURCE`.command,
  'JSON' AS PARSER.`TYPE`,
  '\n' AS PARSER.TUPLE_DELIMITER,
  ','  AS PARSER.FIELD_DELIMITER
);

CREATE SINK result9(
  single5.id UINT64,
  single5.status VARSIZED,
  single5.value UINT64,
  single5.type VARSIZED
) TYPE File SET('./demo-output2.csv' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT);
SELECT id, status, value, type FROM single5 INTO result9;

--python3 ./nes-systests/sources/LinuxProcess_Source_JSON.py