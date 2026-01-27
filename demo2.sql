CREATE LOGICAL SOURCE single2(id UINT64, value UINT64, timestamp UINT64);

CREATE PHYSICAL SOURCE FOR single2 TYPE LinuxProcess SET(
  'python3 ./nes-systests/sources/LinuxProcess_Single2.py' AS `SOURCE`.command,
  'CSV' AS PARSER.`TYPE`,
  '\n' AS PARSER.TUPLE_DELIMITER,
  ','  AS PARSER.FIELD_DELIMITER
);

CREATE SINK result(single2.id UINT64, single2.value UINT64, single2.timestamp UINT64) TYPE File SET('./demo-output.csv' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT);
SELECT * FROM single2 INTO result;
