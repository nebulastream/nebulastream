----------------------------------------------------
--|||||| RUNNING THE EXAMPLE FROM README.MD |||||||||
----------------------------------------------------
-- CREATE LOGICAL SOURCE demo(value UINT64);
-- CREATE PHYSICAL SOURCE FOR demo TYPE File SET('./demo-input.csv' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER);
-- CREATE SINK result(demo.value UINT64) TYPE File SET('./demo-output.csv' AS `SINK`.FILE_PATH, 'CSV' AS `SINK`.INPUT_FORMAT);
-- SELECT value FROM demo INTO result;

---------------------------------------------------------------------------------------------
--|||||| first try: a command to execute a python script with flush=true to be safe |||||||||
---------------------------------------------------------------------------------------------
-- CREATE LOGICAL SOURCE demo(value UINT64);
--
-- CREATE PHYSICAL SOURCE FOR demo TYPE LinuxProcess SET(
--   'python3 /home/saad9098/nebulastream/producer.py' AS `SOURCE`.command,
--   'CSV' AS PARSER.`TYPE`,
--   '\n' AS PARSER.TUPLE_DELIMITER,
--   ','  AS PARSER.FIELD_DELIMITER
-- );
--
-- CREATE SINK result(demo.value UINT64) TYPE File SET(
--   './demo-output.csv' AS `SINK`.FILE_PATH,
--   'CSV' AS `SINK`.INPUT_FORMAT
-- );
-- SELECT value FROM demo INTO result;

---------------------------------------------------------------------------------------------
--|||||| second try: a bash command |||||||||
---------------------------------------------------------------------------------------------
CREATE LOGICAL SOURCE demo(value UINT64);
CREATE PHYSICAL SOURCE FOR demo TYPE LinuxProcess SET(
  'sh -c "echo 1; echo 2; echo 3"' AS `SOURCE`.command,
  'CSV' AS PARSER.`TYPE`,
  '\n' AS PARSER.TUPLE_DELIMITER,
  ','  AS PARSER.FIELD_DELIMITER
);
CREATE SINK result(demo.value UINT64) TYPE File SET(
  './demo-output.csv' AS `SINK`.FILE_PATH,
  'CSV' AS `SINK`.INPUT_FORMAT
);
SELECT value FROM demo INTO result;
