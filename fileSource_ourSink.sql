CREATE LOGICAL SOURCE fileSource_ourSink(value UINT64);
CREATE PHYSICAL SOURCE FOR fileSource_ourSink TYPE File SET('./demo-input.csv' AS `SOURCE`.FILE_PATH, 'CSV' AS PARSER.`TYPE`, '\n' AS PARSER.TUPLE_DELIMITER, ',' AS PARSER.FIELD_DELIMITER);
CREATE SINK result(fileSource_ourSink.value UINT64) TYPE LinuxProcess SET(
  'python3 ./consumer.py' AS `SINK`.command,
  'CSV' AS `SINK`.INPUT_FORMAT
);

SELECT value FROM fileSource_ourSink INTO result;
