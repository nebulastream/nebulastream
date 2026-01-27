CREATE LOGICAL SOURCE ourSource_fileSink(value UINT64);

CREATE PHYSICAL SOURCE FOR ourSource_fileSink TYPE LinuxProcess SET(
  'python3 ./producer.py' AS `SOURCE`.command,
  'CSV' AS PARSER.`TYPE`,
  '\n' AS PARSER.TUPLE_DELIMITER,
  ','  AS PARSER.FIELD_DELIMITER
);

CREATE SINK result(ourSource_fileSink.value UINT64) TYPE File SET(
  './ourSource_fileSink-output.csv' AS `SINK`.FILE_PATH,
  'CSV' AS `SINK`.INPUT_FORMAT
);
SELECT value FROM ourSource_fileSink INTO result;
