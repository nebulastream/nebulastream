CREATE LOGICAL SOURCE demo(value UINT64);
CREATE PHYSICAL SOURCE FOR demo TYPE File SET(
    './nes-plugins/Sinks/LinuxProcessSink/tests/demo-input.csv' AS `SOURCE`.FILE_PATH,
    'CSV' AS PARSER.`TYPE`,
    '\n' AS PARSER.TUPLE_DELIMITER,
    ',' AS PARSER.FIELD_DELIMITER
);

CREATE PHYSICAL SOURCE FOR demo TYPE File SET(
    './nes-plugins/Sinks/LinuxProcessSink/tests/demo-input.csv' AS `SOURCE`.FILE_PATH,
    'CSV' AS PARSER.`TYPE`,
    '\n' AS PARSER.TUPLE_DELIMITER,
    ',' AS PARSER.FIELD_DELIMITER
);

CREATE SINK result(demo.value UINT64) TYPE LinuxProcess SET(
  'python3 ./nes-plugins/Sinks/LinuxProcessSink/tests/consumer.py' AS `SINK`.command,
  'CSV' AS `SINK`.INPUT_FORMAT
);

SELECT value FROM demo INTO result;
