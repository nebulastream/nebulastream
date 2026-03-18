CREATE LOGICAL SOURCE fileSource_jsonSink(value UINT64);
CREATE PHYSICAL SOURCE FOR fileSource_jsonSink TYPE File SET(
    './nes-plugins/Sinks/LinuxProcessSink/tests/demo-input.csv' AS `SOURCE`.FILE_PATH,
    'CSV' AS PARSER.`TYPE`,
    '\n' AS PARSER.TUPLE_DELIMITER,
    ',' AS PARSER.FIELD_DELIMITER
);
CREATE SINK result(fileSource_jsonSink.value UINT64) TYPE LinuxProcess SET(
  'python3 ./nes-plugins/Sinks/LinuxProcessSink/tests/consumer.py' AS `SINK`.command,
  'JSON' AS `SINK`.INPUT_FORMAT
);

SELECT value FROM fileSource_jsonSink INTO result;
