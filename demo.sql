CREATE LOGICAL SOURCE demo(id UINT64,
  status VARSIZED,
  value UINT64,
  type VARSIZED
);

CREATE PHYSICAL SOURCE FOR demo TYPE LinuxProcess SET(
  'python3 /home/saad9098/nebulastream/producer2.py' AS `SOURCE`.command,
  'CSV' AS PARSER.`TYPE`,
  '\n' AS PARSER.TUPLE_DELIMITER,
  ','  AS PARSER.FIELD_DELIMITER
);

CREATE SINK result(
  demo.id UINT64,
  demo.status VARSIZED,
  demo.value UINT64,
  demo.type VARSIZED
) TYPE File SET(
  './demo2-output.csv' AS `SINK`.FILE_PATH,
  'CSV' AS `SINK`.INPUT_FORMAT
);

SELECT id, status, value, type FROM demo INTO result;
