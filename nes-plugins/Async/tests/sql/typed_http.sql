CREATE LOGICAL SOURCE typed_http(id UINT64 NOT NULL, body VARSIZED NOT NULL, timeout_ms UINT16 NOT NULL);
CREATE PHYSICAL SOURCE FOR typed_http TYPE File SET('tests/testdata/typed_http.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK results(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('typed_http.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, HTTP_CALL(body, 'HTTP_SERVER/echo', timeout_ms + 1) AS response FROM typed_http INTO results;
