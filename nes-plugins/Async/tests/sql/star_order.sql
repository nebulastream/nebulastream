-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     https://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Declaration order intentionally differs from lexical order (DATA before ID).
-- Positional sinks make reordered columns or leaked normalization helpers observable.
CREATE LOGICAL SOURCE texts(ID UINT64 NOT NULL, DATA VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK appended(ID UINT64 NOT NULL, DATA VARSIZED NOT NULL, RESPONSE VARSIZED NOT NULL) TYPE File SET('star_appended.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK filtered(ID UINT64 NOT NULL, DATA VARSIZED NOT NULL) TYPE File SET('star_filtered.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK reordered(Z_PAYLOAD VARSIZED NOT NULL, A_ROW_ID UINT64 NOT NULL, RESPONSE VARSIZED NOT NULL) TYPE File SET('star_reordered.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK reordered_filtered(Z_PAYLOAD VARSIZED NOT NULL, A_ROW_ID UINT64 NOT NULL) TYPE File SET('star_reordered_filtered.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT *, HTTP_CALL(DATA, 'HTTP_SERVER/star-append') FROM texts INTO appended;
SELECT * FROM texts WHERE HTTP_CALL(DATA, 'HTTP_SERVER/star-where') = '/star-where:Hello' INTO filtered;
SELECT *, HTTP_CALL(Z_PAYLOAD, 'HTTP_SERVER/star-reordered') AS RESPONSE
FROM (SELECT DATA AS Z_PAYLOAD, ID AS A_ROW_ID FROM texts) INTO reordered;
SELECT * FROM (SELECT DATA AS Z_PAYLOAD, ID AS A_ROW_ID FROM texts)
WHERE HTTP_CALL(Z_PAYLOAD, 'HTTP_SERVER/star-reordered-where') = '/star-reordered-where:Hello' INTO reordered_filtered;
