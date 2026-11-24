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

-- Each path belongs to exactly one proof; counts use received POSTs, not timing.
CREATE LOGICAL SOURCE texts(id UINT64 NOT NULL, data VARSIZED NOT NULL);
CREATE PHYSICAL SOURCE FOR texts TYPE File SET('tests/testdata/texts.csv' AS "SOURCE".FILE_PATH, 'CSV' AS INPUT_FORMATTER."TYPE");
CREATE SINK duplicates(id UINT64 NOT NULL, left_response VARSIZED NOT NULL, right_response VARSIZED NOT NULL) TYPE File SET('duplicates.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK branches(id UINT64 NOT NULL, left_response VARSIZED NOT NULL, right_response VARSIZED NOT NULL) TYPE File SET('branches.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK predicate(id UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('predicate.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK unused(id UINT64 NOT NULL) TYPE File SET('unused.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK copies(id UINT64 NOT NULL, left_response VARSIZED NOT NULL, right_response VARSIZED NOT NULL) TYPE File SET('copies.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK barrier(id UINT64 NOT NULL) TYPE File SET('barrier.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK physical_alias(id UINT64 NOT NULL, original UINT64 NOT NULL, response VARSIZED NOT NULL) TYPE File SET('physical_alias.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
CREATE SINK field_collision(id UINT64 NOT NULL, response VARSIZED NOT NULL, actual VARSIZED NOT NULL) TYPE File SET('field_collision.csv' AS "SINK".FILE_PATH, 'CSV' AS "SINK".OUTPUT_FORMAT);
SELECT id, HTTP_CALL(data, 'HTTP_SERVER/occ-duplicate') AS left_response, HTTP_CALL(data, 'HTTP_SERVER/occ-duplicate') AS right_response FROM texts INTO duplicates;
SELECT id,
 HTTP_CALL(HTTP_CALL(data, 'HTTP_SERVER/occ-inner'), 'HTTP_SERVER/occ-outer') AS left_response,
 HTTP_CALL(HTTP_CALL(data, 'HTTP_SERVER/occ-inner'), 'HTTP_SERVER/occ-outer') AS right_response
FROM texts INTO branches;
SELECT id, HTTP_CALL(data, 'HTTP_SERVER/occ-predicate') AS response FROM texts
WHERE HTTP_CALL(data, 'HTTP_SERVER/occ-predicate') = '/occ-predicate:Hello' INTO predicate;
SELECT id FROM (SELECT id, HTTP_CALL(data, 'HTTP_SERVER/occ-unused') AS response FROM texts) INTO unused;
SELECT id, response AS left_response, response AS right_response FROM (SELECT id, HTTP_CALL(data, 'HTTP_SERVER/occ-copy') AS response FROM texts) INTO copies;
-- The pure outer filter must not suppress effects on the two rejected rows, even when the output is unused.
SELECT id FROM (SELECT id, HTTP_CALL(data, 'HTTP_SERVER/occ-barrier') AS response FROM texts) WHERE id = 1 INTO barrier;
-- ID is both read and overwritten upstream, requiring a distinct physical alias before handoff padding.
SELECT id, original, HTTP_CALL(data, 'HTTP_SERVER/occ-physical-alias') AS response
FROM (SELECT id + UINT64(10) AS id, id AS original, data FROM texts) INTO physical_alias;
-- The source's RESPONSE is also an argument. Ordinary projections preserve it beside the native result.
SELECT *, HTTP_CALL(response, 'HTTP_SERVER/occ-field-collision') AS actual
FROM (SELECT id, data AS response FROM texts) INTO field_collision;
