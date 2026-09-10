--  Licensed under the Apache License, Version 2.0 (the "License");
--  you may not use this file except in compliance with the License.
--  You may obtain a copy of the License at
--
--      https://www.apache.org/licenses/LICENSE-2.0
--
--  Unless required by applicable law or agreed to in writing, software
--  distributed under the License is distributed on an "AS IS" BASIS,
--  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  See the License for the specific language governing permissions and
--  limitations under the License.

DROP TRIGGER IF EXISTS cascade_query_failed_to_query_fragments;
DROP TRIGGER IF EXISTS derive_query_state_on_query_fragment_update;
DROP TRIGGER IF EXISTS release_query_fragment_capacity;
DROP TRIGGER IF EXISTS release_capacity_on_query_fragment_delete;
DROP TRIGGER IF EXISTS acquire_worker_capacity;
DROP TRIGGER IF EXISTS validate_query_state_transition;
DROP TRIGGER IF EXISTS validate_query_fragment_state_transition;
DROP TRIGGER IF EXISTS cascade_worker_removed_to_query_fragments;
DROP TRIGGER IF EXISTS prevent_physical_source_drop_with_active_queries;
DROP TRIGGER IF EXISTS prevent_sink_drop_with_active_queries;
DROP TRIGGER IF EXISTS cleanup_orphaned_source;
DROP TRIGGER IF EXISTS cleanup_orphaned_sink;
