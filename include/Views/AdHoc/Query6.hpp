/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_QUERY6_HPP
#define NES_QUERY6_HPP

#include <Runtime/Execution/PipelineExecutionContext.hpp>

/**
 * Adhoc Query 6
 * (SELECT entity_id FROM wide_table WHERE a_34 = (SELECT MAX(a_34) FROM wide_table WHERE country_id=$1) LIMIT 1)
 * UNION
 * (SELECT entity_id FROM wide_table WHERE a_41 = (SELECT MAX(a_41) FROM wide_table WHERE country_id=$1) LIMIT 1)
 * UNION
 * (SELECT entity_id FROM wide_table WHERE a_13 = (SELECT MAX(a_13) FROM wide_table WHERE country_id=$1) LIMIT 1)
 * UNION
 * (SELECT entity_id FROM wide_table WHERE a_20 = (SELECT MAX(a_20) FROM wide_table WHERE country_id=$1) LIMIT 1)
 *
 * report the entity-ids of the records with the longest call
 * this day and this week for local and long distance calls for
 * a specific country cty
 */

#endif //NES_QUERY6_HPP
