/*
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

#pragma once

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>

#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>

namespace NES
{

/**
 * @brief Eliminates unused fields as close to source operators as possible.
 *
 * Performs a top-down recursive traversal, threading a required field set through each operator:
 * - Sink: seeds required from the child output schema.
 * - Source: recursion stops; a projection is inserted above the source when required is a strict subset of the output schema.
 * - Selection: extends required with all fields accessed by the predicate.
 * - Projection: prunes entries not in required; expands required via the accessed fields of retained projection expressions.
 * - Join: partitions required and join-predicate fields into left/right subsets and recurses independently into each child.
 * - Union: propagates required into each branch, re-mapping field identities per child schema.
 * - Event-time watermark assigner: extends required with the event-time field.
 * - Ingestion-time watermark assigner: passes required through transparently.
 * - Windowed aggregation: retains only required aggregations; seeds required with their input fields and all grouping keys.
 * - Unknown operators: recurses on each child with its full output schema; inserts a projection for required fields above the operator.
 */
class ProjectionPushdownRule
{
public:
    ProjectionPushdownRule() = default;

    static constexpr std::string_view NAME = "ProjectionPushdownRule";

    [[nodiscard]] std::set<std::type_index> wants() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
};

static_assert(RuleConcept<ProjectionPushdownRule, LogicalPlan>);
}
