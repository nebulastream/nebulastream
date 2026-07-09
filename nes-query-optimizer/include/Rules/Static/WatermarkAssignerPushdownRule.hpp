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
 * @brief Pushes watermark assigners as close to source operators as possible.
 *
 * Performs a top-down recursive traversal, accumulating two state components:
 * an `ingestionTime` flag and an `eventTime` list of (field, time-unit) pairs.
 * Watermark assigner nodes are dissolved into this state as the traversal descends and re-materialized when the recursion terminates.
 * - Sink: starts recursion with empty state.
 * - Source: recursion stops; all accumulated watermark assigners are materialized directly above the source.
 * - Event-time watermark assigner: node is consumed; its (field, unit) pair is appended to the accumulated eventTime list.
 * - Ingestion-time watermark assigner: node is consumed; the ingestionTime flag is set.
 * - Selection / Union: both state components pass through transparently.
 * - Projection: event-time assigners whose field passes through unrenamed are pushed further;
 *   assigners for renamed or computed fields are re-materialized above the projection.
 * - Unknown operators: all accumulated assigners are re-materialized above the operator;
 *   recursion restarts below with empty state.
 */
class WatermarkAssignerPushdownRule
{
public:
    static constexpr std::string_view NAME = "WatermarkAssignerPushdownRule";

    [[nodiscard]] std::set<std::type_index> wants() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
};

static_assert(RuleConcept<WatermarkAssignerPushdownRule, LogicalPlan>);
}
