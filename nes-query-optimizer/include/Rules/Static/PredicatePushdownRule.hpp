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

#include <Functions/LogicalFunction.hpp>
#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>

namespace NES
{

/**
 * @brief Pushes filter predicates (selections) as close to source operators as possible.
 *
 * Performs a top-down recursive traversal, accumulating conjuncts from encountered
 * `SelectionLogicalOperator` nodes (split on AND) and routing them through each operator type:
 * - Source: recursion stops, since sources have no children. all remaining predicates are applied.
 * - Union: full predicate set is duplicated into every branch.
 * - Projection: only conjuncts whose fields pass through unrenamed are pushed below.
 * - Join: left-only / right-only conjuncts are pushed to the respective child; cross-input
 *   conjuncts remain above the join.
 * - Windowed aggregation: only conjuncts on grouping keys are pushed below.
 * - Watermark assigners: predicates pass through transparently.
 * - Unknown operators: All predicates are applied before operator,
 *   predicate pushdown is restarted for its children.
 *
 *  Predicates that can no longer be pushed are re-materialised as a `SelectionLogicalOperator`
 *  above the blocking operator.
 *
 */
class PredicatePushdownRule
{
public:
    explicit PredicatePushdownRule() = default;

    static constexpr std::string_view NAME = "PredicatePushdownRule";

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;
    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    bool operator==(const PredicatePushdownRule& other) const;

private:
    [[nodiscard]] LogicalOperator predicatePushdown(LogicalOperator op, std::vector<LogicalFunction> predicateSet) const;

    [[nodiscard]] LogicalOperator
    pushBeyondSelection(TypedLogicalOperator<SelectionLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const;
    [[nodiscard]] LogicalOperator
    pushBeyondUnion(TypedLogicalOperator<UnionLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const;
    [[nodiscard]] LogicalOperator
    pushBeyondProjection(TypedLogicalOperator<ProjectionLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const;
    [[nodiscard]] LogicalOperator
    pushBeyondJoin(TypedLogicalOperator<JoinLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const;
    [[nodiscard]] LogicalOperator pushBeyondIngestionTimeWatermarkAssigner(
        TypedLogicalOperator<IngestionTimeWatermarkAssignerLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const;
    [[nodiscard]] LogicalOperator pushBeyondEventTimeWatermarkAssigner(
        TypedLogicalOperator<EventTimeWatermarkAssignerLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const;
    [[nodiscard]] LogicalOperator pushBeyondWindowedAggregation(
        TypedLogicalOperator<WindowedAggregationLogicalOperator> op, std::vector<LogicalFunction> predicateSet) const;

    [[nodiscard]] std::vector<LogicalFunction> splitPredicate(LogicalFunction function) const;
    [[nodiscard]] LogicalOperator applyToAllChildren(LogicalOperator op, std::vector<LogicalFunction> predicateSet) const;
    [[nodiscard]] LogicalOperator addSelectionIfRequired(LogicalOperator op, std::vector<LogicalFunction> predicateSet) const;
    [[nodiscard]] std::vector<std::string> getAccessedFields(LogicalFunction logicalFunction) const;
};

static_assert(RuleConcept<PredicatePushdownRule, LogicalPlan>);
}
