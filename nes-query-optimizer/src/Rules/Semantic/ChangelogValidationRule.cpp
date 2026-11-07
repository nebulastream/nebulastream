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

#include <Rules/Semantic/ChangelogValidationRule.hpp>

#include <algorithm>
#include <ranges>
#include <set>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Operators/EventTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/IngestionTimeWatermarkAssignerLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Operators/SelectionLogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Util/RowKind.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{
/// An unbounded aggregation withdraws the records of an updating input, if all its aggregations can subtract a record.
bool isRetractingAggregation(const LogicalOperator& op)
{
    const auto aggregation = op.tryGetAs<WindowedAggregationLogicalOperator>();
    if (not aggregation.has_value() or not aggregation.value()->getWindowType().isUnbounded())
    {
        return false;
    }
    return std::ranges::all_of(
        aggregation.value()->getWindowAggregation(),
        [](const WindowedAggregationLogicalOperator::ProjectedAggregation& projected)
        {
            const auto name = projected.function->getName();
            return name == "Sum" or name == "Count" or name == "Avg";
        });
}

bool acceptsUpdates(const LogicalOperator& op)
{
    return isRetractingAggregation(op) or op.tryGetAs<SelectionLogicalOperator>().has_value() or op.tryGetAs<ProjectionLogicalOperator>().has_value()
        or op.tryGetAs<UnionLogicalOperator>().has_value() or op.tryGetAs<SinkLogicalOperator>().has_value()
        or op.tryGetAs<EventTimeWatermarkAssignerLogicalOperator>().has_value()
        or op.tryGetAs<IngestionTimeWatermarkAssignerLogicalOperator>().has_value();
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan ChangelogValidationRule::apply(const LogicalPlan& queryPlan) const
{
    PlanVisitor<> visitor{
        [](const LogicalOperator& op, std::vector<LogicalOperator> children) -> PlanVisitor<>::UpResult
        {
            const auto hasUpdatingChild
                = std::ranges::any_of(children, [](const LogicalOperator& child) { return isUpdatingStream(child.getOutputSchema()); });
            if (hasUpdatingChild and not acceptsUpdates(op))
            {
                throw UnsupportedQuery("{} cannot consume an updating stream (an input carries {})", op.getName(), ROW_KIND_FIELD_NAME);
            }
            return op.withChildren(std::move(children));
        }};
    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> ChangelogValidationRule::needs() const
{
    return {typeid(TypeInferenceRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> ChangelogValidationRule::neededBy() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType ChangelogValidationRule::create(PlanRuleRegistryArguments)
{
    return ChangelogValidationRule{};
}

}
