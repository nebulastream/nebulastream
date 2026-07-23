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

#include <Rules/Static/RedundantProjectionRemovalRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>


#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Static/ProjectionPushdownRule.hpp>
#include <Schema/Binder.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantProjectionRemovalRule::wants() const
{
    return {typeid(ProjectionPushdownRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantProjectionRemovalRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantProjectionRemovalRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

namespace
{
PlanVisitor<>::UpResult removeRedundantProjections(const LogicalOperator& op, const std::vector<LogicalOperator>& children)
{
    if (const auto projection = op.tryGetAs<ProjectionLogicalOperator>())
    {
        const auto trivial = [&]
        {
            INVARIANT(op.getChildren().size() == 1, "Projection operator must have exactly one child");
            for (const auto& [as, function] : projection.value()->getProjections())
            {
                const auto fieldAccessFunction = function.tryGetAs<FieldAccessLogicalFunction>();
                if (!fieldAccessFunction.has_value())
                {
                    return false;
                }
                if (fieldAccessFunction.value()->getField().getLastName() != as.getLastName())
                {
                    return false;
                }
            }
            return unbind(op.getChildren().front().getOutputSchema()) == unbind(op.getOutputSchema());
        }();
        if (trivial)
        {
            return children.at(0);
        }
    }
    return op.withChildren(children);
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan RedundantProjectionRemovalRule::apply(LogicalPlan queryPlan) const
{
    PlanVisitor<> visitor{removeRedundantProjections};
    return visitor.apply(std::move(queryPlan));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType RedundantProjectionRemovalRule::create(PlanRuleRegistryArguments)
{
    return RedundantProjectionRemovalRule{};
}

}
