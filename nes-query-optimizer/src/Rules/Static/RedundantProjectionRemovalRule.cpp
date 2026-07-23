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
#include <Rules/Static/DecideFieldMappings.hpp>
#include <Rules/Static/DecideFieldOrder.hpp>
#include <Schema/Binder.hpp>
#include <ErrorHandling.hpp>
#include <PlanRewriteUtils.hpp>

namespace NES
{

const std::type_info& RedundantProjectionRemovalRule::getType()
{
    return typeid(RedundantProjectionRemovalRule);
}

std::string_view RedundantProjectionRemovalRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantProjectionRemovalRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantProjectionRemovalRule::requiredBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

bool RedundantProjectionRemovalRule::operator==(const RedundantProjectionRemovalRule&) const
{
    return true;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan RedundantProjectionRemovalRule::apply(LogicalPlan queryPlan) const
{
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    /// No upfront deep schema inference: withChildren re-infers each operator's local schema bottom-up,
    /// while a recursive withInferredSchema would copy shared subtrees per parent.
    return rewritePlanBottomUp(
        queryPlan,
        [](const LogicalOperator& op, std::vector<LogicalOperator> newChildren) -> LogicalOperator
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
                    return newChildren.front();
                }
            }
            return op.withChildren(std::move(newChildren));
        });
}

}
