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

#include <LegacyOptimizer/RedundantProjectionRemovalRule.hpp>

#include <ranges>
#include <utility>
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

void RedundantProjectionRemovalRule::apply(LogicalPlan& queryPlan) const ///NOLINT(readability-convert-member-functions-to-static)
{
    for (const auto& projectionOp :
         getOperatorByType<ProjectionLogicalOperator>(queryPlan)
             | std::views::filter(
                 [](const auto& op)
                 {
                     INVARIANT(op.getChildren().size() == 1, "Projection operator must have exactly one child");
                     return op.getChildren().front().getOutputSchema() == op.getOutputSchema();
                 }))
    {
        auto child = projectionOp.getChildren().front();
        auto replaceResult = replaceSubtree(queryPlan, projectionOp.getId(), child);
        INVARIANT(replaceResult.has_value(), "Failed to replace projection with its child");
        queryPlan = std::move(replaceResult.value());
    }
}

}
