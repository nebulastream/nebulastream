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

#include <LegacyOptimizer/TypeInferencePhase.hpp>

#include <vector>
#include <DataTypes/Schema.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>

namespace NES::LegacyOptimizer
{

static LogicalOperator propagateSchema(const LogicalOperator& op)
{
    const std::vector<LogicalOperator> children = op.getChildren();

    if (children.empty())
    {
        return op;
    }

    std::vector<LogicalOperator> newChildren;
    std::vector<Schema> childSchemas;
    for (const auto& child : children)
    {
        const LogicalOperator childWithSchema = propagateSchema(child);
        childSchemas.push_back(childWithSchema.getOutputSchema());
        newChildren.push_back(childWithSchema);
    }

    const LogicalOperator updatedOperator = op.withChildren(newChildren);
    return updatedOperator.withInferredSchema(childSchemas);
}

void TypeInferencePhase::apply(LogicalPlan& queryPlan) const /// NOLINT(readability-convert-member-functions-to-static)
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& sink : queryPlan.getRootOperators())
    {
        const LogicalOperator inferredRoot = propagateSchema(sink);
        newRoots.push_back(inferredRoot);
    }
    queryPlan = queryPlan.withRootOperators(newRoots);
}

}
