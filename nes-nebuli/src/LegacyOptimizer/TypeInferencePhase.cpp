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

#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <LegacyOptimizer/TypeInferencePhase.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <SourceCatalogs/SourceCatalog.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::LegacyOptimizer
{

LogicalOperator propagateSchema(const LogicalOperator& op)
{
    std::vector<LogicalOperator> children = op.getChildren();

    if (children.empty())
    {
        return op;
    }

    std::vector<LogicalOperator> newChildren;
    std::vector<Schema> childSchemas;
    for (const auto& child : children)
    {
        LogicalOperator childWithSchema = propagateSchema(child);
        childSchemas.push_back(childWithSchema.getOutputSchema());
        newChildren.push_back(childWithSchema);
    }

    LogicalOperator updatedOperator = op.withChildren(newChildren);
    return updatedOperator.withInferredSchema(childSchemas);
}

void TypeInferencePhase::apply(LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& sink : queryPlan.rootOperators)
    {
        LogicalOperator inferredRoot = propagateSchema(sink);
        newRoots.push_back(inferredRoot);
    }
    queryPlan.rootOperators = newRoots;
}

}
