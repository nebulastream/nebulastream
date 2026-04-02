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

#include <Rules/Semantic/TypeInferenceRule.hpp>

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/InlineSinkBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>

namespace NES
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

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan TypeInferenceRule::apply(const LogicalPlan& queryPlan) const
{
    std::vector<LogicalOperator> newRoots;
    for (const auto& sink : queryPlan.getRootOperators())
    {
        const LogicalOperator inferredRoot = propagateSchema(sink);
        newRoots.push_back(inferredRoot);
    }
    return queryPlan.withRootOperators(newRoots);
}

const std::type_info& TypeInferenceRule::getType()
{
    return typeid(TypeInferenceRule);
}

std::string_view TypeInferenceRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> TypeInferenceRule::dependsOn() const
{
    return {typeid(LogicalSourceExpansionRule), typeid(SinkBindingRule), typeid(InlineSinkBindingRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> TypeInferenceRule::requiredBy() const
{
    return {};
}

bool TypeInferenceRule::operator==(const TypeInferenceRule&) const
{
    return true;
}


}
