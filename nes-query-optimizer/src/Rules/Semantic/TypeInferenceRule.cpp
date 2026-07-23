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
#include <utility>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/AnonymousSinkBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <PlanRewriteUtils.hpp>

namespace NES
{


/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan TypeInferenceRule::apply(const LogicalPlan& queryPlan) const
{
    /// Infer bottom-up instead of calling withInferredSchema on each root: the recursive withInferredSchema
    /// would copy operators shared between multiple parents once per parent. withChildren re-infers the local
    /// schema, which is equivalent once all children are already inferred.
    return rewritePlanBottomUp(
        queryPlan,
        [](const LogicalOperator& op, std::vector<LogicalOperator> children) -> LogicalOperator
        {
            if (children.empty())
            {
                return op.withInferredSchema();
            }
            return op.withChildren(std::move(children));
        });
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
    return {typeid(LogicalSourceExpansionRule), typeid(SinkBindingRule), typeid(AnonymousSinkBindingRule)};
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
