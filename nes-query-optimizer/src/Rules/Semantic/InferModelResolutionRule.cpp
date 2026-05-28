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

#include <Rules/Semantic/InferModelResolutionRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/InferModelNameLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/InlineSinkBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/OriginIdInferenceRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>

namespace NES
{

namespace
{
LogicalOperator recur(const LogicalOperator& op, const ModelCatalog& modelCatalog)
{
    auto newChildren = op.getChildren() | std::views::transform([&](const auto& child) { return recur(child, modelCatalog); })
        | std::ranges::to<std::vector>();

    if (const auto inferModelName = op.tryGetAs<InferModelNameLogicalOperator>())
    {
        const auto& modelName = inferModelName->get().getModelName();
        if (!modelCatalog.hasModel(modelName))
        {
            throw UnknownModelName("Model '{}' is not registered", modelName);
        }
        PRECONDITION(
            std::ranges::size(newChildren) == 1,
            "Expected InferModelName Logical Operator to have one child, but has {}",
            std::ranges::size(newChildren));
        return TypedLogicalOperator<InferModelLogicalOperator>{modelCatalog.load(modelName), std::move(newChildren.at(0))};
    }
    return op.withChildren(std::move(newChildren));
}
}

LogicalPlan InferModelResolutionRule::apply(const LogicalPlan& queryPlan) const
{
    PRECONDITION(queryPlan.getRootOperators().size() == 1, "Query plan must have exactly one root operator");
    return queryPlan.withRootOperators({recur(queryPlan.getRootOperators().front(), *modelCatalog)});
}

const std::type_info& InferModelResolutionRule::getType()
{
    return typeid(InferModelResolutionRule);
}

std::string_view InferModelResolutionRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> InferModelResolutionRule::dependsOn() const
{
    return {typeid(LogicalSourceExpansionRule), typeid(SinkBindingRule), typeid(InlineSinkBindingRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> InferModelResolutionRule::requiredBy() const
{
    return {typeid(TypeInferenceRule)};
}

bool InferModelResolutionRule::operator==(const InferModelResolutionRule& other) const
{
    return modelCatalog == other.modelCatalog;
}

}
