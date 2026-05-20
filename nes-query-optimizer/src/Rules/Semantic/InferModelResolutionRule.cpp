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

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>

#include <Operators/InferModelLogicalOperator.hpp>
#include <Operators/InferModelNameLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/OriginIdInferenceRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

LogicalPlan InferModelResolutionRule::apply(LogicalPlan queryPlan) const
{
    for (const auto& inferModelNameOp : getOperatorByType<InferModelNameLogicalOperator>(queryPlan))
    {
        const auto& op = inferModelNameOp.get();
        const auto& modelName = op.getModelName();

        if (!modelCatalog->hasModel(modelName))
        {
            throw UnknownModelName("Model '{}' is not registered", modelName);
        }

        auto loaded = modelCatalog->load(modelName);
        auto inferModelOp = TypedLogicalOperator<InferModelLogicalOperator>{std::move(loaded)};
        /// Preserve children from the original operator
        auto replacement = LogicalOperator{inferModelOp->withChildren(op.getChildren())};

        auto replaceResult = replaceSubtree(queryPlan, inferModelNameOp.getId(), replacement);
        INVARIANT(replaceResult.has_value(), "Failed to replace InferModelNameLogicalOperator with InferModelLogicalOperator");
        queryPlan = std::move(replaceResult.value());
    }
    return queryPlan;
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
    return {typeid(LogicalSourceExpansionRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> InferModelResolutionRule::requiredBy() const
{
    return {typeid(TypeInferenceRule), typeid(OriginIdInferenceRule)};
}

bool InferModelResolutionRule::operator==(const InferModelResolutionRule& other) const
{
    return modelCatalog == other.modelCatalog;
}

}
