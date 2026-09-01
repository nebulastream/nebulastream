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
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/AnonymousSinkBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{


LogicalPlan InferModelResolutionRule::apply(const LogicalPlan& queryPlan) const
{
    PlanVisitor<> visitor{
        [this](const LogicalOperator& op, std::vector<LogicalOperator> children) -> PlanVisitor<>::UpResult
        {
            if (const auto inferModelName = op.tryGetAs<InferModelNameLogicalOperator>())
            {
                const auto& modelName = inferModelName->get().getModelName();
                if (!modelCatalog->hasModel(modelName))
                {
                    throw UnknownModelName("Model '{}' is not registered", modelName);
                }
                PRECONDITION(
                    std::ranges::size(children) == 1,
                    "Expected InferModelName Logical Operator to have one child, but has {}",
                    std::ranges::size(children));
                /// Deferred, like LogicalSourceExpansionRule's Union/SourceDescriptor construction below: attach
                /// the child via withChildrenUnsafe rather than the schema-inferring 2-arg constructor. At this
                /// point in the pipeline the child's own schema may itself still be unresolved (e.g. a source not
                /// yet given a schema, or a Projection/Selection holding a UDFCall UDFResolutionRule -- which runs
                /// after this rule -- hasn't attached a descriptor to yet). Real inference happens once, in
                /// TypeInferenceRule, after both catalog-resolution rules have run.
                return LogicalOperator{TypedLogicalOperator<InferModelLogicalOperator>{modelCatalog->load(modelName)}.withChildrenUnsafe(
                    {std::move(children.at(0))})};
            }
            /// Generic fallback: rebuild unresolved operators structurally, without forcing schema/type inference
            /// (see the comment above on why -- the same reasoning applies to every operator in the tree, not just
            /// InferModelName's own child).
            return op.withChildrenUnsafe(std::move(children));
        }};

    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> InferModelResolutionRule::needs() const
{
    return {typeid(LogicalSourceExpansionRule), typeid(SinkBindingRule), typeid(AnonymousSinkBindingRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> InferModelResolutionRule::neededBy() const
{
    return {typeid(TypeInferenceRule), typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType InferModelResolutionRule::create(PlanRuleRegistryArguments arguments)
{
    return InferModelResolutionRule{arguments.modelCatalog};
}

}
