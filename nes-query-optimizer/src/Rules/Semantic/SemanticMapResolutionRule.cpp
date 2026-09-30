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

#include <Rules/Semantic/SemanticMapResolutionRule.hpp>

#include <algorithm>
#include <ranges>
#include <set>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SemanticMapLogicalOperator.hpp>
#include <Operators/SemanticMapNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/AnonymousSinkBindingRule.hpp>
#include <Rules/Semantic/InferModelResolutionRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

LogicalPlan SemanticMapResolutionRule::apply(const LogicalPlan& queryPlan) const
{
    /// The up-context records whether a subtree contains a resolved SEM_MAP, i.e. whether the
    /// operator sits on a spine that has to be relinked.
    using ResolutionVisitor = PlanVisitor<std::monostate, std::monostate, bool>;
    ResolutionVisitor visitor{
        [this](const LogicalOperator& op, std::vector<LogicalOperator> children, std::unordered_map<LogicalOperator, bool> upContexts)
            -> ResolutionVisitor::UpResult
        {
            if (const auto semanticMapName = op.tryGetAs<SemanticMapNameLogicalOperator>())
            {
                const auto modelName = semanticMapName->get().getModelName();
                if (!semanticModelCatalog->hasModel(modelName))
                {
                    throw UnknownSemanticModelName("Semantic model '{}' is not registered", modelName);
                }
                PRECONDITION(
                    std::ranges::size(children) == 1,
                    "Expected SemanticMapName Logical Operator to have one child, but has {}",
                    std::ranges::size(children));
                /// withChildrenUnsafe, not the child-taking constructor: the constructor infers the
                /// local schema eagerly, which reads the child's output schema — absent on a Union
                /// straight out of LogicalSourceExpansionRule and guarded on an unresolved
                /// InferModelName.
                return {
                    LogicalOperator{TypedLogicalOperator<SemanticMapLogicalOperator>{semanticModelCatalog->load(modelName)}}
                        .withChildrenUnsafe(std::move(children)),
                    true};
            }
            /// Subtrees without a resolved SEM_MAP are returned as they are. Rebuilding them would
            /// gain nothing and would put other placeholders (InferModelName) through code paths
            /// that expect a resolved plan.
            const bool onResolvedSpine = std::ranges::any_of(upContexts, [](const auto& entry) { return entry.second; });
            if (!onResolvedSpine)
            {
                return {op, false};
            }
            return {op.withChildrenUnsafe(std::move(children)), true};
        }};

    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> SemanticMapResolutionRule::needs() const
{
    return {typeid(LogicalSourceExpansionRule), typeid(SinkBindingRule), typeid(AnonymousSinkBindingRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> SemanticMapResolutionRule::neededBy() const
{
    /// InferModelResolutionRule rebuilds every operator with withChildren, which re-infers schemas;
    /// it must therefore meet resolved SemanticMap operators rather than placeholders.
    return {typeid(TypeInferenceRule), typeid(SemanticAnalysisBarrier), typeid(InferModelResolutionRule)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType SemanticMapResolutionRule::create(PlanRuleRegistryArguments arguments)
{
    return SemanticMapResolutionRule{arguments.semanticModelCatalog};
}

}
