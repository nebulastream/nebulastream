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

#include <Rules/Semantic/SemMapResolutionRule.hpp>

#include <algorithm>
#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SemMapLogicalOperator.hpp>
#include <Operators/SemMapNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/AnonymousSinkBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Rules/Semantic/InferModelResolutionRule.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{


LogicalPlan SemMapResolutionRule::apply(const LogicalPlan& queryPlan) const
{
    using SemMapVisitor = PlanVisitor<std::monostate, std::monostate, bool>;
    SemMapVisitor visitor{
        [this](const LogicalOperator& op, std::vector<LogicalOperator> children, std::unordered_map<LogicalOperator, bool> upContexts)
            -> SemMapVisitor::UpResult
        {
            if (const auto semMapName = op.tryGetAs<SemMapNameLogicalOperator>())
            {
                const auto& modelName = semMapName->get().getModelName();
                if (!semanticModelCatalog->hasModel(modelName))
                {
                    throw UnknownModelName("Semantic model '{}' is not registered", modelName);
                }
                PRECONDITION(
                    std::ranges::size(children) == 1,
                    "Expected SemMapName Logical Operator to have one child, but has {}",
                    std::ranges::size(children));
                /// The spine-only traversal below leaves subtrees without a resolved SemMapName
                /// untouched, so a child straight out of LogicalSourceExpansionRule (a
                /// UnionLogicalOperator built via withChildrenUnsafe) has no output schema yet —
                /// TypeInferenceRule, which would normally supply it, runs after this rule.
                /// SemMapLogicalOperator's constructor reads the child's output schema eagerly
                /// (mirroring InferModelLogicalOperator), so infer it here first.
                return {
                    LogicalOperator{TypedLogicalOperator<SemMapLogicalOperator>{
                        semanticModelCatalog->load(modelName),
                        semMapName->get().getCallSiteInputs(),
                        children.at(0).withInferredSchema(),
                        semMapName->get().getOutputAlias()}},
                    true};
            }
            /// Subtrees without a resolved SemMapName are returned untouched: other name-variant
            /// operators (e.g. InferModelName) keep their guarded schema accessors until their own
            /// resolution rule runs, so rebuilding their parents here would eagerly re-infer
            /// projection schemas over an unresolved operator. TypeInferenceRule, which runs after
            /// all resolution rules, infers the schemas of the rebuilt spine.
            const bool anyChildResolved = std::ranges::any_of(upContexts, [](const auto& entry) { return entry.second; });
            if (!anyChildResolved)
            {
                return {op, false};
            }
            return {op.withChildren(std::move(children)), true};
        }};

    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> SemMapResolutionRule::needs() const
{
    return {typeid(LogicalSourceExpansionRule), typeid(SinkBindingRule), typeid(AnonymousSinkBindingRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> SemMapResolutionRule::neededBy() const
{
    return {typeid(TypeInferenceRule), typeid(SemanticAnalysisBarrier), typeid(InferModelResolutionRule)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType SemMapResolutionRule::create(PlanRuleRegistryArguments arguments)
{
    return SemMapResolutionRule{arguments.semanticModelCatalog};
}

}
