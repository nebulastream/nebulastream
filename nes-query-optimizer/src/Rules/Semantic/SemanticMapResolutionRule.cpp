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
#include <string>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/SemanticFilterLogicalOperator.hpp>
#include <Operators/SemanticFilterNameLogicalOperator.hpp>
#include <Operators/SemanticMapLogicalOperator.hpp>
#include <Operators/SemanticMapNameLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/AnonymousSinkBindingRule.hpp>
#include <Rules/Semantic/InferModelResolutionRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Rules/Semantic/SemanticAsyncExecution.hpp>
#include <Rules/Semantic/SinkBindingRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Traits/AsyncExecutionTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

/// The resolved operator for a placeholder, with the async marker attached when the model asks for
/// it. Attached here, where the model is loaded, and read much later by AsyncOperatorSplitter: the
/// trait set is stored on the operator, so it survives the rebuilding the remaining rules and the
/// decomposition do.
template <typename Resolved>
LogicalOperator resolve(const RegisteredSemanticModel& model)
{
    auto resolved = LogicalOperator{TypedLogicalOperator<Resolved>{model}};
    if (model.getConfig().execution == SemanticExecution::ASYNCHRONOUS)
    {
        auto traits = resolved.getTraitSet();
        traits.insert(semanticAsyncExecution(model));
        resolved = resolved.withTraitSet(std::move(traits));
    }
    return resolved;
}

}

LogicalPlan SemanticMapResolutionRule::apply(const LogicalPlan& queryPlan) const
{
    /// The up-context records whether a subtree contains a resolved semantic operator, i.e. whether the
    /// operator sits on a spine that has to be relinked.
    using ResolutionVisitor = PlanVisitor<std::monostate, std::monostate, bool>;
    ResolutionVisitor visitor{
        [this](const LogicalOperator& op, std::vector<LogicalOperator> children, std::unordered_map<LogicalOperator, bool> upContexts)
            -> ResolutionVisitor::UpResult
        {
            const auto semanticMapName = op.tryGetAs<SemanticMapNameLogicalOperator>();
            const auto semanticFilterName = op.tryGetAs<SemanticFilterNameLogicalOperator>();
            if (semanticMapName || semanticFilterName)
            {
                const bool isFilter = semanticFilterName.has_value();
                const auto modelName = isFilter ? semanticFilterName->get().getModelName() : semanticMapName->get().getModelName();
                if (!semanticModelCatalog->hasModel(modelName))
                {
                    throw UnknownSemanticModelName("Semantic model '{}' is not registered", modelName);
                }
                PRECONDITION(
                    std::ranges::size(children) == 1,
                    "Expected {} Logical Operator to have one child, but has {}",
                    op.getName(),
                    std::ranges::size(children));
                /// A filter model declares no OUTPUT and a map model no condition, so using one in the
                /// other's place is a mistake in the query rather than something to guess around.
                auto model = semanticModelCatalog->load(modelName);
                if (isFilter && !model.hasFilterStep())
                {
                    throw InvalidSemanticModel(
                        "Semantic model '{}' declares an OUTPUT clause and is a SEM_MAP model; SEM_FILTER needs a model without OUTPUT",
                        modelName);
                }
                if (!isFilter && model.hasFilterStep())
                {
                    throw InvalidSemanticModel(
                        "Semantic model '{}' declares no OUTPUT clause and is a SEM_FILTER model; SEM_MAP needs a model with OUTPUT",
                        modelName);
                }
                /// withChildrenUnsafe, not the child-taking constructor: the constructor infers the
                /// local schema eagerly, which reads the child's output schema — absent on a Union
                /// straight out of LogicalSourceExpansionRule and guarded on an unresolved
                /// InferModelName.
                auto resolved = isFilter ? resolve<SemanticFilterLogicalOperator>(model) : resolve<SemanticMapLogicalOperator>(model);
                return {resolved.withChildrenUnsafe(std::move(children)), true};
            }
            /// Subtrees without a resolved semantic operator are returned as they are. Rebuilding them would
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
    /// it must therefore meet resolved semantic operators rather than placeholders.
    return {typeid(TypeInferenceRule), typeid(SemanticAnalysisBarrier), typeid(InferModelResolutionRule)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType SemanticMapResolutionRule::create(PlanRuleRegistryArguments arguments)
{
    return SemanticMapResolutionRule{arguments.semanticModelCatalog};
}

}
