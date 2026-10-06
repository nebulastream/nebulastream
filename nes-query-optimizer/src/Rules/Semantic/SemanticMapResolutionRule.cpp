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

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
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
#include <Traits/AsyncExecutionTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>
#include <SemanticAsyncWiring.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

/// Enough room that the producer keeps the executor's threads busy without parking a large
/// share of the shared buffer pool: two buffers per concurrent call, never fewer than the
/// framework's own default.
constexpr size_t MinimumChannelCapacity = 64;

/// Turns the model's configuration into the marker `AsyncOperatorSplitter` acts on. Only the
/// model knows what the executor needs, and only here is it loaded, so the whole payload is
/// assembled once and travels as plain data from this point on.
AsyncExecutionTrait asyncExecution(const RegisteredSemanticModel& model)
{
    const auto& config = model.getConfig();
    const auto canonicalNames = [](const SemanticFieldList& fields)
    {
        return fields
            | std::views::transform([](const UnqualifiedUnboundField& field)
                                    { return static_cast<const Identifier&>(field.getFullyQualifiedName()).asCanonicalString(); })
            | std::ranges::to<std::vector>();
    };

    auto encoded = encodeSemanticMapPayload(SemanticMapAsyncPayload{
        .config = config,
        .inputFields = canonicalNames(model.getSchema().inputs),
        .outputFields = canonicalNames(model.getSchema().outputs)});

    return AsyncExecutionTrait{
        "SemanticMap",
        {{std::string{SemanticMapConfigKey}, std::move(encoded)}},
        config.batchSize,
        config.maxConcurrency,
        std::max(MinimumChannelCapacity, 2 * config.maxConcurrency),
        config.preserveOrder};
}

}

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
                auto model = semanticModelCatalog->load(modelName);
                auto resolved = LogicalOperator{TypedLogicalOperator<SemanticMapLogicalOperator>{model}};
                if (model.getConfig().execution == SemanticExecution::ASYNCHRONOUS)
                {
                    /// Attached here, where the model is loaded, and read much later by
                    /// AsyncOperatorSplitter: the trait set is stored on the operator, so it
                    /// survives the rebuilding the remaining rules and the decomposition do.
                    auto traits = resolved.getTraitSet();
                    traits.insert(asyncExecution(model));
                    resolved = resolved.withTraitSet(std::move(traits));
                }
                return {resolved.withChildrenUnsafe(std::move(children)), true};
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
