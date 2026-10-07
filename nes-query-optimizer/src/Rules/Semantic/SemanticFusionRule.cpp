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

#include <Rules/Semantic/SemanticFusionRule.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
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
#include <Operators/SemanticFilterLogicalOperator.hpp>
#include <Operators/SemanticMapLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/InferModelResolutionRule.hpp>
#include <Rules/Semantic/SemanticAsyncExecution.hpp>
#include <Rules/Semantic/SemanticMapResolutionRule.hpp>
#include <Rules/Semantic/TypeInferenceRule.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/Logger/Logger.hpp>
#include <PlanRuleRegistry.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

std::optional<RegisteredSemanticModel> semanticModelOf(const LogicalOperator& op)
{
    if (const auto map = op.tryGetAs<SemanticMapLogicalOperator>())
    {
        return map->get().getModel();
    }
    if (const auto filter = op.tryGetAs<SemanticFilterLogicalOperator>())
    {
        return filter->get().getModel();
    }
    return std::nullopt;
}

/// Everything that decides where and how a request goes. Two operators that differ in any of these
/// would, fused, silently run one of them against the other's configuration.
bool sameTransport(const SemanticModelConfig& lhs, const SemanticModelConfig& rhs)
{
    return lhs.endpoint == rhs.endpoint && lhs.modelName == rhs.modelName && lhs.backend == rhs.backend
        && lhs.apiKeyEnvVar == rhs.apiKeyEnvVar && lhs.payloadFormat == rhs.payloadFormat && lhs.datasetPrompt == rhs.datasetPrompt
        && lhs.execution == rhs.execution && lhs.batchSize == rhs.batchSize && lhs.maxConcurrency == rhs.maxConcurrency
        && lhs.maxRetries == rhs.maxRetries && lhs.maxWaitTime == rhs.maxWaitTime && lhs.requestTimeout == rhs.requestTimeout
        && lhs.preserveOrder == rhs.preserveOrder;
}

std::set<std::string> outputNames(const RegisteredSemanticModel& model)
{
    return model.getSchema().outputs
        | std::views::transform([](const UnqualifiedUnboundField& field)
                                { return static_cast<const Identifier&>(field.getFullyQualifiedName()).asCanonicalString(); })
        | std::ranges::to<std::set>();
}

/// The operator a fused model runs as, carrying a marker rebuilt from the fused model: the old
/// operators' markers describe their own step lists and would hand the executor the wrong prompt.
LogicalOperator fusedOperator(const RegisteredSemanticModel& model)
{
    auto fused = model.hasFilterStep() ? LogicalOperator{TypedLogicalOperator<SemanticFilterLogicalOperator>{model}}
                                       : LogicalOperator{TypedLogicalOperator<SemanticMapLogicalOperator>{model}};
    if (model.getConfig().execution == SemanticExecution::ASYNCHRONOUS)
    {
        auto traits = fused.getTraitSet();
        traits.insert(semanticAsyncExecution(model));
        fused = fused.withTraitSet(std::move(traits));
    }
    return fused;
}

/// Passed up from every operator: whether its subtree was rewritten, so the spine above gets
/// relinked, and how many parents it has in the original plan.
struct FusionUp
{
    bool changed = false;
    size_t parents = 0;
};

}

bool SemanticFusionRule::canFuse(const RegisteredSemanticModel& child, const RegisteredSemanticModel& parent)
{
    if (!child.getConfig().fusion || !parent.getConfig().fusion)
    {
        return false;
    }
    /// Ordered comparison: the payload serializes the inputs positionally, so a different order is
    /// a different prompt.
    if (!(child.getSchema().inputs == parent.getSchema().inputs))
    {
        return false;
    }
    if (!sameTransport(child.getConfig(), parent.getConfig()))
    {
        return false;
    }
    const auto childOutputs = outputNames(child);
    return std::ranges::none_of(outputNames(parent), [&childOutputs](const std::string& name) { return childOutputs.contains(name); });
}

LogicalPlan SemanticFusionRule::apply(const LogicalPlan& queryPlan) const
{
    /// The down pass hands each operator one context per parent edge, which is exactly the parent
    /// count the up pass needs: an operator feeding two parents cannot be folded into one of them.
    using FusionVisitor = PlanVisitor<size_t, std::monostate, FusionUp>;
    FusionVisitor visitor{
        [](const LogicalOperator&, const std::vector<std::monostate>& parentEdges) -> FusionVisitor::DownResult
        { return {.operatorContext = parentEdges.size(), .downContexts = {}}; },
        [](const LogicalOperator& op,
           std::vector<LogicalOperator> children,
           const size_t parents,
           const std::unordered_map<LogicalOperator, FusionUp>& upContexts) -> FusionVisitor::UpResult
        {
            const bool childChanged = std::ranges::any_of(upContexts, [](const auto& entry) { return entry.second.changed; });

            if (const auto parentModel = semanticModelOf(op); parentModel.has_value() && children.size() == 1)
            {
                const auto& child = children.front();
                const auto childModel = semanticModelOf(child);
                if (childModel.has_value() && upContexts.at(child).parents == 1 && canFuse(*childModel, *parentModel))
                {
                    /// The child is upstream, so its steps come first — `op.fuse(next)` in the reference.
                    const auto fusedModel = fuseSemanticModels(*childModel, *parentModel);
                    NES_DEBUG("Fusing semantic models '{}' and '{}' into one prompt", childModel->getName(), parentModel->getName());
                    return {
                        fusedOperator(fusedModel).withChildrenUnsafe(child.getChildren()), FusionUp{.changed = true, .parents = parents}};
                }
            }
            if (childChanged)
            {
                return {op.withChildrenUnsafe(std::move(children)), FusionUp{.changed = true, .parents = parents}};
            }
            return {op, FusionUp{.changed = false, .parents = parents}};
        }};

    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> SemanticFusionRule::needs() const
{
    return {typeid(SemanticMapResolutionRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> SemanticFusionRule::neededBy() const
{
    /// Same reasons as SemanticMapResolutionRule's: type inference has to see the final operators,
    /// and InferModelResolutionRule re-infers every operator it rebuilds.
    return {typeid(TypeInferenceRule), typeid(SemanticAnalysisBarrier), typeid(InferModelResolutionRule)};
}

PlanRuleRegistryReturnType SemanticFusionRule::create(PlanRuleRegistryArguments)
{
    return SemanticFusionRule{};
}

}
