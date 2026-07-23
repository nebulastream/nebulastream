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

#include <Rules/Static/OriginIdInferenceRule.hpp>

#include <algorithm>
#include <iterator>
#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <unordered_set>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{
LogicalOperator propagateOriginIds(const LogicalOperator& op, const std::vector<LogicalOperator>& children, OriginId& lastOriginId)
{
    std::vector<OutputOriginIdsTrait> childOriginIds;
    for (const auto& child : children)
    {
        const auto childOriginIdsOpt = getTrait<OutputOriginIdsTrait>(child.getTraitSet());
        INVARIANT(childOriginIdsOpt.has_value(), "Child operator must have origin ids trait");
        childOriginIds.push_back(childOriginIdsOpt.value().get());
    }

    auto traitSet = op.getTraitSet();

    if (op.tryGetAs<OriginIdAssigner>().has_value())
    {
        lastOriginId = OriginId{lastOriginId.getRawValue() + 1};
        const auto success = tryInsert(traitSet, OutputOriginIdsTrait{{lastOriginId}});
        INVARIANT(success, "Failed to insert origin id trait, did another phase already assign them?");
    }
    else
    {
        const auto success = tryInsert(
            traitSet,
            OutputOriginIdsTrait{
                childOriginIds | std::views::join | std::ranges::to<std::unordered_set>() | std::ranges::to<std::vector>()});
        INVARIANT(success, "Failed to insert origin id trait, did another phase already assign them?");
    }

    return op.withTraitSet(traitSet).withChildren(children);
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> OriginIdInferenceRule::needs() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan OriginIdInferenceRule::apply(const LogicalPlan& queryPlan) const
{
    auto originIdCounter = OriginId{INITIAL_ORIGIN_ID.getRawValue()};

    PlanVisitor<> visitor{
        [&originIdCounter](const LogicalOperator& op, const std::vector<LogicalOperator>& children) -> PlanVisitor<>::UpResult
        { return propagateOriginIds(op, children, originIdCounter); }};

    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType OriginIdInferenceRule::create(PlanRuleRegistryArguments)
{
    return OriginIdInferenceRule{};
}
}
