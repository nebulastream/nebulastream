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

#include <Rules/Semantic/OriginIdInferenceRule.hpp>

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
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Semantic/InlineSourceBindingRule.hpp>
#include <Rules/Semantic/LogicalSourceExpansionRule.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>

namespace NES
{


const std::type_info& OriginIdInferenceRule::getType()
{
    return typeid(OriginIdInferenceRule);
}

std::string_view OriginIdInferenceRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> OriginIdInferenceRule::dependsOn() const
{
    return {typeid(InlineSourceBindingRule), typeid(LogicalSourceExpansionRule)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> OriginIdInferenceRule::requiredBy() const
{
    return {};
}

bool OriginIdInferenceRule::operator==(const OriginIdInferenceRule&) const
{
    return true;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan OriginIdInferenceRule::apply(const LogicalPlan& queryPlan) const
{
    /// origin ids, always start from 1 to n, whereby n is the number of operators that assign new orin ids
    auto lastOriginId = OriginId{INITIAL_ORIGIN_ID.getRawValue()};
    /// propagate origin ids through the complete query plan; an operator shared by multiple parents
    /// (fan-out) is visited exactly once and therefore assigns/reports one origin set for all consumers
    return transformPlan(
        queryPlan,
        [&lastOriginId](const LogicalOperator& visitingOperator, std::vector<LogicalOperator> children)
        {
            auto traitSet = visitingOperator.getTraitSet();
            if (visitingOperator.tryGetAs<OriginIdAssigner>().has_value())
            {
                lastOriginId = OriginId{lastOriginId.getRawValue() + 1};
                const auto success = tryInsert(traitSet, OutputOriginIdsTrait{{lastOriginId}});
                INVARIANT(success, "Failed to insert origin id trait, did another phase already assign them?");
            }
            else
            {
                std::vector<OutputOriginIdsTrait> childOriginIds;
                childOriginIds.reserve(children.size());
                for (const auto& child : children)
                {
                    const auto childOriginIdsOpt = getTrait<OutputOriginIdsTrait>(child.getTraitSet());
                    INVARIANT(childOriginIdsOpt.has_value(), "Child operator must have origin ids trait");
                    childOriginIds.push_back(childOriginIdsOpt.value().get());
                }
                const auto success = tryInsert(
                    traitSet,
                    OutputOriginIdsTrait{
                        childOriginIds | std::views::join | std::ranges::to<std::unordered_set>() | std::ranges::to<std::vector>()});
                INVARIANT(success, "Failed to insert origin id trait, did another phase already assign them?");
            }
            return visitingOperator.withTraitSet(traitSet).withChildren(std::move(children));
        });
}
}
