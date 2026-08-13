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
#include <Rules/Static/DecideMemoryLayoutRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Interface/MemoryLayout/LowerSchemaProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{
PlanVisitor<>::UpResult decideMemoryLayout(const LogicalOperator& op, const std::vector<LogicalOperator>& children)
{
    auto traitSet = op.getTraitSet();
    tryInsert(traitSet, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
    return op.withChildren(children).withTraitSet(std::move(traitSet));
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideMemoryLayoutRule::needs() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan DecideMemoryLayoutRule::apply(const LogicalPlan& queryPlan) const
{
    PlanVisitor<> visitor{decideMemoryLayout};
    return visitor.apply(queryPlan);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType PlanRuleGeneratedRegistrar::RegisterDecideMemoryLayoutPlanRule(PlanRuleRegistryArguments)
{
    return DecideMemoryLayoutRule{};
}

}
