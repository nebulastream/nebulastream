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

#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/MemoryLayoutType.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{
PlanVisitor<>::UpResult
decideMemoryLayout(const LogicalOperator& op, const std::vector<LogicalOperator>& children, const MemoryLayoutType memoryLayout)
{
    auto traitSet = op.getTraitSet();
    tryInsert(traitSet, MemoryLayoutTypeTrait{memoryLayout});
    return op.withChildren(children).withTraitSet(std::move(traitSet));
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideMemoryLayoutRule::needs() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

LogicalPlan DecideMemoryLayoutRule::apply(const LogicalPlan& queryPlan) const
{
    PlanVisitor<> visitor{[this](const LogicalOperator& op, const std::vector<LogicalOperator>& children)
                          { return decideMemoryLayout(op, children, this->memoryLayout); }};
    return visitor.apply(queryPlan);
}

bool DecideMemoryLayoutRule::operator==(const DecideMemoryLayoutRule& other) const
{
    return this->memoryLayout == other.memoryLayout;
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType DecideMemoryLayoutRule::create(PlanRuleRegistryArguments arguments)
{
    return DecideMemoryLayoutRule{arguments.defaultQueryOptimization.memoryLayout};
}

}
