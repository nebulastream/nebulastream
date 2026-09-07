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
#include <Rules/Static/DecideAlignStrategyRule.hpp>

#include <set>
#include <typeindex>
#include <typeinfo>
#include <vector>

#include <Operators/AlignLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <Traits/AlignStrategyTrait.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

namespace
{
LogicalOperator decideAlignStrategy(const LogicalOperator& logicalOperator, const std::vector<LogicalOperator>& children)
{
    auto traitSet = logicalOperator.getTraitSet();
    if (const auto alignOperator = logicalOperator.tryGetAs<AlignLogicalOperator>())
    {
        tryInsert(traitSet, AlignStrategyTrait{alignOperator.value()->getAlignStrategy()});
    }
    return logicalOperator.withChildren(children).withTraitSet(traitSet);
}
}

std::set<std::type_index> DecideAlignStrategyRule::needs() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

LogicalPlan DecideAlignStrategyRule::apply(const LogicalPlan& queryPlan) const
{
    PlanVisitor<> visitor{[](const LogicalOperator& op, const std::vector<LogicalOperator>& children) -> PlanVisitor<>::UpResult
                           { return decideAlignStrategy(op, children); }};

    return visitor.apply(queryPlan);
}

bool DecideAlignStrategyRule::operator==(const DecideAlignStrategyRule&) const
{
    return true;
}

PlanRuleRegistryReturnType DecideAlignStrategyRule::create(PlanRuleRegistryArguments)
{
    return DecideAlignStrategyRule{};
}
}
