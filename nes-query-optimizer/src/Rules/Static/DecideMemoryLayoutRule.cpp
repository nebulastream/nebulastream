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

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <ErrorHandling.hpp>
#include <PlanRewriteUtils.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> DecideMemoryLayoutRule::needs() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

LogicalPlan DecideMemoryLayoutRule::apply(const LogicalPlan& queryPlan) const
{
    PRECONDITION(not queryPlan.getRootOperators().empty(), "Query must have a sink root operator");
    return rewritePlanBottomUp(
        queryPlan,
        [this](const LogicalOperator& logicalOperator, std::vector<LogicalOperator> children)
        { return apply(logicalOperator, std::move(children)); });
}

LogicalOperator DecideMemoryLayoutRule::apply(const LogicalOperator& logicalOperator, std::vector<LogicalOperator> children) const
{
    /// Iterating over all operators and setting the memory layout trait to row
    auto traitSet = logicalOperator.getTraitSet();
    tryInsert(traitSet, MemoryLayoutTypeTrait{MemoryLayoutType::ROW_LAYOUT});
    return logicalOperator.withChildren(std::move(children)).withTraitSet(traitSet);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType PlanRuleGeneratedRegistrar::RegisterDecideMemoryLayoutPlanRule(PlanRuleRegistryArguments)
{
    return DecideMemoryLayoutRule{};
}

}
