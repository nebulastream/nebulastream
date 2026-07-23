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

#include <RedundantUnionRemovalRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <variant>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>
#include <Rules/Barriers/SemanticAnalysisBarrier.hpp>
#include <Rules/PlanVisitor.hpp>
#include <ErrorHandling.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{


namespace
{


PlanVisitor<>::UpResult redundantUnionRemoval(const LogicalOperator& op, std::vector<LogicalOperator> children)
{
    if (op.tryGetAs<UnionLogicalOperator>().has_value() && children.size() == 1)
    {
        return children.front();
    }
    return op.withChildren(std::move(children));
}
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan RedundantUnionRemovalRule::apply(LogicalPlan queryPlan) const
{
    PlanVisitor<> visitor{redundantUnionRemoval};
    return visitor.apply(std::move(queryPlan));
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantUnionRemovalRule::needs() const
{
    return {typeid(SemanticAnalysisBarrier)};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantUnionRemovalRule::neededBy() const
{
    return {typeid(FixedPlanStructureBarrier)};
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param)
PlanRuleRegistryReturnType RedundantUnionRemovalRule::create(PlanRuleRegistryArguments)
{
    return RedundantUnionRemovalRule{};
}

}
