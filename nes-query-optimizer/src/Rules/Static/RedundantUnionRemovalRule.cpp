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

#include <Rules/Static/RedundantUnionRemovalRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

const std::type_info& RedundantUnionRemovalRule::getType() const
{
    return typeid(RedundantUnionRemovalRule);
}

std::string_view RedundantUnionRemovalRule::getName() const
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantUnionRemovalRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantUnionRemovalRule::requiredBy() const
{
    return {};
}

bool RedundantUnionRemovalRule::equals(const Rule& other) const
{
    return dynamic_cast<const RedundantUnionRemovalRule*>(&other) != nullptr;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan RedundantUnionRemovalRule::apply(LogicalPlan queryPlan) const
{
    for (const auto& unionOperator : getOperatorByType<UnionLogicalOperator>(queryPlan)
             | std::views::filter([](const auto& op) { return op.getChildren().size() == 1; }))
    {
        auto child = unionOperator.getChildren().front();
        auto replaceResult = replaceSubtree(queryPlan, unionOperator.getId(), child);
        INVARIANT(replaceResult.has_value(), "Failed to replace union with its child");
        queryPlan = std::move(replaceResult.value());
    }
    return queryPlan;
}

}
