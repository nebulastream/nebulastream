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

#include <Rules/Static/RedundantProjectionRemovalRule.hpp>

#include <ranges>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>
#include <vector>
#include <Operators/LogicalOperator.hpp>
#include <Operators/ProjectionLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

const std::type_info& RedundantProjectionRemovalRule::getType()
{
    return typeid(RedundantProjectionRemovalRule);
}

std::string_view RedundantProjectionRemovalRule::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantProjectionRemovalRule::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> RedundantProjectionRemovalRule::requiredBy() const
{
    return {};
}

bool RedundantProjectionRemovalRule::operator==(const RedundantProjectionRemovalRule&) const
{
    return true;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan RedundantProjectionRemovalRule::apply(LogicalPlan queryPlan) const
{
    return transformPlan(
        queryPlan,
        [](const LogicalOperator& op, std::vector<LogicalOperator> children)
        {
            if (op.tryGetAs<ProjectionLogicalOperator>().has_value())
            {
                INVARIANT(children.size() == 1, "Projection operator must have exactly one child");
                INVARIANT(op.getInputSchemas().size() == 1, "Projection operator must have exactly one input schema");
                /// A projection that neither renames nor drops fields forwards its data unchanged: bypass it.
                if (op.getInputSchemas().front() == op.getOutputSchema())
                {
                    return children.front();
                }
            }
            return op.withChildren(std::move(children));
        });
}

}
