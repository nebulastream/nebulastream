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

#include <Rules/Barriers/FixedPlanStructureBarrier.hpp>

#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <Plans/LogicalPlan.hpp>

namespace NES
{

const std::type_info& FixedPlanStructureBarrier::getType()
{
    return typeid(FixedPlanStructureBarrier);
}

std::string_view FixedPlanStructureBarrier::getName()
{
    return NAME;
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> FixedPlanStructureBarrier::dependsOn() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::set<std::type_index> FixedPlanStructureBarrier::requiredBy() const
{
    return {};
}

/// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
LogicalPlan FixedPlanStructureBarrier::apply(LogicalPlan queryPlan) const
{
    return queryPlan;
}

bool FixedPlanStructureBarrier::operator==(const FixedPlanStructureBarrier&) const
{
    return true;
}
}
