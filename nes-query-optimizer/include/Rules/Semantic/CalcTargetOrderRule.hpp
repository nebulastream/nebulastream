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

#pragma once
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>

namespace NES
{
/// Calculates the schema of anonymous sinks if no schema was specified deterministically.
/// The default behavior for every operator is the following:
/// 1. For any child (in the order of the children): For any of its output fields (In the output order determined for the child):
///     If the current operator outputs a fields that has the same name, append it to the list of fields
/// 2. Any field that does not appear in the input to the operator, is appended to the ordered output fields in lexicographic order.
///
/// Concrete operators can overwrite this behavior by overwriting Reorderer
class CalcTargetOrderRule
{
public:
    static constexpr std::string_view NAME = "CalcTargetOrderRule";

    [[nodiscard]] LogicalPlan apply(LogicalPlan plan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
};

static_assert(RuleConcept<CalcTargetOrderRule, LogicalPlan>);
}
