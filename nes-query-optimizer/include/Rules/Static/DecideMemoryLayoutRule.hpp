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
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>

namespace NES
{

/// Decides what memory layout should be used per operator. For now, we use row-layout across all operators.
class DecideMemoryLayoutRule
{
public:
    static constexpr std::string_view NAME = "DecideMemoryLayoutRule";

    [[nodiscard]] const std::type_info& getType() const;
    [[nodiscard]] std::string_view getName() const;
    [[nodiscard]] std::set<std::type_index> getDependencies() const;
    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    bool operator==(const DecideMemoryLayoutRule& other) const;

private:
    [[nodiscard]] LogicalOperator apply(const LogicalOperator& logicalOperator) const;
};

static_assert(PlanRuleConcept<DecideMemoryLayoutRule>);
}
