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
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <Util/MemoryLayoutType.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// Decides what memory layout should be used per operator.
/// For now, the optimizer configuration decides the memory layout (`optimizer.memory_layout`).
/// There currently is no heuristic involved in the choice of the memory layout. ROW_LAYOUT is the default.
class DecideMemoryLayoutRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);

    explicit DecideMemoryLayoutRule(const MemoryLayoutType memoryLayout) : memoryLayout(memoryLayout) { }

    static constexpr std::string_view NAME = "DecideMemoryLayoutRule";

    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;

    bool operator==(const DecideMemoryLayoutRule& other) const;

private:
    MemoryLayoutType memoryLayout;
};

static_assert(RuleConcept<DecideMemoryLayoutRule, LogicalPlan>);
}
