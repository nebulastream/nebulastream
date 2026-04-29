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
#include <memory>
#include <set>
#include <string_view>
#include <typeindex>
#include <typeinfo>
#include <utility>

#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>

namespace NES
{

struct PlannerContext;

/// The InlineSinkBindingPhase replaces all sink that are defined within the query itself (InlineSinkLogicalOperators), as opposed to
/// sinks that are created in separate CREATE statements, with SinkLogicalOperators based on the given inline sink configuration.

class InlineSinkBindingRule
{
public:
    explicit InlineSinkBindingRule(const PlannerContext& ctx) : ctx{ctx} { }

    static constexpr std::string_view NAME = "InlineSinkBindingRule";

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;
    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    bool operator==(const InlineSinkBindingRule& other) const;

private:
    const PlannerContext& ctx;
};

static_assert(RuleConcept<InlineSinkBindingRule, LogicalPlan>);
}
