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
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// Mandatory occurrence-based normalization after type inference and before semantic completion.
/// Each registered operator call becomes its ordinary unary logical operator, in child/source
/// order. Projections materialize argument expressions; final projections and
/// selection restoration hide internal temporary fields (including from '*').
/// Shared DAG references (e.g. from IN/BETWEEN/IS NAN desugaring, preserved through
/// TypeInferenceContext) resolve by pointer identity to their produced field within one
/// evaluation region. Distinct call nodes are never structurally merged; sharing across
/// regions requires an explicit named output field.
/// Conditional/lazy and unsupported operator expression contexts reject operator calls.
class OperatorFunctionLiftingRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "OperatorFunctionLiftingRule";

    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
    [[nodiscard]] std::set<std::type_index> wants() const;
};

static_assert(RuleConcept<OperatorFunctionLiftingRule, LogicalPlan>);
}
