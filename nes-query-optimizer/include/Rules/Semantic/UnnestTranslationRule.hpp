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

/// Turns the `UNNEST(arr)` placeholder of a projection into an `UnnestLogicalOperator` below it:
///
///     PROJECTION(id, UNNEST(arr) AS item)  =>  PROJECTION(id, __UNNEST_0 AS item)
///                                                UNNEST(arr as __UNNEST_0)
///
/// The element goes into a helper field first, so the projection may reuse any name, `arr` included; an asterisk is
/// spelled out, so the helper does not leak into the output. One UNNEST per projection (several would need a rule for
/// how their elements combine), its argument a field, and only in the SELECT list. Runs after `TypeInferenceRule`,
/// because it reads the child's schema.
class UnnestTranslationRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "UnnestTranslationRule";

    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
};

static_assert(RuleConcept<UnnestTranslationRule, LogicalPlan>);
}
