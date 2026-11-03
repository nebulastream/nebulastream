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
#include <PlanRuleRegistry.hpp>

namespace NES
{

/// Pulls UDF calls that do not execute inside the query pipeline out of selections and projections, into UDFLogicalOperators:
///   child -> pre-projection -> UDF operator -> original expressions, reading the call results
/// The pre-projection computes the arguments that are neither fields nor constants. The calls that can run together become
/// one operator, which fuses them; Inline UDFs stay in their expressions. A join predicate needs both sides of the join, so
/// a non-Inline UDF in one is rejected.
class UDFLiftingRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "UDFLiftingRule";

    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
    [[nodiscard]] std::set<std::type_index> wants() const;
};

static_assert(RuleConcept<UDFLiftingRule, LogicalPlan>);
}
