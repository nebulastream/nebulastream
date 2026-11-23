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

/// Splits calls of HiPy UDFs (COMPILER HIPY) into a chain of native and Python stages, as nes_hipy_split.py prepared it at
/// registration (the "chain" of the compiled UDF). Each output of a stage becomes a call: an Inline HiPy call for a native
/// stage, which stays in the pipeline, and an InProcess Python call for a Python stage, which UDFLiftingRule then lifts into a
/// UDF operator. The calls read each other's results, so the call of the UDF becomes an expression whose leaves are its
/// arguments, and UDFLiftingRule orders the stages by these reads:
///   child -> projection [native stage 0] -> UDF operator [Python stage 1] -> projection [native stage 2, the expression]
/// Only selections and projections are rewritten, like UDFLiftingRule does. NES_HIPY_SPLIT=0 turns the rule off.
class HiPyChainRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "HiPyChainRule";

    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
};

static_assert(RuleConcept<HiPyChainRule, LogicalPlan>);
}
