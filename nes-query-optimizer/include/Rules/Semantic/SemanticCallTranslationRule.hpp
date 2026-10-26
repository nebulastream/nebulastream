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

/// Translates the resolved, possibly fused, semantic operators into ordinary projections and selections over the async
/// function `SEM_CALL`, which `AsyncFunctionLiftingRule` then lifts like any other async call. Value k of the call is
/// step k of the model:
///
///     SEM_MAP(m) with OUTPUT (a)  =>  PROJECTION(*, SEM_CALL(m, in...) AS a)
///     SEM_FILTER(f)               =>  PROJECTION(<child fields>)
///                                       SELECTION(__SEM_VERDICT_0 = 'true')
///                                         PROJECTION(*, SEM_CALL(f, in...) AS __SEM_VERDICT_0)
///     fused [a, b, filter]        =>  as SEM_FILTER, with SEM_CALL(...) AS a, ASYNC_OUTPUT(SEM_CALL(...), 1) AS b,
///                                     ASYNC_OUTPUT(SEM_CALL(...), 2) AS __SEM_VERDICT_0 in the lowest projection
///
/// Every value comes from one projection, and equal calls are evaluated once, so an operator costs one request per row
/// and one async stage; the selection is synchronous. Runs after `TypeInferenceRule`, because the top projection drops
/// the verdicts again by naming the child's fields, and after `SemanticFusionRule`, so a fused chain becomes one call.
/// A plan without semantic operators is returned unchanged.
class SemanticCallTranslationRule
{
public:
    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "SemanticCallTranslationRule";

    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;
};

static_assert(RuleConcept<SemanticCallTranslationRule, LogicalPlan>);
}
