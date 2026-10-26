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
#include <utility>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <PlanRuleRegistry.hpp>
#include <SemanticModelCatalog.hpp>

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
///
/// SEM_JOIN is an inner join whose condition has `SEM_MATCH(m, left, right)` conjuncts. The join keeps the other
/// conjuncts (TRUE if there are none), and each SEM_MATCH becomes a verdict above it, as for SEM_FILTER:
///
///     JOIN(SEM_MATCH(m, l, r) AND c)  =>  PROJECTION(<join fields>)
///                                           SELECTION(__SEM_VERDICT_0 = 'true')
///                                             PROJECTION(*, SEM_CALL(m, l, r) AS __SEM_VERDICT_0)
///                                               JOIN(c)
///
/// SEM_AGG is an aggregation of a windowed aggregation, `SEM_AGG(m, f) AS s`. The model is a map model with one INPUT and
/// one OUTPUT field, and one prompt per window and group carries all of the window's values:
///
///     WINDOW(…, SEM_AGG(m, f) AS s, …)  =>  PROJECTION(<window fields>, s)
///                                             PROJECTION(*, SEM_CALL(m, __SEM_AGG_0) AS s)
///                                               WINDOW(…, JSON_AGG(f) AS __SEM_AGG_0, …)
///
/// The model is looked up here, since the join condition and the aggregation carry only its name.
class SemanticCallTranslationRule
{
public:
    explicit SemanticCallTranslationRule(std::shared_ptr<const SemanticModelCatalog> semanticModelCatalog)
        : semanticModelCatalog(std::move(semanticModelCatalog))
    {
    }

    static PlanRuleRegistryReturnType create(PlanRuleRegistryArguments arguments);
    static constexpr std::string_view NAME = "SemanticCallTranslationRule";

    [[nodiscard]] LogicalPlan apply(const LogicalPlan& queryPlan) const;
    [[nodiscard]] std::set<std::type_index> needs() const;
    [[nodiscard]] std::set<std::type_index> neededBy() const;

private:
    std::shared_ptr<const SemanticModelCatalog> semanticModelCatalog;
};

static_assert(RuleConcept<SemanticCallTranslationRule, LogicalPlan>);
}
