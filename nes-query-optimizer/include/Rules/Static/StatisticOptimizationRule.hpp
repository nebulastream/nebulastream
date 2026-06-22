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

#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>

namespace NES
{

class StatisticRetrievalService;

/**
 * @brief Proof-of-concept rule that demonstrates making statistics accessible to query-structuring decisions.
 *
 * The optimizer normally rewrites a plan from purely static information. This rule shows the missing piece for
 * *adaptive* optimization: pulling a runtime statistic about the data and letting a structuring decision depend on
 * it. A real future rule could, for example, reorder filters by the measured selectivity of the predicates over the
 * actual data, or pick a join side from cardinality estimates.
 *
 * For now this is only the data-flow skeleton: `apply` asks the StatisticRetrievalService for a (mock) statistic,
 * prints the returned value so a run is observable, and then returns the plan **unmodified** — we have no meaningful
 * statistic to act on yet. The point is solely to prove the control plane can surface a statistic value to the
 * optimizer at plan-rewrite time.
 *
 * Because it requires a wired-up StatisticRetrievalService (whose coordinator submits the build/probe queries), this
 * rule is constructed with that service rather than default-constructed, and is *not* part of the default
 * RuleBasedOptimizer sequence (running a statistic retrieval while optimizing the statistic queries themselves would
 * recurse). It is meant to be instantiated and applied explicitly.
 */
class StatisticOptimizationRule
{
public:
    /// @param retrievalService shared ownership of the service used to fetch the statistic.
    explicit StatisticOptimizationRule(std::shared_ptr<const StatisticRetrievalService> retrievalService);

    static constexpr std::string_view NAME = "StatisticOptimizationRule";

    [[nodiscard]] static const std::type_info& getType();
    [[nodiscard]] static std::string_view getName();
    [[nodiscard]] std::set<std::type_index> dependsOn() const;
    [[nodiscard]] std::set<std::type_index> requiredBy() const;
    [[nodiscard]] LogicalPlan apply(LogicalPlan queryPlan) const;
    bool operator==(const StatisticOptimizationRule& other) const;

private:
    /// shared_ptr so the rule stays copyable/assignable as required by the type-erased Rule wrapper, while sharing
    /// ownership of the service.
    std::shared_ptr<const StatisticRetrievalService> retrievalService;
};

static_assert(RuleConcept<StatisticOptimizationRule, LogicalPlan>);
}
