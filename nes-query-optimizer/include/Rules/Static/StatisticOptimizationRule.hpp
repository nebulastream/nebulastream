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
#include <string>
#include <string_view>
#include <typeindex>
#include <typeinfo>

#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>

namespace NES
{

class StatisticRetrievalService;

/// PoC rule that surfaces a statistic to the optimizer. It is applied per query (not as a standing rule) only when
/// the query specifies which statistic to use (via the `USE_STATISTIC` query option). It fetches that statistic and
/// prints it, leaving the plan unmodified — standing in for a future adaptive rewrite (e.g. filter reordering by
/// selectivity).
class StatisticOptimizationRule
{
public:
    /// @param retrievalService shared ownership of the service used to fetch the statistic.
    /// @param statisticSource the source whose statistic this rule fetches during optimization.
    StatisticOptimizationRule(std::shared_ptr<const StatisticRetrievalService> retrievalService, std::string statisticSource);

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
    /// Which statistic (by source) to fetch when this rule is applied.
    std::string statisticSource;
};

static_assert(RuleConcept<StatisticOptimizationRule, LogicalPlan>);
}
