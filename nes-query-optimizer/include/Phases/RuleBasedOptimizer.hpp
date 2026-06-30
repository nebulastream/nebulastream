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
#include <optional>
#include <string>
#include <vector>
#include <Plans/LogicalPlan.hpp>
#include <Rules/Rule.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{
class StatisticRetrievalService;

class RuleBasedOptimizer
{
public:
    /// `statisticRetrievalService` (optional, may be null) enables the PoC StatisticOptimizationRule. The rule is
    /// NOT a standing rule in the sequence; it is applied per query only when that query requests a statistic
    /// (see optimize()'s `useStatisticSource`). When the service is null the rule is never applied.
    explicit RuleBasedOptimizer(
        QueryOptimizerConfiguration defaultQueryOptimization,
        std::shared_ptr<const StatisticRetrievalService> statisticRetrievalService = nullptr);

    /// If `useStatisticSource` is set and a retrieval service is wired in, the StatisticOptimizationRule is applied
    /// for that source after the standing rules. Otherwise the standing rule sequence runs unchanged.
    [[nodiscard]] LogicalPlan optimize(LogicalPlan plan, const std::optional<std::string>& useStatisticSource = std::nullopt) const;

private:
    QueryOptimizerConfiguration defaultQueryOptimization;
    std::vector<Rule<LogicalPlan>> ruleSequence;
    std::shared_ptr<const StatisticRetrievalService> statisticRetrievalService;
};

}
