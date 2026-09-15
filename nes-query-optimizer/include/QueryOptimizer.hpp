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
#include <unordered_map>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Phases/OperatorPlacer.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Catalog.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

/// The result of optimizing one query.
struct OptimizedPlan
{
    /// The plan after the rewrite rules, before it is split across workers.
    LogicalPlan globalPlan;
    /// The parts of the global plan that each worker runs.
    std::unordered_map<Host, std::vector<LogicalPlan>> localPlans;
};

class QueryOptimizer final
{
public:
    explicit QueryOptimizer(const QueryOptimizerConfiguration& defaultQueryOptimization, const std::shared_ptr<Catalog>& catalog)
        : ruleBasedOptimization(defaultQueryOptimization, catalog), operatorPlacement(defaultQueryOptimization, catalog)
    {
    }

    [[nodiscard]] OptimizedPlan optimize(LogicalPlan plan) const;

private:
    RuleBasedOptimizer ruleBasedOptimization;
    OperatorPlacer operatorPlacement;
};

}
