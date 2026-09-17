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

#include <Phases/OperatorPlacer.hpp>
#include <Phases/RuleBasedOptimizer.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Catalog.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

class QueryOptimizer final
{
public:
    explicit QueryOptimizer(const QueryOptimizerConfiguration& defaultQueryOptimization, const std::shared_ptr<Catalog>& catalog)
        : ruleBasedOptimization(defaultQueryOptimization, catalog), operatorPlacement(defaultQueryOptimization, catalog)
    {
    }

    [[nodiscard]] std::unordered_map<Host, std::vector<LogicalPlan>> optimize(LogicalPlan plan) const;

    /// The intermediate stage, after analysis and rewriting but before the split across hosts. Separated out
    /// so a caller that needs both stages can render them without running the rewrite twice.
    [[nodiscard]] LogicalPlan optimizeGlobalPlan(LogicalPlan plan) const;

    /// Expects a plan that has already been rewritten. A plan that has not been is placed as given, with no error.
    [[nodiscard]] std::unordered_map<Host, std::vector<LogicalPlan>> place(LogicalPlan globalPlan) const;

private:
    RuleBasedOptimizer ruleBasedOptimization;
    OperatorPlacer operatorPlacement;
};

}
