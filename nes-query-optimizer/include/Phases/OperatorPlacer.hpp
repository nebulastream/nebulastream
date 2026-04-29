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

#include <unordered_map>
#include <utility>
#include <vector>

#include <Plans/LogicalPlan.hpp>
#include <NetworkTopology.hpp>
#include <PlannerContext.hpp>
#include <QueryOptimizerConfiguration.hpp>

namespace NES
{

struct PlannerContext;

class OperatorPlacer
{
public:
    explicit OperatorPlacer(QueryOptimizerConfiguration defaultQueryOptimization, const PlannerContext &ctx)
        : defaultQueryOptimization(std::move(defaultQueryOptimization)), ctx{ctx},
          topology{WorkerCatalog::getTopology(ctx)} {
    }

    /// Takes the query plan as a logical plan and returns a distributed plan with placement and decomposition
    [[nodiscard]] std::unordered_map<Host, std::vector<LogicalPlan>> place(LogicalPlan plan) const;

private:
    QueryOptimizerConfiguration defaultQueryOptimization;
    const PlannerContext& ctx;
    NetworkTopology topology;
};


}
