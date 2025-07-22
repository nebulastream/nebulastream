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

#include <QueryPlanning.hpp>

#include <ranges>
#include <utility>

#include <Distributed/OperatorPlacement.hpp>
#include <Distributed/QueryDecomposition.hpp>
#include <LegacyOptimizer/LegacyOptimizer.hpp>
#include <YAML/YAMLBinder.hpp>

namespace NES
{

QueryDecomposer::DecomposedLogicalPlan QueryPlanner::plan(const std::string& inputPath)
{
    // Load and process query configuration
    auto queryConfig = CLI::YAMLLoader::load(inputPath);
    auto [plan, topology, nodeCatalog, sourceCatalog, sinkCatalog] = CLI::YAMLBinder::from(std::move(queryConfig)).bind();

    /// Optimize the plan
    LegacyOptimizer::OptimizedLogicalPlan optimizedPlan = LegacyOptimizer::optimize(std::move(plan), sourceCatalog, sinkCatalog);

    /// Place operators
    OperatorPlacer::PlacedLogicalPlan placedPlan = OperatorPlacer::from(topology, nodeCatalog).place(std::move(optimizedPlan));

    /// Decompose the plan
    QueryDecomposer::DecomposedLogicalPlan decomposedPlan
        = QueryDecomposer::from(std::move(placedPlan), topology, sourceCatalog, sinkCatalog).decompose();

    return std::views::transform(
               decomposedPlan,
               [nodeCatalog](const auto& nodePlan)
               {
                   auto& [hostAddr, plan] = nodePlan;
                   return std::make_pair(nodeCatalog.getGrpcAddressFor(std::move(hostAddr)), std::move(plan));
               })
        | std::ranges::to<QueryDecomposer::DecomposedLogicalPlan>();
}
}
