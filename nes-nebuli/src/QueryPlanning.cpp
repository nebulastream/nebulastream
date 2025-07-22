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

#include <Util/Pointers.hpp>
#include <QueryPlanning.hpp>

#include <ranges>
#include <utility>

#include <Distributed/BottomUpPlacement.hpp>
#include <Distributed/OperatorPlacement.hpp>
#include <Distributed/QueryDecomposition.hpp>
#include <LegacyOptimizer/LegacyOptimizer.hpp>

namespace NES
{

QueryPlanner::FinalizedLogicalPlan QueryPlanner::plan(BoundLogicalPlan&& boundPlan)
{
    auto& [plan, topology, nodeCatalog, sourceCatalog, sinkCatalog] = boundPlan;

    LegacyOptimizer::OptimizedLogicalPlan optimizedPlan
        = LegacyOptimizer::optimize(std::move(plan), copyPtr(sourceCatalog), copyPtr(sinkCatalog));

    OperatorPlacer::PlacedLogicalPlan placedPlan = BottomUpOperatorPlacer::from(topology, nodeCatalog).place(std::move(optimizedPlan));

    QueryDecomposer::DecomposedLogicalPlan decomposedPlan
        = QueryDecomposer::from(std::move(placedPlan), topology, copyPtr(sourceCatalog), copyPtr(sinkCatalog)).decompose();

    return FinalizedLogicalPlan{
        std::views::transform(
            std::move(decomposedPlan),
            [&nodeCatalog](const auto& nodePlan)
            {
                auto& [hostAddr, plan] = nodePlan;
                return std::make_pair(nodeCatalog.getGrpcAddressFor(std::move(hostAddr)), std::move(plan));
            })
        | std::ranges::to<QueryDecomposer::DecomposedLogicalPlan>()};
}
}
