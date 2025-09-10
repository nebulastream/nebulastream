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

#include <functional>
#include <ranges>
#include <unordered_map>
#include <utility>
#include <vector>

#include <GlobalOptimizer/BottomUpPlacement.hpp>
#include <GlobalOptimizer/GlobalOptimizer.hpp>
#include <GlobalOptimizer/QueryDecomposition.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <WorkerConfig.hpp>

namespace NES
{

PlanStage::DistributedLogicalPlan QueryPlanner::plan(PlanStage::BoundLogicalPlan&& boundPlan) &&
{
    PlanStage::OptimizedLogicalPlan optimized = GlobalOptimizer::with(context).optimize(std::move(boundPlan));
    PlanStage::PlacedLogicalPlan placed = BottomUpOperatorPlacer::with(context).place(std::move(optimized));
    PlanStage::DecomposedLogicalPlan decomposed = QueryDecomposer::with(context).decompose(std::move(placed));

    /// Swap out host addr to grpc to identify workers and calculate capacity allocations
    auto swapped = std::unordered_map<GrpcAddr, std::vector<LogicalPlan>>{};
    for (auto& [host, plans] : decomposed.localPlans)
    {
        auto conf = context.workerCatalog->getWorker(host);
        INVARIANT(conf.has_value(), "Worker with hostname {} not present in the worker catalog", host);
        swapped.emplace(conf->grpc, std::move(plans));
    }

    /// Create cleanup lambda that captures the WorkerCatalog for capacity cleanup
    auto cleanupCallback = [workers = copyPtr(context.workerCatalog)](const PlanStage::DistributedLogicalPlan& plan)
    {
        std::ranges::for_each(
            plan,
            [&workers](const auto& plansPerNode)
            {
                const auto& [grpc, localPlans] = plansPerNode;
                const auto usedCapacity = std::ranges::fold_left(
                    localPlans, 0, [](auto acc, const auto& localPlan) { return acc + flatten(localPlan).size(); });
                workers->returnCapacity(grpc, usedCapacity);
            });
    };

    return PlanStage::DistributedLogicalPlan{PlanStage::DecomposedLogicalPlan{std::move(swapped)}, std::move(cleanupCallback)};
}

}
