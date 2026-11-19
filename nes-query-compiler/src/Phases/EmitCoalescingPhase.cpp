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

#include <Phases/EmitCoalescingPhase.hpp>

#include <chrono>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <InputCollector.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>
#include <ScanPhysicalOperator.hpp>

namespace NES::QueryCompilation::EmitCoalescingPhase
{

void apply(PipelinedQueryPlan& plan, const std::chrono::microseconds maxDelay)
{
    if (maxDelay == std::chrono::microseconds::zero())
    {
        return;
    }
    /// The plan lists only source pipelines; the others are reached through successors, some along several paths.
    std::vector<std::shared_ptr<Pipeline>> pending = plan.getPipelines();
    std::unordered_set<const Pipeline*> visited;
    while (!pending.empty())
    {
        const auto pipeline = std::move(pending.back());
        pending.pop_back();
        if (!visited.insert(pipeline.get()).second)
        {
            continue;
        }
        if (pipeline->isOperatorPipeline())
        {
            if (auto scan = pipeline->getRootOperator().tryGet<ScanPhysicalOperator>(); scan && !scan->isRaw())
            {
                const auto collectorId = getNextOperatorHandlerId();
                pipeline->getOperatorHandlers().emplace(collectorId, std::make_shared<InputCollector>(scan->getCapacity(), maxDelay));
                scan->setCollector(collectorId);
                pipeline->setRootOperator(*scan);
            }
        }
        pending.insert(pending.end(), pipeline->getSuccessors().begin(), pipeline->getSuccessors().end());
    }
}

}
