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
#include <cstddef>
#include <memory>
#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>
#include <CoalescingEmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <Pipeline.hpp>
#include <PipelinedQueryPlan.hpp>

namespace NES::QueryCompilation::EmitCoalescingPhase
{

namespace
{
/// Bounds the pooled buffers a coalescing emit holds per origin.
constexpr size_t MAX_HELD_RUNS_PER_ORIGIN = 64;

std::optional<EmitPhysicalOperator> closingEmit(const Pipeline& pipeline)
{
    auto current = pipeline.getRootOperator();
    while (const auto child = current.getChild())
    {
        current = *child;
    }
    return current.tryGet<EmitPhysicalOperator>();
}
}

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
            if (const auto emit = closingEmit(*pipeline); emit && CoalescingEmitOperatorHandler::canReadBack(*emit->getBufferRef()))
            {
                pipeline->getOperatorHandlers().insert_or_assign(
                    emit->getOperatorHandlerId(),
                    std::make_shared<CoalescingEmitOperatorHandler>(emit->getBufferRef(), maxDelay, MAX_HELD_RUNS_PER_ORIGIN));
            }
        }
        pending.insert(pending.end(), pipeline->getSuccessors().begin(), pipeline->getSuccessors().end());
    }
}

}
