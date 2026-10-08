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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <CoalescingEmitOperatorHandler.hpp>
#include <CoalescingEmitPhysicalOperator.hpp>
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

/// Copies the chain with its closing emit replaced by a `CoalescingEmitPhysicalOperator`, keeping all ids, and replaces the emit's handler
/// in `handlers`. Returns nullopt if the chain does not end in an emit whose buffers can be concatenated.
std::optional<PhysicalOperator> withCoalescingEmit(
    const PhysicalOperator& op,
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers,
    const std::chrono::microseconds maxDelay)
{
    if (const auto child = op.getChild())
    {
        const auto rebuiltChild = withCoalescingEmit(*child, handlers, maxDelay);
        return rebuiltChild ? std::optional(op.withChild(*rebuiltChild)) : std::nullopt;
    }
    const auto emit = op.tryGet<EmitPhysicalOperator>();
    if (!emit)
    {
        return std::nullopt;
    }
    auto layout = emit->getBufferLayout();
    if (!layout)
    {
        return std::nullopt;
    }
    handlers.insert_or_assign(
        emit->getOperatorHandlerId(),
        std::make_shared<CoalescingEmitOperatorHandler>(std::move(*layout), maxDelay, MAX_HELD_RUNS_PER_ORIGIN));
    return PhysicalOperator{CoalescingEmitPhysicalOperator(*emit)};
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
            if (const auto root = withCoalescingEmit(pipeline->getRootOperator(), pipeline->getOperatorHandlers(), maxDelay))
            {
                pipeline->setRootOperator(*root);
            }
        }
        pending.insert(pending.end(), pipeline->getSuccessors().begin(), pipeline->getSuccessors().end());
    }
}

}
