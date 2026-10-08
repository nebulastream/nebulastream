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

#include <CoalescingEmitPhysicalOperator.hpp>

#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/val.hpp>
#include <CoalescingEmitOperatorHandler.hpp>
#include <EmitPhysicalOperator.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <RangeCoalescer.hpp>
#include <function.hpp>

namespace NES
{

namespace
{
class CoalescingEmitState final : public EmitState
{
public:
    explicit CoalescingEmitState(const RecordBuffer& resultBuffer) : EmitState(resultBuffer) { }

    /// Whether `execute` emitted a full buffer, which splits the output into chunks.
    nautilus::val<bool> spilled = false;
};

void closeCoalescingProxy(
    OperatorHandler* handler, PipelineExecutionContext* pec, TupleBuffer* buffer, bool spilled, ChunkNumber chunkNumber, bool lastChunk)
{
    PRECONDITION(handler != nullptr, "Expects a valid handler");
    auto& emitHandler = dynamic_cast<CoalescingEmitOperatorHandler&>(*handler);
    /// Only the whole output of an unchunked input covers its range.
    if (chunkNumber == INITIAL_CHUNK_NUMBER && lastChunk && !spilled)
    {
        emitHandler.coalescer.offer(*buffer, *pec, RangeCoalescer::Clock::now());
        return;
    }
    emitHandler.setChunkNumber(true, chunkNumber, lastChunk, *buffer);
    pec->emitBuffer(*buffer);
}

void stopHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pec)
{
    PRECONDITION(handler != nullptr, "Expects a valid handler");
    handler->stop(QueryTerminationType::Graceful, *pec);
}
}

void CoalescingEmitPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer&) const
{
    ctx.setLocalOperatorState(id, std::make_unique<CoalescingEmitState>(RecordBuffer{ctx.allocateBuffer()}));
}

void CoalescingEmitPhysicalOperator::onFullBufferEmitted(EmitState& state) const
{
    dynamic_cast<CoalescingEmitState&>(state).spilled = true;
}

void CoalescingEmitPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer&) const
{
    auto& state = dynamic_cast<CoalescingEmitState&>(*ctx.getLocalState(id));
    setMetadata(ctx, state.resultBuffer, state.outputIndex);
    nautilus::invoke(
        closeCoalescingProxy,
        ctx.getGlobalOperatorHandler(getOperatorHandlerId()),
        ctx.pipelineContext,
        state.resultBuffer.getReference(),
        state.spilled,
        ctx.chunkNumber,
        ctx.lastChunk);
}

void CoalescingEmitPhysicalOperator::terminate(ExecutionContext& ctx) const
{
    nautilus::invoke(stopHandlerProxy, ctx.getGlobalOperatorHandler(getOperatorHandlerId()), ctx.pipelineContext);
}

}
