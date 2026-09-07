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

#include <cstdint>
#include <memory>
#include <Align/AlignEagerLEOperatorHandler.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <nautilus/val.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <function.hpp>
#include <val_ptr.hpp>

namespace NES
{

class AlignEagerLEEmitState : public OperatorState
{
public:
    explicit AlignEagerLEEmitState(const RecordBuffer& resultBuffer) : resultBuffer(resultBuffer) { }

    nautilus::val<uint64_t> outputIndex = 0;
    RecordBuffer resultBuffer;
};

inline void openAlignEagerLEEmit(ExecutionContext& ctx, OperatorId id)
{
    const RecordBuffer resultBuffer{ctx.allocateBuffer()};
    ctx.setLocalOperatorState(id, std::make_unique<AlignEagerLEEmitState>(resultBuffer));
}

inline void emitAlignEagerLERecordBuffer(
    ExecutionContext& ctx,
    OperatorHandlerId operatorHandlerId,
    RecordBuffer& recordBuffer,
    const nautilus::val<uint64_t>& numRecords,
    const nautilus::val<bool>& potentialLastChunk)
{
    recordBuffer.setNumRecords(numRecords);
    recordBuffer.setWatermarkTs(ctx.watermarkTs);
    recordBuffer.setOriginId(ctx.originId);
    recordBuffer.setSequenceNumber(ctx.sequenceNumber);
    recordBuffer.setCreationTs(ctx.currentTs);

    nautilus::invoke(
        +[](OperatorHandler* handler,
            bool closesChunk,
            ChunkNumber currentChunkNumber,
            bool isCurrentBufferTheLastChunk,
            TupleBuffer* newBuffer)
        {
            PRECONDITION(handler != nullptr, "Expects a valid handler");
            PRECONDITION(newBuffer != nullptr, "Expects a valid buffer");
            PRECONDITION(currentChunkNumber != INVALID<ChunkNumber>, "Expects a valid chunkNumber");
            dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).setChunkNumber(
                closesChunk, currentChunkNumber, isCurrentBufferTheLastChunk, *newBuffer);
        },
        ctx.getGlobalOperatorHandler(operatorHandlerId),
        potentialLastChunk,
        ctx.chunkNumber,
        ctx.lastChunk,
        recordBuffer.getReference());

    ctx.emitBuffer(recordBuffer);
}

inline void emitAlignEagerLERecord(
    ExecutionContext& ctx,
    OperatorId id,
    OperatorHandlerId operatorHandlerId,
    const std::shared_ptr<TupleBufferRef>& bufferRef,
    Record& record)
{
    auto* const state = dynamic_cast<AlignEagerLEEmitState*>(ctx.getLocalState(id));
    auto writeResult = bufferRef->writeRecord(state->outputIndex, state->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    if (!writeResult.successful)
    {
        emitAlignEagerLERecordBuffer(ctx, operatorHandlerId, state->resultBuffer, state->outputIndex, false);
        state->resultBuffer = RecordBuffer{ctx.allocateBuffer()};
        state->outputIndex = nautilus::val<uint64_t>(0);
        writeResult = bufferRef->writeRecord(state->outputIndex, state->resultBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);
    }
    state->outputIndex = state->outputIndex + writeResult.writtenRecords;
}

inline void closeAlignEagerLEEmit(ExecutionContext& ctx, OperatorId id, OperatorHandlerId operatorHandlerId)
{
    auto* const state = dynamic_cast<AlignEagerLEEmitState*>(ctx.getLocalState(id));
    emitAlignEagerLERecordBuffer(ctx, operatorHandlerId, state->resultBuffer, state->outputIndex, true);
}

}
