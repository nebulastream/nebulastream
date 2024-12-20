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

#include <cstdint>
#include <memory>
#include <utility>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>

namespace NES::Runtime::Execution::Operators
{

/// Updates the sliceState of all slices and emits buffers, if the slices can be emitted
void passMetadataProxy(
    OperatorHandler* ptrOpHandler,
    PipelineExecutionContext* pipelineCtx,
    const Timestamp watermarkTs,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    const bool lastChunk,
    const OriginId originId)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");

    auto* opHandler = dynamic_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    const BufferMetaData bufferMetaData(watermarkTs, SequenceData(sequenceNumber, chunkNumber, lastChunk), originId);
    const auto globalWatermark = opHandler->setWatermark(bufferMetaData);

    auto tupleBuffer = pipelineCtx->getBufferManager()->getBufferBlocking();
    tupleBuffer.setNumberOfTuples(1);

    /// As we are here "emitting" a buffer, we have to set the originId, the seq number, and the watermark.
    tupleBuffer.setOriginId(opHandler->getOutputOriginId());
    tupleBuffer.setSequenceNumber(sequenceNumber);
    tupleBuffer.setChunkNumber(chunkNumber);
    tupleBuffer.setLastChunk(lastChunk);
    tupleBuffer.setWatermark(globalWatermark);

    pipelineCtx->emitBuffer(tupleBuffer, PipelineExecutionContext::ContinuationPolicy::NEVER);
}

void triggerAllWindowsProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* piplineContext)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

    auto* opHandler = dynamic_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    opHandler->triggerAllWindows(piplineContext);
}

StreamJoinBuild::StreamJoinBuild(
    const uint64_t operatorHandlerIndex,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider)
    : operatorHandlerIndex(operatorHandlerIndex)
    , joinBuildSide(joinBuildSide)
    , timeFunction(std::move(timeFunction))
    , memoryProvider(memoryProvider)
{
}

void StreamJoinBuild::setup(ExecutionContext& executionCtx) const
{
    nautilus::invoke(
        +[](const PipelineExecutionContext* pipelineExecutionContext)
        {
            std::ofstream pipelinesFile;
            pipelinesFile.open("pipelines.txt", std::ios_base::app);
            if (pipelinesFile.is_open())
            {
                pipelinesFile << "StreamJoinBuild pipelineId: " << pipelineExecutionContext->getPipelineId() << std::endl;
                pipelinesFile.flush();
            }
        },
        executionCtx.pipelineContext);
    ExecutableOperator::setup(executionCtx);
}

void StreamJoinBuild::close(ExecutionContext& executionCtx, RecordBuffer&) const
{
    /// Update the watermark for the nlj operator and trigger slices
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(
        passMetadataProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);
}

void StreamJoinBuild::terminate(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(triggerAllWindowsProxy, operatorHandlerMemRef, executionCtx.pipelineContext);
}
}
