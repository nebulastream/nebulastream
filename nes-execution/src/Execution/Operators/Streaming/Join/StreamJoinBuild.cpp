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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinBuild.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinOperatorHandler.hpp>

namespace NES::Runtime::Execution::Operators
{

/// Updates the sliceState of all slices and emits buffers, if the slices can be emitted
void checkWindowsTriggerProxy(
    OperatorHandler* ptrOpHandler,
    PipelineExecutionContext* pipelineCtx,
    const Timestamp::Underlying watermarkTs,
    const SequenceNumber::Underlying sequenceNumber,
    const ChunkNumber::Underlying chunkNumber,
    const bool lastChunk,
    const OriginId::Underlying originId)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");

    auto* opHandler = dynamic_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    const BufferMetaData bufferMetaData(
        watermarkTs, SequenceData(SequenceNumber(sequenceNumber), ChunkNumber(chunkNumber), lastChunk), OriginId(originId));
    opHandler->checkAndTriggerWindows(bufferMetaData, pipelineCtx);
}

void triggerAllWindowsProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* piplineContext)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

    auto* opHandler = dynamic_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    opHandler->triggerAllWindows(piplineContext);
}

void setupProxy(OperatorHandler* ptrOpHandler, const PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

    auto* opHandler = dynamic_cast<StreamJoinOperatorHandler*>(ptrOpHandler);
    opHandler->setBufferProvider(pipelineCtx->getBufferManager());
    opHandler->setWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
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

void StreamJoinBuild::close(ExecutionContext& executionCtx, RecordBuffer&) const
{
    /// Update the watermark for the nlj operator and trigger slices
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(
        checkWindowsTriggerProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);
}

void StreamJoinBuild::setup(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(setupProxy, operatorHandlerMemRef, executionCtx.pipelineContext);
}

void StreamJoinBuild::terminate(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(triggerAllWindowsProxy, operatorHandlerMemRef, executionCtx.pipelineContext);
}
}
