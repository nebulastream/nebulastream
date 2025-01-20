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
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Execution/Operators/Streaming/WindowOperatorBuild.hpp>
#include <Execution/Operators/Watermark/TimeFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <Execution/Operators/Streaming/WindowOperatorTriggerProbe.hpp>

namespace NES::Runtime::Execution::Operators
{
/// Updates the sliceState of all slices and emits buffers, if the slices can be emitted
void checkWindowsTriggerProxy(
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

    auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);
    const BufferMetaData bufferMetaData(watermarkTs, SequenceData(sequenceNumber, chunkNumber, lastChunk), originId);
    opHandler->checkAndTriggerWindows(bufferMetaData, pipelineCtx);
}

void WindowOperatorTriggerProbe::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Giving the chance for any child operator to open its state
    ExecutableOperator::open(executionCtx, recordBuffer);

    /// Update the watermark for the nlj operator and trigger slices
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(
        checkWindowsTriggerProxy,
        operatorHandlerMemRef,
        executionCtx.pipelineContext,
        recordBuffer.getWatermarkTs(),
        recordBuffer.getSequenceNumber(),
        recordBuffer.getChunkNumber(),
        recordBuffer.isLastChunk(),
        recordBuffer.getOriginId());
}

void WindowOperatorTriggerProbe::terminate(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(
        +[](OperatorHandler* ptrOpHandler, PipelineExecutionContext* piplineContext)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
            PRECONDITION(piplineContext != nullptr, "pipeline context should not be null");

            auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);
            opHandler->triggerAllWindows(piplineContext);
        },
        operatorHandlerMemRef,
        executionCtx.pipelineContext);
}

WindowOperatorTriggerProbe::WindowOperatorTriggerProbe(uint64_t operatorHandlerIndex)
    : operatorHandlerIndex(operatorHandlerIndex)
{
}
}