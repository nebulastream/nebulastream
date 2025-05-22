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
#include <utility>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/WindowBasedOperatorHandler.hpp>
#include <Execution/Operators/Streaming/WindowOperatorProbe.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Operators/LogicalOperators/Windows/WindowOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Execution.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>

namespace NES::Runtime::Execution::Operators
{


void garbageCollectSlicesProxy(
    OperatorHandler* ptrOpHandler,
    const Timestamp watermarkTs,
    const SequenceNumber sequenceNumber,
    const ChunkNumber chunkNumber,
    const bool lastChunk,
    const OriginId originId)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

    const auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);
    const BufferMetaData bufferMetaData(watermarkTs, SequenceData(sequenceNumber, chunkNumber, lastChunk), originId);

    opHandler->garbageCollectSlicesAndWindows(bufferMetaData);
}

void deleteAllSlicesAndWindowsProxy(OperatorHandler* ptrOpHandler)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");

    const auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);
    opHandler->getSliceAndWindowStore().deleteState();
}

void setupProxy(OperatorHandler* ptrOpHandler, const PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null!");

    auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);
    opHandler->setWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
}

WindowOperatorProbe::WindowOperatorProbe(const uint64_t operatorHandlerIndex, WindowMetaData windowMetaData)
    : operatorHandlerIndex(operatorHandlerIndex), windowMetaData(std::move(windowMetaData))
{
}

void WindowOperatorProbe::setup(ExecutionContext& executionCtx) const
{
    /// Giving child operators the change to setup
    Operator::setup(executionCtx);
    nautilus::invoke(setupProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerIndex), executionCtx.pipelineContext);
}

void WindowOperatorProbe::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Update the watermark for the probe and delete all slices that can be deleted
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    invoke(
        garbageCollectSlicesProxy,
        operatorHandlerMemRef,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);

    /// Now close for all children
    Operator::close(executionCtx, recordBuffer);
}
}
