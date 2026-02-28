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
#include <WindowBuildPhysicalOperator.hpp>

#include <memory>
#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <function.hpp>

namespace NES
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

void triggerAllWindowsProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* piplineContext)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(piplineContext != nullptr, "pipeline context should not be null");

    auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);
    opHandler->triggerAllWindows(piplineContext);
}

/// The slice store needs to know in how many pipelines this operator appears, and consequently, how many terminations it will receive
void registerActivePipeline(OperatorHandler* ptrOpHandler, const uint64_t sliceCacheMemorySize, AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(bufferProvider != nullptr, "BufferProvider should not be null!");
    auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);
    opHandler->getSliceAndWindowStore().incrementNumberOfInputPipelines();
    opHandler->allocateSpaceForSliceCache(sliceCacheMemorySize, bufferProvider);
}

WindowBuildPhysicalOperator::WindowBuildPhysicalOperator(
    OperatorHandlerId operatorHandlerId, std::unique_ptr<TimeFunction> timeFunction, SliceCacheConfiguration sliceCacheConfiguration)
    : operatorHandlerId(operatorHandlerId)
    , timeFunction(std::move(timeFunction))
    , sliceCache(SliceCache::createSliceCache(sliceCacheConfiguration))
{
}

void WindowBuildPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer&) const
{
    /// Update the watermark for the nlj operator and trigger slices
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
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

void WindowBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        registerActivePipeline,
        operatorHandler,
        nautilus::val<uint64_t>{sliceCache->getCacheMemorySize()},
        executionCtx.pipelineMemoryProvider.bufferProvider);

    const auto sliceCacheMemory = nautilus::invoke(
        {.modRefInfo = nautilus::ModRefInfo::Ref, .noUnwind = true},
        +[](const OperatorHandler* ptrOpHandler, const WorkerThreadId workerThreadId)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
            auto* opHandler = dynamic_cast<const WindowBasedOperatorHandler*>(ptrOpHandler);
            return opHandler->getSliceCache(workerThreadId);
        },
        operatorHandler);
    sliceCache->setStartOfEntries(sliceCacheMemory);
};

void WindowBuildPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Initializing the time function
    timeFunction->open(executionCtx, recordBuffer);

    /// Creating the local state for the window operator build.
    const auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    executionCtx.setLocalOperatorState(id, std::make_unique<WindowOperatorBuildLocalState>(operatorHandler, std::move(sliceCache)));
}

void WindowBuildPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(triggerAllWindowsProxy, operatorHandlerMemRef, executionCtx.pipelineContext);
}

std::optional<PhysicalOperator> WindowBuildPhysicalOperator::getChild() const
{
    return child;
}

void WindowBuildPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

WindowBuildPhysicalOperator::WindowBuildPhysicalOperator(const WindowBuildPhysicalOperator& other)
    : PhysicalOperatorConcept(other.id)
    , child(other.child)
    , operatorHandlerId(other.operatorHandlerId)
    , timeFunction(other.timeFunction ? other.timeFunction->clone() : nullptr)
    , sliceCacheConfiguration(other.sliceCacheConfiguration)
{
}
}
