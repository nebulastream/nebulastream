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
#include <WindowProbePhysicalOperator.hpp>

#include <optional>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <function.hpp>

namespace NES
{

namespace
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

void setupProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null!");

    auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);
    opHandler->start(*pipelineCtx, 0);
    opHandler->setWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
}

void terminateProxy(OperatorHandler* ptrOpHandler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(pipelineCtx != nullptr, "pipeline context should not be null");

    auto* opHandler = dynamic_cast<WindowBasedOperatorHandler*>(ptrOpHandler);
    opHandler->stop(QueryTerminationType::Graceful, *pipelineCtx);
}
}

WindowProbePhysicalOperator::WindowProbePhysicalOperator(OperatorHandlerId operatorHandlerId, WindowMetaData windowMetaData)
    : operatorHandlerId(operatorHandlerId), windowMetaData(std::move(windowMetaData))
{
}

std::optional<PhysicalOperator> WindowProbePhysicalOperator::getChild() const
{
    return child;
}

WindowProbePhysicalOperator WindowProbePhysicalOperator::withChild(PhysicalOperator newChild) const
{
    auto copy = *this;
    copy.child = std::move(newChild);
    return copy;
}

void WindowProbePhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    /// Giving child operators the change to setup
    setupChild(executionCtx, compilationContext);
    invoke(setupProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerId), executionCtx.pipelineContext);
}

void WindowProbePhysicalOperator::open(ExecutionContext& ctx, RecordBuffer& recordBuffer) const
{
    openChild(ctx, recordBuffer);
}

void WindowProbePhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Update the watermark for the probe and delete all slices that can be deleted
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    invoke(
        garbageCollectSlicesProxy,
        operatorHandlerMemRef,
        executionCtx.watermarkTs,
        executionCtx.sequenceNumber,
        executionCtx.chunkNumber,
        executionCtx.lastChunk,
        executionCtx.originId);

    /// Now close for all children
    closeChild(executionCtx, recordBuffer);
}

void WindowProbePhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(terminateProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerId), executionCtx.pipelineContext);
    terminateChild(executionCtx);
}

void WindowProbePhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    executeChild(ctx, record);
}

void WindowProbePhysicalOperator::setupChild(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().setup(executionCtx, compilationContext);
}

void WindowProbePhysicalOperator::openChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().open(executionCtx, recordBuffer);
}

void WindowProbePhysicalOperator::closeChild(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().close(executionCtx, recordBuffer);
}

void WindowProbePhysicalOperator::executeChild(ExecutionContext& executionCtx, Record& record) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().execute(executionCtx, record);
}

void WindowProbePhysicalOperator::terminateChild(ExecutionContext& executionCtx) const
{
    INVARIANT(child.has_value(), "Child operator is not set");
    child.value().terminate(executionCtx);
}
}
