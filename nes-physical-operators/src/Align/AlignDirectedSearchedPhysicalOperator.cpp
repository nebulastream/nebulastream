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

#include <Align/AlignDirectedSearchedPhysicalOperator.hpp>

#include <optional>
#include <utility>
#include <Align/AlignDirectedEmit.hpp>
#include <Align/AlignDirectedMatch.hpp>
#include <Align/AlignDirectedOperatorHandler.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

AlignDirectedSearchedPhysicalOperator::AlignDirectedSearchedPhysicalOperator(
    OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> drivingTimeFunction,
    std::unique_ptr<TimeFunction> searchedTimeFunction,
    std::shared_ptr<PagedVectorTupleLayout> pendingDrivingLayout,
    std::shared_ptr<PagedVectorTupleLayout> searchedLogLayout,
    std::shared_ptr<TupleBufferRef> outputBufferRef)
    : operatorHandlerId(operatorHandlerId)
    , drivingTimeFunction(std::move(drivingTimeFunction))
    , searchedTimeFunction(std::move(searchedTimeFunction))
    , pendingDrivingLayout(std::move(pendingDrivingLayout))
    , searchedLogLayout(std::move(searchedLogLayout))
    , outputBufferRef(std::move(outputBufferRef))
{
}

AlignDirectedSearchedPhysicalOperator::AlignDirectedSearchedPhysicalOperator(const AlignDirectedSearchedPhysicalOperator& other)
    : PhysicalOperatorConcept(other.id)
    , operatorHandlerId(other.operatorHandlerId)
    , drivingTimeFunction(other.drivingTimeFunction ? other.drivingTimeFunction->clone() : nullptr)
    , searchedTimeFunction(other.searchedTimeFunction ? other.searchedTimeFunction->clone() : nullptr)
    , pendingDrivingLayout(other.pendingDrivingLayout)
    , searchedLogLayout(other.searchedLogLayout)
    , outputBufferRef(other.outputBufferRef)
    , child(other.child)
{
}

void AlignDirectedSearchedPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    const auto handlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    nautilus::invoke(
        +[](OperatorHandler* handler, PipelineExecutionContext* pipelineCtx)
        {
            PRECONDITION(handler != nullptr, "Expects a valid handler");
            PRECONDITION(pipelineCtx != nullptr, "Expects a valid pipeline execution context");
            dynamic_cast<AlignDirectedOperatorHandler&>(*handler).ensureBuffersAllocated(*pipelineCtx);
        },
        handlerMemRef,
        executionCtx.pipelineContext);
}

void AlignDirectedSearchedPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer&) const
{
    openAlignDirectedEmit(ctx, id);
}

void AlignDirectedSearchedPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer&) const
{
    closeAlignDirectedEmit(ctx, id, operatorHandlerId);
}

void AlignDirectedSearchedPhysicalOperator::terminate(ExecutionContext&) const
{
}

void AlignDirectedSearchedPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto handler = ctx.getGlobalOperatorHandler(operatorHandlerId);

    nautilus::invoke(+[](OperatorHandler* handler) { dynamic_cast<AlignDirectedOperatorHandler&>(*handler).lock(); }, handler);

    const auto searchedLogBufPtr = nautilus::invoke(
        +[](OperatorHandler* handler) -> const TupleBuffer*
        { return dynamic_cast<AlignDirectedOperatorHandler&>(*handler).getSearchedLogBuffer(); },
        handler);
    PagedVectorRef searchedLogRef{BorrowedNautilusBuffer::from(searchedLogBufPtr), searchedLogLayout};
    searchedLogRef.pushBack(record, ctx.pipelineMemoryProvider.bufferProvider);

    tryResolvePendingDriving(
        ctx,
        operatorHandlerId,
        *drivingTimeFunction,
        *searchedTimeFunction,
        pendingDrivingLayout,
        searchedLogLayout,
        [this](ExecutionContext& c, Record& r) { emitAlignDirectedRecord(c, id, operatorHandlerId, outputBufferRef, r); });

    nautilus::invoke(+[](OperatorHandler* handler) { dynamic_cast<AlignDirectedOperatorHandler&>(*handler).unlock(); }, handler);
}

std::optional<PhysicalOperator> AlignDirectedSearchedPhysicalOperator::getChild() const
{
    return child;
}

void AlignDirectedSearchedPhysicalOperator::setChild(PhysicalOperator childOperator)
{
    this->child = std::move(childOperator);
}

}
