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

#include <Align/AlignNNAnchorPhysicalOperator.hpp>

#include <cstdint>
#include <optional>
#include <utility>
#include <Align/AlignNNEmit.hpp>
#include <Align/AlignNNMatch.hpp>
#include <Align/AlignNNOperatorHandler.hpp>
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

AlignNNAnchorPhysicalOperator::AlignNNAnchorPhysicalOperator(
    OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> leftTimeFunction,
    std::unique_ptr<TimeFunction> rightTimeFunction,
    std::shared_ptr<PagedVectorTupleLayout> pendingLeftLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightLogLayout,
    std::shared_ptr<TupleBufferRef> outputBufferRef)
    : operatorHandlerId(operatorHandlerId)
    , leftTimeFunction(std::move(leftTimeFunction))
    , rightTimeFunction(std::move(rightTimeFunction))
    , pendingLeftLayout(std::move(pendingLeftLayout))
    , rightLogLayout(std::move(rightLogLayout))
    , outputBufferRef(std::move(outputBufferRef))
{
}

AlignNNAnchorPhysicalOperator::AlignNNAnchorPhysicalOperator(const AlignNNAnchorPhysicalOperator& other)
    : PhysicalOperatorConcept(other.id)
    , operatorHandlerId(other.operatorHandlerId)
    , leftTimeFunction(other.leftTimeFunction ? other.leftTimeFunction->clone() : nullptr)
    , rightTimeFunction(other.rightTimeFunction ? other.rightTimeFunction->clone() : nullptr)
    , pendingLeftLayout(other.pendingLeftLayout)
    , rightLogLayout(other.rightLogLayout)
    , outputBufferRef(other.outputBufferRef)
    , child(other.child)
{
}

void AlignNNAnchorPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    const auto handlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    nautilus::invoke(
        +[](OperatorHandler* handler, PipelineExecutionContext* pipelineCtx)
        {
            PRECONDITION(handler != nullptr, "Expects a valid handler");
            PRECONDITION(pipelineCtx != nullptr, "Expects a valid pipeline execution context");
            dynamic_cast<AlignNNOperatorHandler&>(*handler).ensureBuffersAllocated(*pipelineCtx);
        },
        handlerMemRef,
        executionCtx.pipelineContext);
}

void AlignNNAnchorPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer&) const
{
    openAlignNNEmit(ctx, id);
}

void AlignNNAnchorPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer&) const
{
    closeAlignNNEmit(ctx, id, operatorHandlerId);
}

void AlignNNAnchorPhysicalOperator::terminate(ExecutionContext&) const
{
}

void AlignNNAnchorPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto handler = ctx.getGlobalOperatorHandler(operatorHandlerId);

    nautilus::invoke(+[](OperatorHandler* handler) { dynamic_cast<AlignNNOperatorHandler&>(*handler).lock(); }, handler);

    const auto pendingLeftBufPtr = nautilus::invoke(
        +[](OperatorHandler* handler) -> const TupleBuffer*
        { return dynamic_cast<AlignNNOperatorHandler&>(*handler).getPendingLeftBuffer(); },
        handler);
    PagedVectorRef pendingLeftRef{BorrowedNautilusBuffer::from(pendingLeftBufPtr), pendingLeftLayout};
    pendingLeftRef.pushBack(record, ctx.pipelineMemoryProvider.bufferProvider);

    tryResolvePendingLeft(
        ctx,
        operatorHandlerId,
        *leftTimeFunction,
        *rightTimeFunction,
        pendingLeftLayout,
        rightLogLayout,
        [this](ExecutionContext& c, Record& r) { emitAlignNNRecord(c, id, operatorHandlerId, outputBufferRef, r); });

    nautilus::invoke(+[](OperatorHandler* handler) { dynamic_cast<AlignNNOperatorHandler&>(*handler).unlock(); }, handler);
}

std::optional<PhysicalOperator> AlignNNAnchorPhysicalOperator::getChild() const
{
    return child;
}

void AlignNNAnchorPhysicalOperator::setChild(PhysicalOperator childOperator)
{
    this->child = std::move(childOperator);
}

}
