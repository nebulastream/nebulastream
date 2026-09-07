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

#include <Align/AlignEagerLEDrivingPhysicalOperator.hpp>

#include <optional>
#include <utility>
#include <Align/AlignEagerLEEmit.hpp>
#include <Align/AlignEagerLEOperatorHandler.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

AlignEagerLEDrivingPhysicalOperator::AlignEagerLEDrivingPhysicalOperator(
    OperatorHandlerId operatorHandlerId,
    std::shared_ptr<TupleBufferRef> searchedStateLayout,
    std::shared_ptr<TupleBufferRef> outputBufferRef)
    : operatorHandlerId(operatorHandlerId)
    , searchedStateLayout(std::move(searchedStateLayout))
    , searchedProjections(this->searchedStateLayout->getAllFieldNames())
    , outputBufferRef(std::move(outputBufferRef))
{
}

void AlignEagerLEDrivingPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    const auto handlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    nautilus::invoke(
        +[](OperatorHandler* handler, PipelineExecutionContext* pipelineCtx)
        {
            PRECONDITION(handler != nullptr, "Expects a valid handler");
            PRECONDITION(pipelineCtx != nullptr, "Expects a valid pipeline execution context");
            dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).ensureBufferAllocated(*pipelineCtx);
        },
        handlerMemRef,
        executionCtx.pipelineContext);
}

void AlignEagerLEDrivingPhysicalOperator::open(ExecutionContext& ctx, RecordBuffer&) const
{
    openAlignEagerLEEmit(ctx, id);
}

void AlignEagerLEDrivingPhysicalOperator::close(ExecutionContext& ctx, RecordBuffer&) const
{
    closeAlignEagerLEEmit(ctx, id, operatorHandlerId);
}

void AlignEagerLEDrivingPhysicalOperator::terminate(ExecutionContext&) const
{
}

void AlignEagerLEDrivingPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto handler = ctx.getGlobalOperatorHandler(operatorHandlerId);

    nautilus::invoke(+[](OperatorHandler* handler) { dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).lock(); }, handler);

    const auto hasSearched = nautilus::invoke(
        +[](OperatorHandler* handler) { return dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).hasSearchedRecord(); }, handler);

    if (hasSearched)
    {
        const auto stateBufPtr = nautilus::invoke(
            +[](OperatorHandler* handler) -> const TupleBuffer*
            { return dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).getSearchedStateBuffer(); },
            handler);
        RecordBuffer stateRecordBuffer{BorrowedNautilusBuffer::from(stateBufPtr)};
        auto readIndex = nautilus::val<uint64_t>(0);
        auto searchedRecord = searchedStateLayout->readRecord(searchedProjections, stateRecordBuffer, readIndex);

        Record output;
        output.reassignFields(record);
        output.reassignFields(searchedRecord);
        emitAlignEagerLERecord(ctx, id, operatorHandlerId, outputBufferRef, output);
    }

    nautilus::invoke(+[](OperatorHandler* handler) { dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).unlock(); }, handler);
}

std::optional<PhysicalOperator> AlignEagerLEDrivingPhysicalOperator::getChild() const
{
    return child;
}

void AlignEagerLEDrivingPhysicalOperator::setChild(PhysicalOperator childOperator)
{
    this->child = std::move(childOperator);
}

}
