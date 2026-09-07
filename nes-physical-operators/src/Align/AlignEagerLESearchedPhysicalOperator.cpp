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

#include <Align/AlignEagerLESearchedPhysicalOperator.hpp>

#include <optional>
#include <utility>
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
#include <val_ptr.hpp>

namespace NES
{

AlignEagerLESearchedPhysicalOperator::AlignEagerLESearchedPhysicalOperator(
    OperatorHandlerId operatorHandlerId, std::shared_ptr<TupleBufferRef> searchedStateLayout)
    : operatorHandlerId(operatorHandlerId), searchedStateLayout(std::move(searchedStateLayout))
{
}

void AlignEagerLESearchedPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
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

void AlignEagerLESearchedPhysicalOperator::open(ExecutionContext&, RecordBuffer&) const
{
}

void AlignEagerLESearchedPhysicalOperator::close(ExecutionContext&, RecordBuffer&) const
{
}

void AlignEagerLESearchedPhysicalOperator::terminate(ExecutionContext&) const
{
}

void AlignEagerLESearchedPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto handler = ctx.getGlobalOperatorHandler(operatorHandlerId);

    nautilus::invoke(+[](OperatorHandler* handler) { dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).lock(); }, handler);

    const auto stateBufPtr = nautilus::invoke(
        +[](OperatorHandler* handler) -> const TupleBuffer*
        { return dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).getSearchedStateBuffer(); },
        handler);
    RecordBuffer stateRecordBuffer{BorrowedNautilusBuffer::from(stateBufPtr)};
    auto writeIndex = nautilus::val<uint64_t>(0);
    searchedStateLayout->writeRecord(writeIndex, stateRecordBuffer, record, ctx.pipelineMemoryProvider.bufferProvider);

    nautilus::invoke(
        +[](OperatorHandler* handler) { dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).setHasSearchedRecord(true); }, handler);

    nautilus::invoke(+[](OperatorHandler* handler) { dynamic_cast<AlignEagerLEOperatorHandler&>(*handler).unlock(); }, handler);
}

std::optional<PhysicalOperator> AlignEagerLESearchedPhysicalOperator::getChild() const
{
    return child;
}

void AlignEagerLESearchedPhysicalOperator::setChild(PhysicalOperator childOperator)
{
    this->child = std::move(childOperator);
}

}
