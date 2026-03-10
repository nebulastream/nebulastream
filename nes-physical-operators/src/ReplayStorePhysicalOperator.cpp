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

#include <ReplayStorePhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

#include <DataTypes/Schema.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <ReplayStoreOperatorHandler.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

void setupHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pipelineCtx)
{
    if ((handler == nullptr) || (pipelineCtx == nullptr))
    {
        return;
    }
    handler->start(*pipelineCtx, 0);
}

void stopHandlerProxy(OperatorHandler* handler, PipelineExecutionContext* pipelineCtx)
{
    PRECONDITION(handler != nullptr, "OperatorHandler must not be null");
    PRECONDITION(pipelineCtx != nullptr, "PipelineExecutionContext must not be null");
    handler->stop(QueryTerminationType::Graceful, *pipelineCtx);
}

}

ReplayStorePhysicalOperator::ReplayStorePhysicalOperator(OperatorHandlerId handlerId, const Schema& inputSchema)
    : handlerId(handlerId), inputSchema(inputSchema)
{
    fieldNames.reserve(this->inputSchema.getNumberOfFields());
    fieldTypes.reserve(this->inputSchema.getNumberOfFields());
    fieldSizes.reserve(this->inputSchema.getNumberOfFields());
    fieldOffsets.reserve(this->inputSchema.getNumberOfFields());
    uint32_t offset = 0;
    for (const auto& f : this->inputSchema.getFields())
    {
        fieldNames.emplace_back(f.name);
        fieldTypes.emplace_back(f.dataType);
        uint32_t sz = 0;
        switch (f.dataType.type)
        {
            case DataType::Type::VARSIZED:
                sz = 0; /// TODO #11: Add Varsized Support
                break;
            default:
                sz = f.dataType.getSizeInBytesWithoutNull();
        }
        fieldSizes.emplace_back(sz);
        fieldOffsets.emplace_back(offset);
        offset += sz;
    }
    rowWidth = offset;
}

void ReplayStorePhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    if (child.has_value())
    {
        setupChild(executionCtx, compilationContext);
    }
    nautilus::invoke(setupHandlerProxy, executionCtx.getGlobalOperatorHandler(handlerId), executionCtx.pipelineContext);
}

void ReplayStorePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (child.has_value())
    {
        openChild(executionCtx, recordBuffer);
    }
    auto handler = executionCtx.getGlobalOperatorHandler(handlerId);
    nautilus::invoke(
        +[](OperatorHandler* h, PipelineExecutionContext* pctx)
        {
            if (!h || !pctx)
            {
                return;
            }
            if (auto* store = dynamic_cast<ReplayStoreOperatorHandler*>(h))
            {
                store->ensureHeader(*pctx);
            }
        },
        handler,
        executionCtx.pipelineContext);
}

void ReplayStorePhysicalOperator::encodeAndAppend(Record& record, ExecutionContext& executionCtx) const
{
    auto handler = executionCtx.getGlobalOperatorHandler(handlerId);

    /// Get a reusable row buffer from the handler.
    auto bufferPtr = nautilus::invoke(
        +[](OperatorHandler* h, uint32_t width) -> int8_t*
        {
            return dynamic_cast<ReplayStoreOperatorHandler*>(h)->getRowBuffer(width);
        },
        handler,
        nautilus::val<uint32_t>(rowWidth));

    /// Write each field into the buffer at its precomputed offset.
    for (nautilus::static_val<size_t> i = 0; i < fieldNames.size(); ++i)
    {
        if (fieldSizes[i] == 0)
        {
            /// TODO #11: Add Varsized Support
            continue;
        }
        const auto& vv = record.read(fieldNames[i]);
        vv.writeToMemory(bufferPtr + nautilus::static_val<int32_t>(fieldOffsets[i]));
    }

    /// Commit the encoded row to the store.
    nautilus::invoke(
        +[](OperatorHandler* h, uint32_t len)
        {
            dynamic_cast<ReplayStoreOperatorHandler*>(h)->commitRow(len);
        },
        handler,
        nautilus::val<uint32_t>(rowWidth));
}

void ReplayStorePhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    encodeAndAppend(record, executionCtx);
    if (child.has_value())
    {
        executeChild(executionCtx, record);
    }
}

void ReplayStorePhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (child.has_value())
    {
        closeChild(executionCtx, recordBuffer);
    }
}

void ReplayStorePhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(stopHandlerProxy, executionCtx.getGlobalOperatorHandler(handlerId), executionCtx.pipelineContext);
    if (child.has_value())
    {
        terminateChild(executionCtx);
    }
}

std::optional<PhysicalOperator> ReplayStorePhysicalOperator::getChild() const
{
    return child;
}

void ReplayStorePhysicalOperator::setChild(PhysicalOperator c)
{
    child = std::move(c);
}

}
