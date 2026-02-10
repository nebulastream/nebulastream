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

#include <StorePhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <PipelineExecutionContext.hpp>
#include <StoreOperatorHandler.hpp>
#include <function.hpp>
#include <val_ptr.hpp>
#include "CompilationContext.hpp"
#include "DataTypes/Schema.hpp"

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

StorePhysicalOperator::StorePhysicalOperator(OperatorHandlerId handlerId, const Schema& inputSchema)
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
                sz = f.dataType.getSizeInBytes();
        }
        fieldSizes.emplace_back(sz);
        fieldOffsets.emplace_back(offset);
        offset += sz;
    }
    rowWidth = offset;
}

void StorePhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    if (child.has_value())
    {
        setupChild(executionCtx, compilationContext);
    }
    nautilus::invoke(setupHandlerProxy, executionCtx.getGlobalOperatorHandler(handlerId), executionCtx.pipelineContext);
}

void StorePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
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
            if (auto* store = dynamic_cast<StoreOperatorHandler*>(h))
            {
                store->ensureHeader(*pctx);
            }
        },
        handler,
        executionCtx.pipelineContext);
}

void StorePhysicalOperator::encodeAndAppend(Record& record, ExecutionContext& executionCtx) const
{
    std::vector<uint8_t> row;
    row.resize(rowWidth);
    for (size_t i = 0; i < fieldNames.size(); ++i)
    {
        const auto sz = fieldSizes[i];
        if (sz == 0)
        {
            /// TODO #11: Add Varsized Support
            continue;
        }
        const auto name = fieldNames[i];
        auto* const dstPtr = reinterpret_cast<int8_t*>(row.data() + fieldOffsets[i]);
        auto dstVal = nautilus::val<int8_t*>(dstPtr);
        const auto& vv = record.read(name);
        vv.writeToMemory(dstVal);
    }

    auto handler = executionCtx.getGlobalOperatorHandler(handlerId);
    auto dataPtr = nautilus::val<int8_t*>(reinterpret_cast<int8_t*>(row.data()));
    nautilus::invoke(
        +[](OperatorHandler* h, int8_t* data, uint32_t len)
        {
            if (auto* store = dynamic_cast<StoreOperatorHandler*>(h))
            {
                store->append(reinterpret_cast<const uint8_t*>(data), static_cast<size_t>(len));
            }
        },
        handler,
        dataPtr,
        nautilus::val<uint32_t>(rowWidth));
}

void StorePhysicalOperator::execute(ExecutionContext& executionCtx, Record& record) const
{
    encodeAndAppend(record, executionCtx);
    if (child.has_value())
    {
        executeChild(executionCtx, record);
    }
}

void StorePhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    if (child.has_value())
    {
        closeChild(executionCtx, recordBuffer);
    }
}

void StorePhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(stopHandlerProxy, executionCtx.getGlobalOperatorHandler(handlerId), executionCtx.pipelineContext);
    if (child.has_value())
    {
        terminateChild(executionCtx);
    }
}

std::optional<PhysicalOperator> StorePhysicalOperator::getChild() const
{
    return child;
}

void StorePhysicalOperator::setChild(PhysicalOperator c)
{
    child = std::move(c);
}

}
