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

#include "IREEInferenceOperator.hpp"

#include <ranges>

#include <Configurations/Worker/QueryOptimizerConfiguration.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Util/Common.hpp>
#include <nautilus/function.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>

#include "IREEAdapter.hpp"
#include "IREEInferenceOperatorHandler.hpp"

namespace NES::QueryCompilation::PhysicalOperators
{
class PhysicalInferModelOperator;
}
namespace NES
{

template <class T>
void addValueToModel(int index, float value, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(index, value);
}

void copyVarSizedFromModel(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->copyResultTo(std::span{content, size});
}

void copyVarSizedToModel(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(std::span{content, size});
}

void applyModel(void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->infer();
}

float getValueFromModel(int index, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    return adapter->getResultAt(index);
}
template <typename T>
nautilus::val<T> min(const nautilus::val<T>& lhs, const nautilus::val<T>& rhs)
{
    return lhs < rhs ? lhs : rhs;
}

IREEInferenceOperator::IREEInferenceOperator(
    const OperatorHandlerId operatorHandlerId,
    std::vector<PhysicalFunction> inputs,
    std::vector<std::string> outputFieldNames,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> memoryProvider)
    : WindowProbePhysicalOperator(operatorHandlerId)
    , inputs(std::move(inputs))
    , outputFieldNames(std::move(outputFieldNames))
    , memoryProvider(std::move(memoryProvider))
{
}

void IREEInferenceOperator::performInference(
    const Interface::PagedVectorRef& pagedVectorRef,
    Interface::MemoryProvider::TupleBufferMemoryProvider& memoryProvider,
    ExecutionContext& executionCtx,
    nautilus::val<OperatorHandler*> operatorHandler) const
{
    const auto fields = memoryProvider.getMemoryLayout()->getSchema().getFieldNames();

    nautilus::val<int> rowIdx(0);
    for (auto it = pagedVectorRef.begin(fields); it != pagedVectorRef.end(fields); ++it)
    {
        auto record = createRecord(*it, fields);

        if (!this->isVarSizedInput)
        {
            for (nautilus::static_val<size_t> i = 0; i < inputs.size(); ++i)
            {
                nautilus::invoke(
                    addValueToModel<float>,
                    rowIdx,
                    inputs.at(i).execute(record, executionCtx.pipelineMemoryProvider.arena).cast<nautilus::val<float>>(),
                    operatorHandler,
                    executionCtx.workerThreadId);
                ++rowIdx;
            }
        }
        else
        {
            VarVal value = inputs.at(0).execute(record, executionCtx.pipelineMemoryProvider.arena);
            auto varSizedValue = value.cast<VariableSizedData>();
            nautilus::invoke(
                copyVarSizedToModel,
                varSizedValue.getContent(),
                min(varSizedValue.getContentSize(), nautilus::val<uint32_t>(static_cast<uint32_t>(this->inputSize))),
                operatorHandler,
                executionCtx.workerThreadId);
            ++rowIdx;
        }
    }

    nautilus::invoke(applyModel, operatorHandler, executionCtx.workerThreadId);

    rowIdx = nautilus::val<int>(0);
    for (auto it = pagedVectorRef.begin(fields); it != pagedVectorRef.end(fields); ++it)
    {
        auto record = createRecord(*it, fields);

        if (!this->isVarSizedOutput)
        {
            for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
            {
                VarVal result = VarVal(nautilus::invoke(getValueFromModel, rowIdx, operatorHandler, executionCtx.workerThreadId));
                record.write(outputFieldNames.at(i), result);
                ++rowIdx;
            }
        }
        else
        {
            auto output = executionCtx.pipelineMemoryProvider.arena.allocateVariableSizedData(this->outputSize);
            nautilus::invoke(copyVarSizedFromModel, output.getContent(), output.getContentSize(), operatorHandler, executionCtx.workerThreadId);
            record.write(outputFieldNames.at(0), output);
            ++rowIdx;
        }
        executeChild(executionCtx, record);
    }
}

void IREEInferenceOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    const auto emittedBatch = static_cast<nautilus::val<EmittedBatch*>>(recordBuffer.getBuffer());

    const auto workerThreadIdForPages = nautilus::val<WorkerThreadId>(WorkerThreadId(0));
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);

    const auto batchMemRef = nautilus::invoke(
            +[](OperatorHandler* ptrOpHandler, const EmittedBatch* batch)
            {
                PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
                const auto* opHandler = dynamic_cast<IREEInferenceOperatorHandler*>(ptrOpHandler);
                return opHandler->batches[batch->batchId].get();
            }, operatorHandler, emittedBatch);

    const auto batchPagedVectorMemRef = nautilus::invoke(
        +[](Batch* batch, const WorkerThreadId workerThreadId)
        {
            PRECONDITION(batch != nullptr, "batch context should not be null!");
            return batch->getPagedVectorRef(workerThreadId);
        },
        batchMemRef,
        workerThreadIdForPages);

    const Interface::PagedVectorRef batchPagedVectorRef(batchPagedVectorMemRef, memoryProvider);
    performInference(batchPagedVectorRef, *memoryProvider, executionCtx, operatorHandler);
}

void IREEInferenceOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PhysicalOperatorConcept::close(executionCtx, recordBuffer);
}

Record IREEInferenceOperator::createRecord(const Record& featureRecord, const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    Record record;
    for (const auto& fieldName : nautilus::static_iterable(projections))
    {
        record.write(fieldName, featureRecord.read(fieldName));
    }
    return record;
}

}
