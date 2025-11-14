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

#include <ExecutionContext.hpp>
#include <IREEAdapter.hpp>
#include <IREEBatchInferenceOperator.hpp>
#include <IREEBatchInferenceOperatorHandler.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <nautilus/function.hpp>
#include <ranges>

namespace NES::QueryCompilation::PhysicalOperators
{
class PhysicalInferModelOperator;
}

namespace NES::IREEBatchInference
{
template <class T>
void addValueToModelProxy(int index, float value, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(index, value);
}

void copyVarSizedFromModelProxy(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->copyResultTo(std::span{content, size});
}

void copyVarSizedToModelProxy(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(std::span{content, size});
}

void applyModelProxy(void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->infer();
}

float getValueFromModelProxy(int index, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    return adapter->getResultAt(index);
}

template <typename T>
nautilus::val<T> min(const nautilus::val<T>& lhs, const nautilus::val<T>& rhs)
{
    return lhs < rhs ? lhs : rhs;
}

void garbageCollectBatchesProxy(void* inferModelHandler)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    handler->garbageCollectBatches();
}
}

namespace NES
{

IREEBatchInferenceOperator::IREEBatchInferenceOperator(
    const OperatorHandlerId operatorHandlerId,
    std::vector<PhysicalFunction> inputs,
    std::vector<std::string> outputFieldNames,
    std::shared_ptr<Interface::BufferRef::TupleBufferRef> tupleBufferRef)
    : WindowProbePhysicalOperator(operatorHandlerId)
    , inputs(std::move(inputs))
    , outputFieldNames(std::move(outputFieldNames))
    , tupleBufferRef(std::move(tupleBufferRef))
{
}

void IREEBatchInferenceOperator::performInference(
    const Interface::PagedVectorRef& pagedVectorRef,
    Interface::BufferRef::TupleBufferRef& tupleBufferRef,
    ExecutionContext& executionCtx) const
{
    const auto fields = tupleBufferRef.getMemoryLayout()->getSchema().getFieldNames();
    const auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);

    nautilus::val<int> rowIdx(0);
    for (auto it = pagedVectorRef.begin(fields); it != pagedVectorRef.end(fields); ++it)
    {
        auto record = createRecord(*it, fields);

        if (!this->isVarSizedInput)
        {
            for (nautilus::static_val<size_t> i = 0; i < inputs.size(); ++i)
            {
                nautilus::invoke(
                    IREEBatchInference::addValueToModelProxy<float>,
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
                IREEBatchInference::copyVarSizedToModelProxy,
                varSizedValue.getContent(),
                IREEBatchInference::min(varSizedValue.getContentSize(), nautilus::val<uint32_t>(static_cast<uint32_t>(this->inputSize))),
                operatorHandler,
                executionCtx.workerThreadId);
            ++rowIdx;
        }
    }

    nautilus::invoke(IREEBatchInference::applyModelProxy, operatorHandler, executionCtx.workerThreadId);

    rowIdx = nautilus::val<int>(0);
    for (auto it = pagedVectorRef.begin(fields); it != pagedVectorRef.end(fields); ++it)
    {
        auto record = createRecord(*it, fields);

        if (!this->isVarSizedOutput)
        {
            for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
            {
                VarVal result = VarVal(nautilus::invoke(IREEBatchInference::getValueFromModelProxy, rowIdx, operatorHandler, executionCtx.workerThreadId));
                record.write(outputFieldNames.at(i), result);
                ++rowIdx;
            }
        }
        else
        {
            auto output = executionCtx.pipelineMemoryProvider.arena.allocateVariableSizedData(this->outputSize);
            nautilus::invoke(IREEBatchInference::copyVarSizedFromModelProxy, output.getContent(), output.getContentSize(), operatorHandler, executionCtx.workerThreadId);
            record.write(outputFieldNames.at(0), output);
            ++rowIdx;
        }
        executeChild(executionCtx, record);
    }
}

void IREEBatchInferenceOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    const auto emittedBatch = static_cast<nautilus::val<EmittedBatch*>>(recordBuffer.getReference());
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);

    const auto batchMemRef = nautilus::invoke(
        +[](OperatorHandler* ptrOpHandler, const EmittedBatch* currentBatch)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
            const auto* opHandler = dynamic_cast<IREEBatchInferenceOperatorHandler*>(ptrOpHandler);
            std::shared_ptr<Batch> batch = opHandler->getBatch(currentBatch->batchId);
            return batch.get();
        }, operatorHandlerMemRef, emittedBatch);

    const auto batchPagedVectorMemRef = nautilus::invoke(
        +[](const Batch* batch)
        {
            PRECONDITION(batch != nullptr, "batch context should not be null!");
            return batch->getPagedVectorRef();
        }, batchMemRef);

    const Interface::PagedVectorRef batchPagedVectorRef(batchPagedVectorMemRef, tupleBufferRef);
    performInference(batchPagedVectorRef, *tupleBufferRef, executionCtx);

    nautilus::invoke(
        +[](OperatorHandler* ptrOpHandler, const EmittedBatch* currentBatch)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
            const auto* opHandler = dynamic_cast<IREEBatchInferenceOperatorHandler*>(ptrOpHandler);
            std::shared_ptr<Batch> batch = opHandler->getBatch(currentBatch->batchId);
            batch->setState(BatchState::MARKED_AS_PROCESSED);
        }, operatorHandlerMemRef, emittedBatch);
}

void IREEBatchInferenceOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    nautilus::invoke(IREEBatchInference::garbageCollectBatchesProxy, operatorHandlerMemRef);
    PhysicalOperatorConcept::close(executionCtx, recordBuffer);
}

Record
IREEBatchInferenceOperator::createRecord(const Record& featureRecord, const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    Record record;
    for (const auto& fieldName : nautilus::static_iterable(projections))
    {
        record.write(fieldName, featureRecord.read(fieldName));
    }
    return record;
}

}
