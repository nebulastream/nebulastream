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
#include <IREEBatchCacheInferenceOperator.hpp>
#include <IREEBatchInferenceOperatorHandler.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PredictionCache/PredictionCache2Q.hpp>
#include <PredictionCache/PredictionCacheFIFO.hpp>
#include <PredictionCache/PredictionCacheLFU.hpp>
#include <PredictionCache/PredictionCacheLRU.hpp>
#include <PredictionCache/PredictionCacheSecondChance.hpp>
#include <PredictionCache/PredictionCacheUtil.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <nautilus/function.hpp>
#include <ranges>

namespace NES::QueryCompilation::PhysicalOperators
{
class PhysicalInferModelOperator;
}

namespace NES::IREEBatchCacheInference
{
template <class T>
void addValueToModelProxy(int index, T value, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput<T>(index, value);
}

template <class T>
T getValueFromModelProxy(int index, void* inferModelHandler, WorkerThreadId thread, std::byte* outputData)
{
    PRECONDITION(outputData != nullptr, "Should have received a valid pointer to the model output");

    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);

    PRECONDITION(static_cast<size_t>(index) < adapter->outputSize / 4, "Index is too large");
    return std::bit_cast<T*>(outputData)[index];
}

void copyVarSizedToModelProxy(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(std::span{content, size});
}

void copyVarSizedFromModelProxy(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread, std::byte* outputData)
{
    PRECONDITION(outputData != nullptr, "Should have received a valid pointer to the model output");

    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    const auto span = std::span{content, size};

    PRECONDITION(adapter->outputSize == span.size(), "Output size does not match");
    std::ranges::copy_n(outputData, std::min(span.size(), adapter->outputSize), span.data());
}

template <class T>
void applyModelProxy(void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->infer<T>();
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


IREEBatchCacheInferenceOperator::IREEBatchCacheInferenceOperator(
    const OperatorHandlerId operatorHandlerId,
    std::vector<PhysicalFunction> inputs,
    std::vector<std::string> outputFieldNames,
    std::shared_ptr<TupleBufferRef> tupleBufferRef,
    Configurations::PredictionCacheOptions predictionCacheOptions,
    DataType inputDtype,
    DataType outputDtype)
    : WindowProbePhysicalOperator(operatorHandlerId)
    , inputs(std::move(inputs))
    , outputFieldNames(std::move(outputFieldNames))
    , tupleBufferRef(std::move(tupleBufferRef))
    , predictionCacheOptions(predictionCacheOptions)
    , inputDtype(inputDtype)
    , outputDtype(outputDtype)
{
}

template <typename T>
nautilus::val<std::byte*> IREEBatchCacheInferenceOperator::performInference(
    const PagedVectorRef& pagedVectorRef,
    TupleBufferRef& tupleBufferRef,
    ExecutionContext& executionCtx) const
{
    const auto fields = tupleBufferRef.getMemoryLayout()->getSchema().getFieldNames();
    auto* predictionCache = dynamic_cast<PredictionCache*>(executionCtx.getLocalState(id));
    const auto operatorHandler = predictionCache->getOperatorHandler();

    nautilus::val<int> rowIdx(0);
    for (auto it = pagedVectorRef.begin(fields); it != pagedVectorRef.end(fields); ++it)
    {
        auto record = createRecord(*it, fields);

        if (!this->isVarSizedInput)
        {
            for (nautilus::static_val<size_t> i = 0; i < inputs.size(); ++i)
            {
                nautilus::invoke(
                    IREEBatchCacheInference::addValueToModelProxy<T>,
                    rowIdx,
                    inputs.at(i).execute(record, executionCtx.pipelineMemoryProvider.arena).cast<nautilus::val<T>>(),
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
                IREEBatchCacheInference::copyVarSizedToModelProxy,
                varSizedValue.getContent(),
                IREEBatchCacheInference::min(varSizedValue.getContentSize(), nautilus::val<uint32_t>(static_cast<uint32_t>(this->inputSize))),
                operatorHandler,
                executionCtx.workerThreadId);
            ++rowIdx;
        }
    }

    auto inputDataVal = nautilus::invoke(
        +[](void* inferModelHandler, WorkerThreadId thread)
        {
            auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
            auto adapter = handler->getIREEAdapter(thread);
            return adapter->inputData.get();
        }, operatorHandler, executionCtx.workerThreadId);

    return predictionCache->getDataStructureRef(
        inputDataVal,
        [&](
            const nautilus::val<PredictionCacheEntry*>& predictionCacheEntryToReplace, const nautilus::val<uint64_t>&)
        {
            return nautilus::invoke(
                +[](PredictionCacheEntry* predictionCacheEntry, void* opHandlerPtr, WorkerThreadId thread)
                {
                    auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(opHandlerPtr);
                    auto adapter = handler->getIREEAdapter(thread);
                    adapter->infer<T>();
                    adapter->misses += 1;

                    predictionCacheEntry->recordSize = adapter->inputSize;
                    predictionCacheEntry->record = new std::byte[adapter->inputSize];
                    std::memcpy(predictionCacheEntry->record, adapter->inputData.get(), adapter->inputSize);

                    predictionCacheEntry->dataSize = adapter->outputSize;
                    predictionCacheEntry->dataStructure = new std::byte[adapter->outputSize];
                    std::memcpy(predictionCacheEntry->dataStructure, adapter->outputData.get(), adapter->outputSize);

                    return predictionCacheEntry->dataStructure;
                }, predictionCacheEntryToReplace, operatorHandler, executionCtx.workerThreadId);
        });
}

template <typename T>
void IREEBatchCacheInferenceOperator::writeOutputRecord(
    const PagedVectorRef& pagedVectorRef,
    TupleBufferRef& tupleBufferRef,
    ExecutionContext& executionCtx,
    const nautilus::val<std::byte*>& prediction) const
{
    const auto fields = tupleBufferRef.getMemoryLayout()->getSchema().getFieldNames();
    auto* predictionCache = dynamic_cast<PredictionCache*>(executionCtx.getLocalState(id));
    const auto operatorHandler = predictionCache->getOperatorHandler();

    nautilus::val<int> rowIdx(0);
    for (auto it = pagedVectorRef.begin(fields); it != pagedVectorRef.end(fields); ++it)
    {
        auto record = createRecord(*it, fields);

        if (!this->isVarSizedOutput)
        {
            for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
            {
                const VarVal result = VarVal(nautilus::invoke(
                    IREEBatchCacheInference::getValueFromModelProxy<T>,
                    nautilus::val<int>(i),
                    operatorHandler,
                    executionCtx.workerThreadId,
                    prediction));
                record.write(outputFieldNames.at(i), result);
                ++rowIdx;
            }
        }
        else
        {
            auto output = executionCtx.pipelineMemoryProvider.arena.allocateVariableSizedData(this->outputSize);
            nautilus::invoke(
                IREEBatchCacheInference::copyVarSizedFromModelProxy,
                output.getContent(),
                output.getContentSize(),
                operatorHandler,
                executionCtx.workerThreadId,
                prediction);
            record.write(outputFieldNames.at(0), output);
            ++rowIdx;
        }
        executeChild(executionCtx, record);
    }
}

void IREEBatchCacheInferenceOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    const auto emittedBatch = static_cast<nautilus::val<EmittedBatch*>>(recordBuffer.getMemArea());
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
    const PagedVectorRef batchPagedVectorRef(batchPagedVectorMemRef, tupleBufferRef);

    const auto startOfEntries = nautilus::invoke(
        +[](const IREEBatchInferenceOperatorHandler* opHandler, const WorkerThreadId workerThreadId)
        {
            return opHandler->getStartOfPredictionCacheEntries(
                IREEBatchInferenceOperatorHandler::StartPredictionCacheEntriesIREEInference{workerThreadId});
        }, operatorHandlerMemRef, executionCtx.workerThreadId);

    const auto inputSize = nautilus::invoke(
        +[](void* inferModelHandler, WorkerThreadId thread)
        {
            auto handler = static_cast<IREEBatchInferenceOperatorHandler*>(inferModelHandler);
            auto adapter = handler->getIREEAdapter(thread);
            return adapter->inputSize;
        }, operatorHandlerMemRef, executionCtx.workerThreadId);

    auto predictionCache = NES::Util::createPredictionCache(
        predictionCacheOptions, operatorHandlerMemRef, startOfEntries, inputSize);
    executionCtx.setLocalOperatorState(id, std::move(predictionCache));

    nautilus::val<std::byte*> prediction;
    switch (inputDtype.type)
    {
        case DataType::Type::UINT8: prediction = performInference<uint8_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;
        case DataType::Type::UINT16: prediction = performInference<uint16_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;
        case DataType::Type::UINT32: prediction = performInference<uint32_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;
        case DataType::Type::UINT64: prediction = performInference<uint64_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;
        case DataType::Type::INT8: prediction = performInference<int8_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;
        case DataType::Type::INT16: prediction = performInference<int16_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;
        case DataType::Type::INT32: prediction = performInference<int32_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;
        case DataType::Type::INT64: prediction = performInference<int64_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;
        case DataType::Type::FLOAT32: prediction = performInference<float>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;
        case DataType::Type::FLOAT64: prediction = performInference<double>(batchPagedVectorRef, *tupleBufferRef, executionCtx); break;

        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP:
            throw UnknownDataType("Physical Type: type {} is currently not implemented", magic_enum::enum_name(inputDtype.type));
    }

    switch (outputDtype.type)
    {
        case DataType::Type::UINT8: writeOutputRecord<uint8_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;
        case DataType::Type::UINT16: writeOutputRecord<uint16_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;
        case DataType::Type::UINT32: writeOutputRecord<uint32_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;
        case DataType::Type::UINT64: writeOutputRecord<uint64_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;
        case DataType::Type::INT8: writeOutputRecord<int8_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;
        case DataType::Type::INT16: writeOutputRecord<int16_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;
        case DataType::Type::INT32: writeOutputRecord<int32_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;
        case DataType::Type::INT64: writeOutputRecord<int64_t>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;
        case DataType::Type::FLOAT32: writeOutputRecord<float>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;
        case DataType::Type::FLOAT64: writeOutputRecord<double>(batchPagedVectorRef, *tupleBufferRef, executionCtx, prediction); break;

        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP:
            throw UnknownDataType("Physical Type: type {} is currently not implemented", magic_enum::enum_name(outputDtype.type));
    }

    nautilus::invoke(
        +[](OperatorHandler* ptrOpHandler, const EmittedBatch* currentBatch)
        {
            PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
            const auto* opHandler = dynamic_cast<IREEBatchInferenceOperatorHandler*>(ptrOpHandler);
            std::shared_ptr<Batch> batch = opHandler->getBatch(currentBatch->batchId);
            batch->setState(BatchState::MARKED_AS_PROCESSED);
        }, operatorHandlerMemRef, emittedBatch);
}

void IREEBatchCacheInferenceOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    nautilus::invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->start(*pec, 0); },
        globalOperatorHandler,
        executionCtx.pipelineContext);

    nautilus::val<uint64_t> sizeOfEntry = 0;
    nautilus::val<uint64_t> numberOfEntries = predictionCacheOptions.numberOfEntries;
    switch (predictionCacheOptions.predictionCacheType)
    {
        case Configurations::PredictionCacheType::NONE:
            return;
        case Configurations::PredictionCacheType::FIFO:
            sizeOfEntry = sizeof(PredictionCacheEntryFIFO);
            break;
        case Configurations::PredictionCacheType::LFU:
            sizeOfEntry = sizeof(PredictionCacheEntryLFU);
            break;
        case Configurations::PredictionCacheType::LRU:
            sizeOfEntry = sizeof(PredictionCacheEntryLRU);
            break;
        case Configurations::PredictionCacheType::SECOND_CHANCE:
            sizeOfEntry = sizeof(PredictionCacheEntrySecondChance);
            break;
        case Configurations::PredictionCacheType::TWO_QUEUES:
            sizeOfEntry = sizeof(PredictionCacheEntry2Q);
            break;
    }

    nautilus::invoke(
        +[](IREEBatchInferenceOperatorHandler* opHandler,
            AbstractBufferProvider* bufferProvider,
            const uint64_t sizeOfEntryVal,
            const uint64_t numberOfEntriesVal)
        { opHandler->allocatePredictionCacheEntries(sizeOfEntryVal, numberOfEntriesVal, bufferProvider); },
        globalOperatorHandler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        sizeOfEntry,
        numberOfEntries);
}

void IREEBatchCacheInferenceOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto operatorHandlerMemRef = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    nautilus::invoke(IREEBatchCacheInference::garbageCollectBatchesProxy, operatorHandlerMemRef);
    PhysicalOperatorConcept::close(executionCtx, recordBuffer);
}

Record
IREEBatchCacheInferenceOperator::createRecord(const Record& featureRecord, const std::vector<Record::RecordFieldIdentifier>& projections) const
{
    Record record;
    for (const auto& fieldName : nautilus::static_iterable(projections))
    {
        record.write(fieldName, featureRecord.read(fieldName));
    }
    return record;
}

}
