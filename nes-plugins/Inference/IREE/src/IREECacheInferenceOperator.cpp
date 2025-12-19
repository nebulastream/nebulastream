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
#include <IREECacheInferenceOperator.hpp>
#include <IREEInferenceOperatorHandler.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PredictionCache/PredictionCache2Q.hpp>
#include <PredictionCache/PredictionCacheFIFO.hpp>
#include <PredictionCache/PredictionCacheLFU.hpp>
#include <PredictionCache/PredictionCacheLRU.hpp>
#include <PredictionCache/PredictionCacheSecondChance.hpp>
#include <PredictionCache/PredictionCacheUtil.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <nautilus/function.hpp>
#include <ranges>

namespace NES::QueryCompilation::PhysicalOperators
{
class PhysicalInferModelOperator;
}

namespace NES::IREECacheInference
{
template <class T>
void addValueToModelProxy(int index, T value, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput<T>(index, value);
}

template <class T>
T getValueFromModelProxy(int index, void* inferModelHandler, WorkerThreadId thread, std::byte* outputData)
{
    PRECONDITION(outputData != nullptr, "Should have received a valid pointer to the model output");

    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);

    PRECONDITION(static_cast<size_t>(index) < adapter->outputSize / 4, "Index is too large");
    return std::bit_cast<T*>(outputData)[index];
}

void copyVarSizedToModelProxy(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(std::span{content, size});
}

void copyVarSizedFromModelProxy(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread, std::byte* outputData)
{
    PRECONDITION(outputData != nullptr, "Should have received a valid pointer to the model output");

    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    const auto span = std::span{content, size};

    PRECONDITION(adapter->outputSize == span.size(), "Output size does not match");
    std::ranges::copy_n(outputData, std::min(span.size(), adapter->outputSize), span.data());
}

template <class T>
void applyModelProxy(void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->infer<T>();
}

template <typename T>
nautilus::val<T> min(const nautilus::val<T>& lhs, const nautilus::val<T>& rhs)
{
    return lhs < rhs ? lhs : rhs;
}
}

namespace NES
{

IREECacheInferenceOperator::IREECacheInferenceOperator(
    const OperatorHandlerId inferModelHandlerIndex,
    std::vector<PhysicalFunction> inputs,
    std::vector<std::string> outputFieldNames,
    Configurations::PredictionCacheOptions predictionCacheOptions,
    DataType inputDtype,
    DataType outputDtype)
    : inferModelHandlerIndex(inferModelHandlerIndex)
    , inputs(std::move(inputs))
    , outputFieldNames(std::move(outputFieldNames))
    , predictionCacheOptions(predictionCacheOptions)
    , inputDtype(inputDtype)
    , outputDtype(outputDtype)
{
}

template <class T>
nautilus::val<std::byte*> IREECacheInferenceOperator::performInference(
    ExecutionContext& executionCtx, NES::Record& record, PredictionCache* predictionCache) const
{
    auto inferModelHandler = predictionCache->getOperatorHandler();

    if (!this->isVarSizedInput)
    {
        for (nautilus::static_val<size_t> i = 0; i < inputs.size(); ++i)
        {
            invoke(
                IREECacheInference::addValueToModelProxy<T>,
                nautilus::val<int>(i),
                inputs.at(i).execute(record, executionCtx.pipelineMemoryProvider.arena).cast<nautilus::val<T>>(),
                inferModelHandler,
                executionCtx.workerThreadId);
        }
    }
    else
    {
        VarVal value = inputs.at(0).execute(record, executionCtx.pipelineMemoryProvider.arena);
        auto varSizedValue = value.cast<VariableSizedData>();
        invoke(
            IREECacheInference::copyVarSizedToModelProxy,
            varSizedValue.getContent(),
            IREECacheInference::min(varSizedValue.getContentSize(), nautilus::val<uint32_t>(static_cast<uint32_t>(this->inputSize))),
            inferModelHandler,
            executionCtx.workerThreadId);
    }

    auto inputDataVal = invoke(
        +[](void* inferModelHandler, WorkerThreadId thread)
        {
            auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
            auto adapter = handler->getIREEAdapter(thread);
            return adapter->inputData.get();
        }, inferModelHandler, executionCtx.workerThreadId);

    return predictionCache->getDataStructureRef(
        inputDataVal,
        [&](
            const nautilus::val<PredictionCacheEntry*>& predictionCacheEntryToReplace, const nautilus::val<uint64_t>&)
        {
            return invoke(
                +[](PredictionCacheEntry* predictionCacheEntry, void* opHandlerPtr, WorkerThreadId thread)
                {
                    auto handler = static_cast<IREEInferenceOperatorHandler*>(opHandlerPtr);
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
                }, predictionCacheEntryToReplace, inferModelHandler, executionCtx.workerThreadId);
        });
}

template <class T>
void IREECacheInferenceOperator::writeOutputRecord(
    ExecutionContext& executionCtx,
    NES::Record& record,
    const nautilus::val<std::byte*>& prediction,
    PredictionCache* predictionCache) const
{
    auto inferModelHandler = predictionCache->getOperatorHandler();
    
    if (!this->isVarSizedOutput)
    {
        for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
        {
            const VarVal result = VarVal(invoke(
                IREECacheInference::getValueFromModelProxy<T>,
                nautilus::val<int>(i), inferModelHandler, executionCtx.workerThreadId, prediction));
            record.write(outputFieldNames.at(i), result);
        }
    }
    else
    {
        auto output = executionCtx.pipelineMemoryProvider.arena.allocateVariableSizedData(this->outputSize);
        invoke(
            IREECacheInference::copyVarSizedFromModelProxy,
            output.getContent(), output.getContentSize(), inferModelHandler, executionCtx.workerThreadId, prediction);
        record.write(outputFieldNames.at(0), output);
    }

    child->execute(executionCtx, record);
}

void IREECacheInferenceOperator::execute(ExecutionContext& executionCtx, NES::Record& record) const
{
    auto* predictionCache = dynamic_cast<PredictionCache*>(executionCtx.getLocalState(id));
    nautilus::val<std::byte*> prediction;
    switch (inputDtype.type)
    {
        case DataType::Type::UINT8: prediction = performInference<uint8_t>(executionCtx, record, predictionCache); break;
        case DataType::Type::UINT16: prediction = performInference<uint16_t>(executionCtx, record, predictionCache); break;
        case DataType::Type::UINT32: prediction = performInference<uint32_t>(executionCtx, record, predictionCache); break;
        case DataType::Type::UINT64: prediction = performInference<uint64_t>(executionCtx, record, predictionCache); break;
        case DataType::Type::INT8: prediction = performInference<int8_t>(executionCtx, record, predictionCache); break;
        case DataType::Type::INT16: prediction = performInference<int16_t>(executionCtx, record, predictionCache); break;
        case DataType::Type::INT32: prediction = performInference<int32_t>(executionCtx, record, predictionCache); break;
        case DataType::Type::INT64: prediction = performInference<int64_t>(executionCtx, record, predictionCache); break;
        case DataType::Type::FLOAT32: prediction = performInference<float>(executionCtx, record, predictionCache); break;
        case DataType::Type::FLOAT64: prediction = performInference<double>(executionCtx, record, predictionCache); break;

        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP:
            throw UnknownDataType("Physical Type: type {} is currently not implemented", magic_enum::enum_name(inputDtype.type));
    }

    switch (outputDtype.type)
    {
        case DataType::Type::UINT8: writeOutputRecord<uint8_t>(executionCtx, record, prediction, predictionCache); break;
        case DataType::Type::UINT16: writeOutputRecord<uint16_t>(executionCtx, record, prediction, predictionCache); break;
        case DataType::Type::UINT32: writeOutputRecord<uint32_t>(executionCtx, record, prediction, predictionCache); break;
        case DataType::Type::UINT64: writeOutputRecord<uint64_t>(executionCtx, record, prediction, predictionCache); break;
        case DataType::Type::INT8: writeOutputRecord<int8_t>(executionCtx, record, prediction, predictionCache); break;
        case DataType::Type::INT16: writeOutputRecord<int16_t>(executionCtx, record, prediction, predictionCache); break;
        case DataType::Type::INT32: writeOutputRecord<int32_t>(executionCtx, record, prediction, predictionCache); break;
        case DataType::Type::INT64: writeOutputRecord<int64_t>(executionCtx, record, prediction, predictionCache); break;
        case DataType::Type::FLOAT32: writeOutputRecord<float>(executionCtx, record, prediction, predictionCache); break;
        case DataType::Type::FLOAT64: writeOutputRecord<double>(executionCtx, record, prediction, predictionCache); break;

        case DataType::Type::BOOLEAN:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
        case DataType::Type::VARSIZED:
        case DataType::Type::VARSIZED_POINTER_REP:
            throw UnknownDataType("Physical Type: type {} is currently not implemented", magic_enum::enum_name(outputDtype.type));
    }
}

void IREECacheInferenceOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex);
    invoke(
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

    invoke(
        +[](IREEInferenceOperatorHandler* opHandler,
            AbstractBufferProvider* bufferProvider,
            const uint64_t sizeOfEntryVal,
            const uint64_t numberOfEntriesVal)
        { opHandler->allocatePredictionCacheEntries(sizeOfEntryVal, numberOfEntriesVal, bufferProvider); },
        globalOperatorHandler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        sizeOfEntry,
        numberOfEntries);
}

void IREECacheInferenceOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PhysicalOperatorConcept::open(executionCtx, recordBuffer);
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex);

    const auto startOfEntries = invoke(
        +[](const IREEInferenceOperatorHandler* opHandler, const WorkerThreadId workerThreadId)
        {
            return opHandler->getStartOfPredictionCacheEntries(
                IREEInferenceOperatorHandler::StartPredictionCacheEntriesIREEInference{workerThreadId});
        }, globalOperatorHandler, executionCtx.workerThreadId);

    const auto inputSize = invoke(
        +[](void* inferModelHandler, WorkerThreadId thread)
        {
            auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
            auto adapter = handler->getIREEAdapter(thread);
            return adapter->inputSize;
        }, globalOperatorHandler, executionCtx.workerThreadId);

    auto predictionCache = NES::Util::createPredictionCache(
        predictionCacheOptions, globalOperatorHandler, startOfEntries, inputSize);
    executionCtx.setLocalOperatorState(id, std::move(predictionCache));
}

void IREECacheInferenceOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PhysicalOperatorConcept::close(executionCtx, recordBuffer);
}

void IREECacheInferenceOperator::terminate(ExecutionContext& executionCtx) const
{
    invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->stop(QueryTerminationType::Graceful, *pec); },
        executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex),
        executionCtx.pipelineContext);
}
}
