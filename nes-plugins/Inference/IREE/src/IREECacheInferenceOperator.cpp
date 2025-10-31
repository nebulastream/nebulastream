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
#include <Operators/LogicalOperator.hpp>
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
}

namespace NES
{

void IREECacheInferenceOperator::execute(ExecutionContext& executionCtx, NES::Nautilus::Record& record) const
{
    auto* predictionCache = dynamic_cast<PredictionCache*>(executionCtx.getLocalState(id));
    auto inferModelHandler = predictionCache->getOperatorHandler();

    if (!this->isVarSizedInput)
    {
        for (nautilus::static_val<size_t> i = 0; i < inputs.size(); ++i)
        {
            nautilus::invoke(
                IREECacheInference::addValueToModel<float>,
                nautilus::val<int>(i),
                inputs.at(i).execute(record, executionCtx.pipelineMemoryProvider.arena).cast<nautilus::val<float>>(),
                inferModelHandler,
                executionCtx.workerThreadId);
        }
    }
    else
    {
        VarVal value = inputs.at(0).execute(record, executionCtx.pipelineMemoryProvider.arena);
        auto varSizedValue = value.cast<VariableSizedData>();
        nautilus::invoke(
            IREECacheInference::copyVarSizedToModel,
            varSizedValue.getContent(),
            IREECacheInference::min(varSizedValue.getContentSize(), nautilus::val<uint32_t>(static_cast<uint32_t>(this->inputSize))),
            inferModelHandler,
            executionCtx.workerThreadId);
    }

    nautilus::val<std::byte*> inputDataVal = nautilus::invoke(
        +[](void* inferModelHandler, WorkerThreadId thread)
        {
            auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
            auto adapter = handler->getIREEAdapter(thread);
            return adapter->inputData.get();
        }, inferModelHandler, executionCtx.workerThreadId);

    const auto pred = predictionCache->getDataStructureRef(
        inputDataVal,
        [&](
            const nautilus::val<PredictionCacheEntry*>& predictionCacheEntryToReplace, const nautilus::val<uint64_t>&)
        {
            return nautilus::invoke(
                +[](PredictionCacheEntry* predictionCacheEntry, void* opHandlerPtr, WorkerThreadId thread)
                {
                    auto handler = static_cast<IREEInferenceOperatorHandler*>(opHandlerPtr);
                    auto adapter = handler->getIREEAdapter(thread);
                    adapter->infer();

                    std::memcpy(adapter->inputDataCache.get(), adapter->inputData.get(), adapter->inputSize);
                    predictionCacheEntry->record = adapter->inputDataCache.get();

                    predictionCacheEntry->dataStructure = reinterpret_cast<int8_t*>(adapter->outputData.get());
                    return predictionCacheEntry->dataStructure;
                }, predictionCacheEntryToReplace, inferModelHandler, executionCtx.workerThreadId);
        });

    if (!this->isVarSizedOutput)
    {
        for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
        {
            VarVal result = VarVal(nautilus::invoke(IREECacheInference::getValueFromModel, nautilus::val<int>(i), inferModelHandler, executionCtx.workerThreadId));
            record.write(outputFieldNames.at(i), result);
        }
    }
    else
    {
        auto output = executionCtx.pipelineMemoryProvider.arena.allocateVariableSizedData(this->outputSize);
        nautilus::invoke(IREECacheInference::copyVarSizedFromModel, output.getContent(), output.getContentSize(), inferModelHandler, executionCtx.workerThreadId);
        record.write(outputFieldNames.at(0), output);
    }

    child->execute(executionCtx, record);
}

void IREECacheInferenceOperator::setup(ExecutionContext& executionCtx) const
{
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex);
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

    const auto startOfEntries = nautilus::invoke(
        +[](const IREEInferenceOperatorHandler* opHandler, const WorkerThreadId workerThreadId)
        {
            return opHandler->getStartOfPredictionCacheEntries(
                IREEInferenceOperatorHandler::StartPredictionCacheEntriesIREEInference{workerThreadId});
        }, globalOperatorHandler, executionCtx.workerThreadId);

    const auto inputSize = nautilus::invoke(
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
    auto* predictionCache = dynamic_cast<PredictionCache*>(executionCtx.getLocalState(id));
    auto inferModelHandler = predictionCache->getOperatorHandler();

    auto hits = predictionCache->getHitsRef();
    auto misses = predictionCache->getMissesRef();

    nautilus::invoke(
        +[](uint64_t* hits, uint64_t* misses)
        {
            NES_INFO("Hits: {}; Misses: {}", *hits, *misses);
        }, hits, misses);
}

void IREECacheInferenceOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->stop(QueryTerminationType::Graceful, *pec); },
        executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex),
        executionCtx.pipelineContext);
}
}
