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
#include <cstdint>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/SliceCache/SliceCacheFIFO.hpp>
#include <Execution/Operators/SliceCache/SliceCacheLRU.hpp>
#include <Execution/Operators/SliceCache/SliceCacheSecondChance.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationBuildCache.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationSlice.hpp>
#include <Execution/Operators/Streaming/WindowOperatorBuild.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <QueryCompiler/Configurations/QueryCompilerConfiguration.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Time/Timestamp.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>

namespace NES::Runtime::Execution::Operators
{
AggregationBuildCache::AggregationBuildCache(
    const uint64_t operatorHandlerIndex,
    std::unique_ptr<TimeFunction> timeFunction,
    std::vector<std::unique_ptr<Functions::Function>> keyFunctions,
    WindowAggregationOperator windowAggregationOperator,
    QueryCompilation::Configurations::SliceCacheOptions sliceCacheOptions)
    : WindowAggregationOperator(std::move(windowAggregationOperator))
    , WindowOperatorBuild(operatorHandlerIndex, std::move(timeFunction))
    , keyFunctions(std::move(keyFunctions))
    , sliceCacheOptions(std::move(sliceCacheOptions))

{
}
void AggregationBuildCache::setup(ExecutionContext& executionCtx) const
{
    WindowOperatorBuild::setup(executionCtx);
    /// I know that we should not set the numberOfWorkerThreads in the build operator, due to a race condition.
    /// However, we need to set it here, as we need to know the number of worker threads to allocate the slice cache entries.
    /// This should/is fine for this PoC
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    nautilus::invoke(
        +[](AggregationOperatorHandler* opHandler, const PipelineExecutionContext* pipelineExecutionContext)
        { opHandler->setWorkerThreads(pipelineExecutionContext->getNumberOfWorkerThreads()); },
        globalOperatorHandler,
        executionCtx.pipelineContext);

    /// Allocating the memory for the slice cache, so that we can use it in the execute method
    nautilus::val<uint64_t> sizeOfEntry = 0;
    nautilus::val<uint64_t> numberOfEntries = sliceCacheOptions.numberOfEntries;
    switch (sliceCacheOptions.sliceCacheType)
    {
        case QueryCompilation::Configurations::SliceCacheType::NONE:
            return;
        case QueryCompilation::Configurations::SliceCacheType::FIFO:
            sizeOfEntry = sizeof(SliceCacheEntryFIFO);
            break;
        case QueryCompilation::Configurations::SliceCacheType::LRU:
            sizeOfEntry = sizeof(SliceCacheEntryLRU);
            break;
        case QueryCompilation::Configurations::SliceCacheType::SECOND_CHANCE:
            sizeOfEntry = sizeof(SliceCacheEntrySecondChance);
            break;
    }
    nautilus::invoke(
        +[](AggregationOperatorHandler* opHandler,
            Memory::AbstractBufferProvider* bufferProvider,
            const uint64_t sizeOfEntry,
            const uint64_t numberOfEntries) { opHandler->allocateSliceCacheEntries(sizeOfEntry, numberOfEntries, bufferProvider); },
        globalOperatorHandler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        sizeOfEntry,
        numberOfEntries);
}

void AggregationBuildCache::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    WindowOperatorBuild::open(executionCtx, recordBuffer);

    /// Creating the local state for the slice cache, so that we can use it in the execute method
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto startOfSliceEntries = nautilus::invoke(
        +[](AggregationOperatorHandler* opHandler, const WorkerThreadId workerThreadId)
        { return opHandler->getStartOfSliceCacheEntries(workerThreadId); },
        globalOperatorHandler,
        executionCtx.getWorkerThreadId());
    const auto hitsRef = startOfSliceEntries;
    const auto missesRef = hitsRef + nautilus::val<uint64_t>(sizeof(uint64_t));
    const auto sliceCacheEntries = startOfSliceEntries + nautilus::val<uint64_t>(sizeof(HitsAndMisses));
    const auto startOfDataEntry = nautilus::invoke(
        +[](const SliceCacheEntry* sliceCacheEntry)
        {
            const auto startOfDataStructure = &sliceCacheEntry->dataStructure;
            return startOfDataStructure;
        },
        sliceCacheEntries);

    switch (sliceCacheOptions.sliceCacheType)
    {
        case QueryCompilation::Configurations::SliceCacheType::NONE:
            break;
        case QueryCompilation::Configurations::SliceCacheType::FIFO:
            executionCtx.setLocalOperatorState(
                this,
                std::make_unique<SliceCacheFIFO>(
                    sliceCacheOptions.numberOfEntries,
                    sizeof(SliceCacheEntryFIFO),
                    sliceCacheEntries,
                    startOfDataEntry,
                    hitsRef,
                    missesRef));
            break;
        case QueryCompilation::Configurations::SliceCacheType::LRU:
            executionCtx.setLocalOperatorState(
                this,
                std::make_unique<SliceCacheLRU>(
                    sliceCacheOptions.numberOfEntries,
                    sizeof(SliceCacheEntryLRU),
                    sliceCacheEntries,
                    startOfDataEntry,
                    hitsRef,
                    missesRef));
            break;
        case QueryCompilation::Configurations::SliceCacheType::SECOND_CHANCE:
            executionCtx.setLocalOperatorState(
                this,
                std::make_unique<SliceCacheSecondChance>(
                    sliceCacheOptions.numberOfEntries,
                    sizeof(SliceCacheEntrySecondChance),
                    sliceCacheEntries,
                    startOfDataEntry,
                    hitsRef,
                    missesRef));
            break;
    }
}
namespace
{
int8_t* createNewAggregationSliceProxy(
    SliceCacheEntry* sliceCacheEntry,
    OperatorHandler* ptrOpHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuildCache* buildOperator)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    const auto* operatorHandler = dynamic_cast<AggregationOperatorHandler*>(ptrOpHandler);
    /// If a new aggregation slice is created, we need to set the cleanup function for the aggregation states
    auto wrappedCreateFunction(
        [createFunction = operatorHandler->getCreateNewSlicesFunction(),
         buildOperator](const SliceStart sliceStart, const SliceEnd sliceEnd)
        {
            const auto createdSlices = createFunction(sliceStart, sliceEnd);
            for (const auto& slice : createdSlices)
            {
                const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(slice);
                INVARIANT(aggregationSlice != nullptr, "The slice should be an AggregationSlice in an AggregationBuild");
                aggregationSlice->setCleanupFunction(buildOperator->getStateCleanupFunction());
            }
            return createdSlices;
        });

    const auto hashMap = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, wrappedCreateFunction);
    INVARIANT(
        hashMap.size() == 1,
        "We expect exactly one slice for the given timestamp during the AggregationBuild, as we currently solely support "
        "slicing");


    /// Updating the slice cache entry with the new slice
    const auto newAggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(hashMap[0]);
    sliceCacheEntry->sliceStart = newAggregationSlice->getSliceStart();
    sliceCacheEntry->sliceEnd = newAggregationSlice->getSliceEnd();
    sliceCacheEntry->dataStructure = reinterpret_cast<int8_t*>(newAggregationSlice->getHashMapPtr(workerThreadId));
    return sliceCacheEntry->dataStructure;
}
}

std::function<void(const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>&)> AggregationBuildCache::getStateCleanupFunction() const
{
    return [copyOfFieldKeys = this->fieldKeys,
            copyOfFieldValues = this->fieldValues,
            copyOfAggregationFunctions = this->aggregationFunctions,
            copyOfEntriesPerPage = this->entriesPerPage,
            copyOfEntrySize = this->entrySize](const std::vector<std::unique_ptr<Nautilus::Interface::HashMap>>& hashMaps)
    {
        nautilus::engine::Options options;
        options.setOption("engine.Compilation", true);
        const nautilus::engine::NautilusEngine nautilusEngine(options);
        static auto cleanupStateNautilusFunc = nautilusEngine.registerFunction(
            std::function(
                [copyOfFieldKeys, copyOfFieldValues, copyOfAggregationFunctions, copyOfEntriesPerPage, copyOfEntrySize](
                    nautilus::val<Nautilus::Interface::HashMap*> hashMap)
                {
                    const Interface::ChainedHashMapRef hashMapRef(
                        hashMap, copyOfFieldKeys, copyOfFieldValues, copyOfEntriesPerPage, copyOfEntrySize);
                    for (const auto entry : hashMapRef)
                    {
                        const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, copyOfFieldKeys, copyOfFieldValues);
                        auto state = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRefReset.getValueMemArea());
                        for (const auto& aggFunction : nautilus::static_iterable(copyOfAggregationFunctions))
                        {
                            aggFunction->cleanup(state);
                            state = state + aggFunction->getSizeOfStateInBytes();
                        }
                    }
                }));

        for (const auto& hashMap :
             hashMaps | std::views::filter([](const auto& hashMapPtr) { return hashMapPtr->getNumberOfTuples() > 0; }))
        {
            /// Calling the compiled nautilus function
            cleanupStateNautilusFunc(hashMap.get());
        }
    };
}


void AggregationBuildCache::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Getting the slice cache from the local state
    auto sliceCache = dynamic_cast<SliceCache*>(executionCtx.getLocalState(this));

    /// Getting the current hash map that we have to insert/update the record into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto hashMapPtr = sliceCache->getDataStructureRef(
        timestamp,
        [&](const nautilus::val<SliceCacheEntry*>& sliceCacheEntryToReplace)
        {
            return nautilus::invoke(
                createNewAggregationSliceProxy,
                sliceCacheEntryToReplace,
                globalOperatorHandler,
                timestamp,
                executionCtx.getWorkerThreadId(),
                nautilus::val<const AggregationBuildCache*>(this));
        });
    Interface::ChainedHashMapRef hashMap(hashMapPtr, fieldKeys, fieldValues, entriesPerPage, entrySize);


    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = fieldKeys[i];
        const auto& function = keyFunctions[i];
        const auto value = function->execute(record, executionCtx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashFunction,
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, fieldKeys, fieldValues);
            auto state = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
            {
                aggFunction->reset(state, executionCtx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        executionCtx.pipelineMemoryProvider.bufferProvider);


    /// Updating the aggregation states
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(hashMapEntry, fieldKeys, fieldValues);
    auto state = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
    {
        aggFunction->lift(state, executionCtx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}
}
