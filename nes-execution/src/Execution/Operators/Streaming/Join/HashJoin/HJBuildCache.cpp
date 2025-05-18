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

#include <ranges>
#include <Execution/Operators/SliceCache/SliceCacheFIFO.hpp>
#include <Execution/Operators/SliceCache/SliceCacheLRU.hpp>
#include <Execution/Operators/SliceCache/SliceCacheSecondChance.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJBuildCache.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJSlice.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <magic_enum/magic_enum.hpp>
#include <nautilus/val_enum.hpp>
#include <Engine.hpp>

namespace NES::Runtime::Execution::Operators
{

HJBuildCache::HJBuildCache(
    const uint64_t operatorHandlerIndex,
    const QueryCompilation::JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
    HashMapOptions hashMapOptions,
    QueryCompilation::Configurations::SliceCacheOptions sliceCacheOptions)
    : HashMapOptions(std::move(hashMapOptions))
    , StreamJoinBuild(operatorHandlerIndex, joinBuildSide, std::move(timeFunction), memoryProvider)
    , sliceCacheOptions(std::move(sliceCacheOptions))

{
}

void HJBuildCache::setup(ExecutionContext& executionCtx) const
{
    StreamJoinBuild::setup(executionCtx);

    /// I know that we should not set the numberOfWorkerThreads in the build operator, due to a race condition.
    /// However, we need to set it here, as we need to know the number of worker threads to allocate the slice cache entries.
    /// This should/is fine for this PoC
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    nautilus::invoke(
        +[](HJOperatorHandler* opHandler, const PipelineExecutionContext* pipelineExecutionContext)
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
        +[](HJOperatorHandler* opHandler,
            Memory::AbstractBufferProvider* bufferProvider,
            const uint64_t sizeOfEntry,
            const uint64_t numberOfEntries) { opHandler->allocateSliceCacheEntries(sizeOfEntry, numberOfEntries, bufferProvider); },
        globalOperatorHandler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        sizeOfEntry,
        numberOfEntries);

    /// Creating the cleanup function for the slice of current stream
    nautilus::invoke(
        +[](HJOperatorHandler* operatorHandler, const HJBuildCache* buildOperator, const QueryCompilation::JoinBuildSideType buildSide)
        {
            nautilus::engine::Options options;
            options.setOption("engine.Compilation", true);
            const nautilus::engine::NautilusEngine nautilusEngine(options);
            const auto cleanupStateNautilusFunction
                = std::make_shared<HJOperatorHandler::NautilusCleanupExec>(nautilusEngine.registerFunction(
                    std::function(
                        [copyOfFieldKeys = buildOperator->fieldKeys,
                         copyOfFieldValues = buildOperator->fieldValues,
                         copyOfEntriesPerPage = buildOperator->entriesPerPage,
                         copyOfEntrySize = buildOperator->entrySize](nautilus::val<Nautilus::Interface::HashMap*> hashMap)
                        {
                            const Interface::ChainedHashMapRef hashMapRef(
                                hashMap, copyOfFieldKeys, copyOfFieldValues, copyOfEntriesPerPage, copyOfEntrySize);
                            for (const auto entry : hashMapRef)
                            {
                                const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(
                                    entry, copyOfFieldKeys, copyOfFieldValues);
                                const auto state = entryRefReset.getValueMemArea();
                                nautilus::invoke(
                                    +[](int8_t* pagedVectorMemArea) -> void
                                    {
                                        /// Calls the destructor of the PagedVector
                                        auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(
                                            pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                                        pagedVector->~PagedVector();
                                    },
                                    state);
                            }
                        })));
            operatorHandler->setNautilusCleanupExec(std::move(cleanupStateNautilusFunction), buildSide);
        },
        executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
        nautilus::val<const HJBuildCache*>(this),
        nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide));
}


void HJBuildCache::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    StreamJoinBuild::open(executionCtx, recordBuffer);

    /// Creating the local state for the slice cache, so that we can use it in the execute method
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
    const auto startOfSliceEntries = nautilus::invoke(
        +[](HJOperatorHandler* opHandler, const WorkerThreadId workerThreadId, const QueryCompilation::JoinBuildSideType joinBuildSide)
        { return opHandler->getStartOfSliceCacheEntries(workerThreadId, joinBuildSide); },
        globalOperatorHandler,
        executionCtx.getWorkerThreadId(),
        nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide));
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

int8_t* createNewHJSliceProxy(
    SliceCacheEntry* sliceCacheEntry,
    OperatorHandler* ptrOpHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const QueryCompilation::JoinBuildSideType buildSide,
    const HJBuildCache* buildOperator)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    PRECONDITION(buildOperator != nullptr, "build operator should not be null!");
    const auto* opHandler = dynamic_cast<HJOperatorHandler*>(ptrOpHandler);
    const auto createFunction = opHandler->getCreateNewSlicesFunction();
    const auto newHJSlice
        = dynamic_cast<HJSlice*>(opHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, createFunction)[0].get());

    /// Updating the slice cache entry with the new slice
    newHJSlice->setCleanupFunction(buildOperator->getSliceCleanupFunction(opHandler->getNautilusCleanupExec(buildSide)), buildSide);
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        buildOperator->keySize, buildOperator->valueSize, buildOperator->pageSize, buildOperator->numberOfBuckets};
    sliceCacheEntry->sliceStart = newHJSlice->getSliceStart();
    sliceCacheEntry->sliceEnd = newHJSlice->getSliceEnd();
    sliceCacheEntry->dataStructure
        = reinterpret_cast<int8_t*>(newHJSlice->getHashMapPtrOrCreate(workerThreadId, buildSide, hashMapSliceArgs));
    return sliceCacheEntry->dataStructure;
}

void HJBuildCache::execute(ExecutionContext& executionCtx, Record& record) const
{
    /// Getting the slice cache from the local state
    const auto sliceCache = dynamic_cast<SliceCache*>(executionCtx.getLocalState(this));

    /// Getting the current hash map that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(executionCtx, record);
    const auto hashMapPtr = sliceCache->getDataStructureRef(
        timestamp,
        [&](const nautilus::val<SliceCacheEntry*>& sliceCacheEntryToReplace)
        {
            const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerIndex);
            return nautilus::invoke(
                createNewHJSliceProxy,
                sliceCacheEntryToReplace,
                globalOperatorHandler,
                timestamp,
                executionCtx.getWorkerThreadId(),
                nautilus::val<QueryCompilation::JoinBuildSideType>(joinBuildSide),
                nautilus::val<const HJBuildCache*>(this));
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
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the underyling paged vector
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, fieldKeys, fieldValues);
            const auto state = entryRefReset.getValueMemArea();
            nautilus::invoke(
                +[](int8_t* pagedVectorMemArea) -> void
                {
                    /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
                    auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
                    new (pagedVector) Nautilus::Interface::PagedVector();
                },
                state);
        },
        executionCtx.pipelineMemoryProvider.bufferProvider);

    /// Inserting the tuple into the corresponding hash entry
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(hashMapEntry, fieldKeys, fieldValues);
    auto entryMemArea = entryRef.getValueMemArea();
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(entryMemArea, memoryProvider);
    pagedVectorRef.writeRecord(record, executionCtx.pipelineMemoryProvider.bufferProvider);
}
}
