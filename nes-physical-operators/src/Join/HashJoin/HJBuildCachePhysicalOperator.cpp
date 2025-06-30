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

#include <Join/HashJoin/HJBuildCachePhysicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <Configurations/Worker/SliceCacheConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <SliceCache/SliceCache2Q.hpp>
#include <SliceCache/SliceCacheFIFO.hpp>
#include <SliceCache/SliceCacheLRU.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>
#include <SliceCache/SliceCacheUtil.hpp>
#include <Time/Timestamp.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>


namespace NES
{

int8_t* createAndAddHashJoinSliceToCache(
    SliceCacheEntry* sliceCacheEntry,
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType buildSide,
    const HJBuildCachePhysicalOperator* buildOperator)
{
    const auto hjSlice = getHJSliceProxy(operatorHandler, timestamp, buildOperator);
    INVARIANT(hjSlice != nullptr, "HJSlice can not be null");

    /// Updating the slice cache entry with the new slice
    sliceCacheEntry->sliceStart = hjSlice->getSliceStart();
    sliceCacheEntry->sliceEnd = hjSlice->getSliceEnd();
    sliceCacheEntry->dataStructure = reinterpret_cast<int8_t*>(hjSlice->getHashMapPtrOrCreate(workerThreadId, buildSide));
    return sliceCacheEntry->dataStructure;
}

void HJBuildCachePhysicalOperator::setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const
{
    HJBuildPhysicalOperator::setup(executionCtx, engine);

    /// I know that we should not set the numberOfWorkerThreads in the build operator, due to a race condition.
    /// However, we need to set it here, as we need to know the number of worker threads to allocate the slice cache entries.
    /// This should/is fine for this PoC
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
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
        case NES::Configurations::SliceCacheType::NONE:
            return;
        case NES::Configurations::SliceCacheType::FIFO:
            sizeOfEntry = sizeof(SliceCacheEntryFIFO);
            break;
        case NES::Configurations::SliceCacheType::LRU:
            sizeOfEntry = sizeof(SliceCacheEntryLRU);
            break;
        case NES::Configurations::SliceCacheType::SECOND_CHANCE:
            sizeOfEntry = sizeof(SliceCacheEntrySecondChance);
            break;
        case NES::Configurations::SliceCacheType::TWO_QUEUES:
            sizeOfEntry = sizeof(SliceCacheEntry2Q);
            break;
            std::unreachable();
    }
    nautilus::invoke(
        +[](HJOperatorHandler* opHandler,
            Memory::AbstractBufferProvider* bufferProvider,
            const uint64_t sizeOfEntryVal,
            const uint64_t numberOfEntriesVal)
        { opHandler->allocateSliceCacheEntries(sizeOfEntryVal, numberOfEntriesVal, bufferProvider); },
        globalOperatorHandler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        sizeOfEntry,
        numberOfEntries);
}

void HJBuildCachePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    HJBuildPhysicalOperator::open(executionCtx, recordBuffer);

    /// Creating the local state for the slice cache, so that we can use it in the execute method
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto startOfSliceEntries = nautilus::invoke(
        +[](const HJOperatorHandler* opHandler, const WorkerThreadId workerThreadId, const JoinBuildSideType joinBuildSide)
        {
            return opHandler->getStartOfSliceCacheEntries(
                StreamJoinOperatorHandler::StartSliceCacheEntriesStreamJoin{workerThreadId, joinBuildSide});
        },
        globalOperatorHandler,
        executionCtx.workerThreadId,
        nautilus::val<JoinBuildSideType>(joinBuildSide));
    auto sliceCache = NES::Util::createSliceCache(sliceCacheOptions, globalOperatorHandler, startOfSliceEntries);
    executionCtx.setLocalOperatorState(id, std::move(sliceCache));
}

void HJBuildCachePhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the slice cache
    auto* sliceCache = dynamic_cast<SliceCache*>(ctx.getLocalState(id));
    auto operatorHandler = sliceCache->getOperatorHandler();

    /// Get the current slice / hash map that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr = sliceCache->getDataStructureRef(
        timestamp,
        [&](const nautilus::val<SliceCacheEntry*>& sliceCacheEntryToReplace, const nautilus::val<uint64_t>&)
        {
            return nautilus::invoke(
                createAndAddHashJoinSliceToCache,
                sliceCacheEntryToReplace,
                sliceCache->getOperatorHandler(),
                timestamp,
                ctx.workerThreadId,
                nautilus::val<JoinBuildSideType>(joinBuildSide),
                nautilus::val<const HJBuildCachePhysicalOperator*>(this));
        });

    Interface::ChainedHashMapRef hashMap{
        hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize};

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapOptions.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapOptions.fieldKeys[i];
        const auto& function = hashMapOptions.keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashMapOptions.hashFunction,
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the underyling paged vector
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset{
                entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
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
        ctx.pipelineMemoryProvider.bufferProvider);

    /// Inserting the tuple into the corresponding hash entry
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef{
        hashMapEntry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
    auto entryMemArea = entryRef.getValueMemArea();
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(entryMemArea, memoryProvider);
    pagedVectorRef.writeRecord(record, ctx.pipelineMemoryProvider.bufferProvider);
}

HJBuildCachePhysicalOperator::HJBuildCachePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& memoryProvider,
    HashMapOptions hashMapOptions,
    NES::Configurations::SliceCacheOptions sliceCacheOptions)
    : HJBuildPhysicalOperator(operatorHandlerId, joinBuildSide, std::move(timeFunction), memoryProvider, std::move(hashMapOptions))
    , sliceCacheOptions(sliceCacheOptions)
{
}

}
