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
#include <Aggregation/AggregationBuildCachePhysicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Configurations/Worker/SliceCacheConfiguration.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <SliceCache/SliceCache2Q.hpp>
#include <SliceCache/SliceCacheFIFO.hpp>
#include <SliceCache/SliceCacheLRU.hpp>
#include <SliceCache/SliceCacheSecondChance.hpp>
#include <SliceCache/SliceCacheUtil.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapSlice.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES
{

int8_t* createAndAddAggregationSliceToCache(
    SliceCacheEntry* sliceCacheEntry,
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    const auto aggregationSlice = getAggregationSliceProxy(operatorHandler, timestamp, buildOperator);
    sliceCacheEntry->sliceStart = aggregationSlice->getSliceStart();
    sliceCacheEntry->sliceEnd = aggregationSlice->getSliceEnd();
    sliceCacheEntry->dataStructure = reinterpret_cast<int8_t*>(aggregationSlice->getHashMapPtrOrCreate(workerThreadId));
    return sliceCacheEntry->dataStructure;
}


void AggregationBuildCachePhysicalOperator::setup(ExecutionContext& executionCtx, const nautilus::engine::NautilusEngine& engine) const
{
    AggregationBuildPhysicalOperator::setup(executionCtx, engine);

    /// I know that we should not set the numberOfWorkerThreads in the build operator, due to a race condition.
    /// However, we need to set it here, as we need to know the number of worker threads to allocate the slice cache entries.
    /// This should/is fine for this PoC
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
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
    }
    nautilus::invoke(
        +[](AggregationOperatorHandler* opHandler,
            Memory::AbstractBufferProvider* bufferProvider,
            const uint64_t sizeOfEntryVal,
            const uint64_t numberOfEntriesVal)
        { opHandler->allocateSliceCacheEntries(sizeOfEntryVal, numberOfEntriesVal, bufferProvider); },
        globalOperatorHandler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        sizeOfEntry,
        numberOfEntries);
}

void AggregationBuildCachePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    AggregationBuildPhysicalOperator::open(executionCtx, recordBuffer);

    /// Creating the local state for the slice cache, so that we can use it in the execute method
    const auto globalOperatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    const auto startOfSliceEntries = nautilus::invoke(
        +[](const AggregationOperatorHandler* opHandler, const WorkerThreadId workerThreadId)
        { return opHandler->getStartOfSliceCacheEntries(SliceCacheOperatorHandler::StartSliceCacheEntriesArgs{workerThreadId}); },
        globalOperatorHandler,
        executionCtx.workerThreadId);
    auto sliceCache = NES::Util::createSliceCache(sliceCacheOptions, globalOperatorHandler, startOfSliceEntries);
    executionCtx.setLocalOperatorState(id, std::move(sliceCache));
}

void AggregationBuildCachePhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the slice cache
    auto* sliceCache = dynamic_cast<SliceCache*>(ctx.getLocalState(id));
    auto globalOperatorHandler = sliceCache->getOperatorHandler();

    /// Getting the correspinding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr = sliceCache->getDataStructureRef(
        timestamp,
        [&](const nautilus::val<SliceCacheEntry*>& sliceCacheEntryToReplace, const nautilus::val<uint64_t>&)
        {
            return nautilus::invoke(
                createAndAddAggregationSliceToCache,
                sliceCacheEntryToReplace,
                globalOperatorHandler,
                timestamp,
                ctx.workerThreadId,
                nautilus::val<const AggregationBuildCachePhysicalOperator*>(this));
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
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(
                entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                aggFunction->reset(state, ctx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.pipelineMemoryProvider.bufferProvider);


    /// Updating the aggregation states
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(
        hashMapEntry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
    auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
    {
        aggFunction->lift(state, ctx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

AggregationBuildCachePhysicalOperator::AggregationBuildCachePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
    HashMapOptions hashMapOptions,
    const NES::Configurations::SliceCacheOptions sliceCacheOptions)
    : AggregationBuildPhysicalOperator(
          operatorHandlerId, std::move(timeFunction), std::move(aggregationFunctions), std::move(hashMapOptions))
    , sliceCacheOptions(sliceCacheOptions)
{
}

}
