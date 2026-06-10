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

#include <SliceStore/DefaultTimeBasedSliceStoreRef.hpp>

#include <memory>
#include <utility>
#include <Aggregation/AggregationSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/NestedLoopJoin/NLJSlice.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/SliceCache/SliceCache.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SliceCacheConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <function.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Unified proxy function that handles slice cache misses for all build operators.
/// Called via nautilus::invoke from JIT-compiled code when a slice is not found in the cache.
template <class SliceType, class DataStructureType>
void defaultTimeBasedSliceStoreRefCacheMissProxy(
    SliceCacheEntry* entryToReplace,
    OperatorHandler* operatorHandlerPtr,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const DefaultTimeBasedSliceStoreRef<SliceType, DataStructureType>* sliceStoreRef,
    DefaultTimeBasedSliceStore* sliceStore,
    AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(operatorHandlerPtr != nullptr, "The operator handler should not be null");
    PRECONDITION(sliceStoreRef != nullptr, "The slice store ref should not be null");
    PRECONDITION(sliceStore != nullptr, "The slice store should not be null");
    PRECONDITION(bufferProvider != nullptr, "The buffer provider should not be null");

    auto* windowHandler = dynamic_cast<WindowBasedOperatorHandler*>(operatorHandlerPtr);
    PRECONDITION(windowHandler != nullptr, "The operator handler should be a WindowBasedOperatorHandler");

    /// Get a fresh SliceCreateFunction from the operator handler (picks up dynamic state like rolling avg bucket count for the hash join)
    const auto createFunction = sliceStoreRef->createSlicesFunction(*windowHandler, *bufferProvider);

    /// Look up or create the slice in the slice store
    const auto slices = sliceStore->getSlicesOrCreate(timestamp, createFunction);
    INVARIANT(slices.size() == 1, "Expected exactly one slice for the given timestamp, but got {}", slices.size());

    /// This store instance only holds SliceType, so the static_cast is fine
    auto& slice = *slices[0];
    INVARIANT(dynamic_cast<SliceType*>(&slice) != nullptr, "Slice is not of the expected type for this slice store ref");

    /// Extract the operator-specific data structure from the typed slice and store its pointer for nautilus.
    entryToReplace->sliceStart = slice.getSliceStart().getRawValue();
    entryToReplace->sliceEnd = slice.getSliceEnd().getRawValue();
    entryToReplace->dataStructure = sliceStoreRef->dataStructureExtractor(static_cast<SliceType&>(slice), workerThreadId);
}

template <class SliceType, class DataStructureType>
DefaultTimeBasedSliceStoreRef<SliceType, DataStructureType>::DefaultTimeBasedSliceStoreRef(
    SliceCacheConfiguration sliceCacheConfiguration,
    DefaultTimeBasedSliceStore* sliceStore,
    DataStructureExtractor dataStructureExtractor,
    CreateSlicesFunction createSlicesFunction)
    : dataStructureExtractor(std::move(dataStructureExtractor))
    , createSlicesFunction(std::move(createSlicesFunction))
    , sliceCache(SliceCache::createSliceCache(std::move(sliceCacheConfiguration)))
    , sliceStore(sliceStore)
{
}

template <class SliceType, class DataStructureType>
DefaultTimeBasedSliceStoreRef<SliceType, DataStructureType>::DefaultTimeBasedSliceStoreRef(const DefaultTimeBasedSliceStoreRef& other)
    : dataStructureExtractor(other.dataStructureExtractor)
    , createSlicesFunction(other.createSlicesFunction)
    , sliceCache(other.sliceCache->clone())
    , sliceStore(other.sliceStore)
{
}

template <class SliceType, class DataStructureType>
std::unique_ptr<SliceStoreRef<DataStructureType>> DefaultTimeBasedSliceStoreRef<SliceType, DataStructureType>::clone()
{
    return std::make_unique<DefaultTimeBasedSliceStoreRef>(*this);
}

template <class SliceType, class DataStructureType>
nautilus::val<DataStructureType> DefaultTimeBasedSliceStoreRef<SliceType, DataStructureType>::getDataStructureRef(
    const nautilus::val<Timestamp>& timestamp,
    const nautilus::val<WorkerThreadId>& workerThreadId,
    const nautilus::val<OperatorHandler*>& operatorHandler,
    nautilus::val<AbstractBufferProvider*> bufferProvider)
{
    nautilus::val<DefaultTimeBasedSliceStore*> sliceStoreVal{sliceStore};
    const nautilus::val<SliceCacheEntry::DataStructure> rawDataStructure = sliceCache->getDataStructureRef(
        timestamp,
        workerThreadId,
        [&](const nautilus::val<SliceCacheEntry*>& entryToReplace)
        {
            nautilus::invoke(
                defaultTimeBasedSliceStoreRefCacheMissProxy<SliceType, DataStructureType>,
                entryToReplace,
                operatorHandler,
                timestamp,
                workerThreadId,
                nautilus::val<const DefaultTimeBasedSliceStoreRef*>(this),
                sliceStoreVal,
                bufferProvider);
        });
    return rawDataStructure;
}

template <class SliceType, class DataStructureType>
void setupSliceStoreProxy(
    DefaultTimeBasedSliceStore* sliceStore,
    const PipelineExecutionContext* pipelineCtx,
    DefaultTimeBasedSliceStoreRef<SliceType, DataStructureType>* self)
{
    PRECONDITION(sliceStore != nullptr, "The slice store not be null");
    PRECONDITION(pipelineCtx->getBufferManager() != nullptr, "bufferProvider should not be null!");

    /// Creating new space for the slice cache of this pipeline
    /// The order is important. First, we need to set the number of worker threads, as the slice cache depends on it.
    self->sliceCache->setNumberOfWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
    const auto startOfEntries = sliceStore->allocateSpaceForSliceCache(
        self->sliceCache->getCacheMemorySize(), pipelineCtx->getPipelineId(), *pipelineCtx->getBufferManager());
    self->sliceCache->setStartOfEntries(startOfEntries);
}

template <class SliceType, class DataStructureType>
void DefaultTimeBasedSliceStoreRef<SliceType, DataStructureType>::setupSliceStore(
    const nautilus::val<PipelineExecutionContext*>& pipelineCtx)
{
    nautilus::invoke(
        setupSliceStoreProxy<SliceType, DataStructureType>,
        nautilus::val<DefaultTimeBasedSliceStore*>{sliceStore},
        pipelineCtx,
        nautilus::val<DefaultTimeBasedSliceStoreRef*>{this});
}

/// Explicit instantiations for the operator/slice/data-structure combinations produced during lowering.
template class DefaultTimeBasedSliceStoreRef<AggregationSlice, HashMap*>;
template class DefaultTimeBasedSliceStoreRef<HJSlice, HashMap*>;
template class DefaultTimeBasedSliceStoreRef<NLJSlice, TupleBuffer*>;

}
