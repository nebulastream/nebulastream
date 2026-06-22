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
#include <Identifiers/Identifiers.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/TimestampRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/Spill/SpillManager.hpp>
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
void defaultTimeBasedSliceStoreRefCacheMissProxy(
    SliceCacheEntry* entryToReplace,
    OperatorHandler* operatorHandlerPtr,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const DefaultTimeBasedSliceStoreRef* sliceStoreRef,
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

    /// Pin (and reload if evicted) the resolved slice for this worker before handing out a pointer into its state, so a
    /// concurrent eviction cannot discard the arena pages we are about to access. No-op when spilling is disabled.
    windowHandler->pinSliceForBuild(workerThreadId, slices[0]);

    /// Use the data structure extractor to get the operator-specific data structure, then store its pointer for usage in nautilus
    entryToReplace->sliceStart = slices[0]->getSliceStart().getRawValue();
    entryToReplace->sliceEnd = slices[0]->getSliceEnd().getRawValue();
    entryToReplace->dataStructure = sliceStoreRef->dataStructureExtractor(*slices[0], workerThreadId);
}

DefaultTimeBasedSliceStoreRef::DefaultTimeBasedSliceStoreRef(
    SliceCacheConfiguration sliceCacheConfiguration,
    DefaultTimeBasedSliceStore* sliceStore,
    DataStructureExtractor dataStructureExtractor,
    CreateSlicesFunction createSlicesFunction)
    : dataStructureExtractor(std::move(dataStructureExtractor))
    , createSlicesFunction(std::move(createSlicesFunction))
    , sliceCache(SliceCache::createSliceCache(sliceCacheConfiguration))
    , sliceStore(sliceStore)
{
}

std::unique_ptr<SliceStoreRef> DefaultTimeBasedSliceStoreRef::clone()
{
    return std::make_unique<DefaultTimeBasedSliceStoreRef>(*this);
}

DefaultTimeBasedSliceStoreRef::DefaultTimeBasedSliceStoreRef(const DefaultTimeBasedSliceStoreRef& other)
    : dataStructureExtractor(other.dataStructureExtractor)
    , createSlicesFunction(other.createSlicesFunction)
    , sliceCache(other.sliceCache->clone())
    , sliceStore(other.sliceStore)
{
}

nautilus::val<SliceCacheEntry::DataStructure> DefaultTimeBasedSliceStoreRef::getDataStructureRef(
    const nautilus::val<Timestamp>& timestamp,
    const nautilus::val<WorkerThreadId>& workerThreadId,
    const nautilus::val<OperatorHandler*>& operatorHandler,
    nautilus::val<AbstractBufferProvider*> bufferProvider)
{
    nautilus::val<DefaultTimeBasedSliceStore*> sliceStoreVal{sliceStore};
    return sliceCache->getDataStructureRef(
        timestamp,
        workerThreadId,
        [&](const nautilus::val<SliceCacheEntry*>& entryToReplace)
        {
            nautilus::invoke(
                defaultTimeBasedSliceStoreRefCacheMissProxy,
                entryToReplace,
                operatorHandler,
                timestamp,
                workerThreadId,
                nautilus::val<const DefaultTimeBasedSliceStoreRef*>(this),
                sliceStoreVal,
                bufferProvider);
        });
}

void setupSliceStoreProxy(
    DefaultTimeBasedSliceStore* sliceStore, const PipelineExecutionContext* pipelineCtx, DefaultTimeBasedSliceStoreRef* self)
{
    PRECONDITION(sliceStore != nullptr, "The slice store not be null");
    PRECONDITION(pipelineCtx->getBufferManager() != nullptr, "bufferProvider should not be null!");

    /// State spilling relies on every slice access going through the cache-miss proxy (so the proxy can pin/reload the
    /// slice). A caching slice cache would serve a hit with a raw pointer into a possibly-evicted arena -> corruption.
    /// Therefore spilling requires the no-op slice cache (enable_slice_cache=false).
    if (const auto& spillManager = pipelineCtx->getSpillManager(); spillManager != nullptr && spillManager->configuration().enabled)
    {
        PRECONDITION(
            self->sliceCache->alwaysMisses(),
            "State spilling (enable_state_spilling=true) requires the slice cache to be disabled (enable_slice_cache=false)");
    }

    /// Creating new space for the slice cache of this pipeline
    /// The order is important. First, we need to set the number of worker threads, as the slice cache depends on it.
    self->sliceCache->setNumberOfWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
    const auto startOfEntries = sliceStore->allocateSpaceForSliceCache(
        self->sliceCache->getCacheMemorySize(), pipelineCtx->getPipelineId(), *pipelineCtx->getBufferManager());
    self->sliceCache->setStartOfEntries(startOfEntries);
}

void DefaultTimeBasedSliceStoreRef::setupSliceStore(const nautilus::val<PipelineExecutionContext*>& pipelineCtx)
{
    nautilus::invoke(
        setupSliceStoreProxy,
        nautilus::val<DefaultTimeBasedSliceStore*>{sliceStore},
        pipelineCtx,
        nautilus::val<DefaultTimeBasedSliceStoreRef*>{this});
}

}
