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

#include <Join/IntervalJoin/IntervalSliceStoreRef.hpp>

#include <memory>
#include <utility>
#include <Identifiers/Identifiers.hpp>
#include <Join/IntervalJoin/IntervalJoinOperatorHandler.hpp>
#include <Join/IntervalJoin/IntervalSliceStore.hpp>
#include <Interface/NESStrongTypeRef.hpp>
#include <Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/SliceCache/SliceCache.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SliceCacheConfiguration.hpp>
#include <function.hpp>
#include <val_ptr.hpp>

namespace NES
{

void intervalSliceStoreRefCacheMissProxy(
    SliceCacheEntry* entryToReplace,
    OperatorHandler* operatorHandlerPtr,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const IntervalSliceStoreRef* sliceStoreRef,
    IntervalSliceStore* sliceStore)
{
    PRECONDITION(operatorHandlerPtr != nullptr, "operator handler should not be null");
    PRECONDITION(sliceStoreRef != nullptr, "slice store ref should not be null");
    PRECONDITION(sliceStore != nullptr, "slice store should not be null");

    auto* intervalHandler = dynamic_cast<IntervalJoinOperatorHandler*>(operatorHandlerPtr);
    PRECONDITION(intervalHandler != nullptr, "operator handler should be an IntervalJoinOperatorHandler");

    const auto createFunction = sliceStoreRef->createSlicesFunction(*intervalHandler);
    const auto newSlices = sliceStore->getSlicesOrCreate(timestamp, createFunction);
    INVARIANT(newSlices.size() == 1, "Interval-join expects one slice per timestamp; got {}", newSlices.size());

    entryToReplace->sliceStart = newSlices[0]->getSliceStart().getRawValue();
    entryToReplace->sliceEnd = newSlices[0]->getSliceEnd().getRawValue();
    entryToReplace->dataStructure = sliceStoreRef->dataStructureExtractor(*newSlices[0], workerThreadId).data();
}

IntervalSliceStoreRef::IntervalSliceStoreRef(
    SliceCacheConfiguration sliceCacheConfiguration,
    IntervalSliceStore* sliceStore,
    DataStructureExtractor dataStructureExtractor,
    CreateSlicesFunction createSlicesFunction)
    : dataStructureExtractor(std::move(dataStructureExtractor))
    , createSlicesFunction(std::move(createSlicesFunction))
    , sliceCache(SliceCache::createSliceCache(sliceCacheConfiguration))
    , sliceStore(sliceStore)
{
}

IntervalSliceStoreRef::IntervalSliceStoreRef(const IntervalSliceStoreRef& other)
    : dataStructureExtractor(other.dataStructureExtractor)
    , createSlicesFunction(other.createSlicesFunction)
    , sliceCache(other.sliceCache->clone())
    , sliceStore(other.sliceStore)
{
}

std::unique_ptr<SliceStoreRef> IntervalSliceStoreRef::clone()
{
    return std::make_unique<IntervalSliceStoreRef>(*this);
}

nautilus::val<SliceCacheEntry::DataStructure> IntervalSliceStoreRef::getDataStructureRef(
    const nautilus::val<Timestamp>& timestamp,
    const nautilus::val<WorkerThreadId>& workerThreadId,
    const nautilus::val<OperatorHandler*>& operatorHandler)
{
    nautilus::val<IntervalSliceStore*> sliceStoreVal{sliceStore};
    return sliceCache->getDataStructureRef(
        timestamp,
        workerThreadId,
        [&](const nautilus::val<SliceCacheEntry*>& entryToReplace)
        {
            nautilus::invoke(
                intervalSliceStoreRefCacheMissProxy,
                entryToReplace,
                operatorHandler,
                timestamp,
                workerThreadId,
                nautilus::val<const IntervalSliceStoreRef*>(this),
                sliceStoreVal);
        });
}

void setupIntervalSliceStoreProxy(IntervalSliceStore* sliceStore, const PipelineExecutionContext* pipelineCtx, IntervalSliceStoreRef* self)
{
    PRECONDITION(sliceStore != nullptr, "slice store should not be null");
    PRECONDITION(pipelineCtx->getBufferManager() != nullptr, "buffer manager should not be null");

    self->sliceCache->setNumberOfWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
    const auto startOfEntries = sliceStore->allocateSpaceForSliceCache(
        self->sliceCache->getCacheMemorySize(), pipelineCtx->getPipelineId(), *pipelineCtx->getBufferManager());
    self->sliceCache->setStartOfEntries(startOfEntries);
}

void IntervalSliceStoreRef::setupSliceStore(const nautilus::val<PipelineExecutionContext*>& pipelineCtx)
{
    nautilus::invoke(
        setupIntervalSliceStoreProxy,
        nautilus::val<IntervalSliceStore*>{sliceStore},
        pipelineCtx,
        nautilus::val<IntervalSliceStoreRef*>{this});
}

std::unique_ptr<SliceStoreRef> createIntervalSliceStoreRef(
    IntervalSliceStore& sliceStore,
    IntervalSliceStoreRef::DataStructureExtractor extractor,
    IntervalSliceStoreRef::CreateSlicesFunction creator,
    SliceCacheConfiguration sliceCacheConfiguration)
{
    return std::make_unique<IntervalSliceStoreRef>(
        std::move(sliceCacheConfiguration), &sliceStore, std::move(extractor), std::move(creator));
}

}
