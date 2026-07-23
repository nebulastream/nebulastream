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

#pragma once

#include <functional>
#include <memory>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/TimestampRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceCache/SliceCache.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SliceCacheConfiguration.hpp>
#include <WindowBasedOperatorHandler.hpp>
#include <function.hpp>
#include <val_base.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// Concrete SliceStoreRef shared by every store. `Store` supplies the get-or-create slice lookup and
/// the per-pipeline cache allocation. `Handler` is the concrete operator handler the cache-miss
/// create-function expects. The base hands it a generic OperatorHandler*, which this ref
/// dynamic_casts to `Handler`.
template <typename Store, typename Handler>
class CachingSliceStoreRef final : public SliceStoreRef
{
public:
    /// Extracts the operator-specific data structure (e.g. a TupleBuffer) from a Slice.
    using DataStructureExtractor = std::function<void*(Slice&, WorkerThreadId)>;
    /// Creates new slices given a start and end timestamp.
    using SliceCreateFunction = std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>;
    using CreateSlicesFunction = std::function<SliceCreateFunction(Handler&, AbstractBufferProvider&)>;

    explicit CachingSliceStoreRef(
        const SliceCacheConfiguration& sliceCacheConfiguration,
        Store* sliceStore,
        DataStructureExtractor dataStructureExtractor,
        CreateSlicesFunction createSlicesFunction)
        : dataStructureExtractor(std::move(dataStructureExtractor))
        , createSlicesFunction(std::move(createSlicesFunction))
        , sliceCache(SliceCache::createSliceCache(sliceCacheConfiguration))
        , sliceStore(sliceStore)
    {
    }

    CachingSliceStoreRef(const CachingSliceStoreRef& other)
        : dataStructureExtractor(other.dataStructureExtractor)
        , createSlicesFunction(other.createSlicesFunction)
        , sliceCache(other.sliceCache->clone())
        , sliceStore(other.sliceStore)
    {
    }

    CachingSliceStoreRef(CachingSliceStoreRef&& other) = default;
    CachingSliceStoreRef& operator=(const CachingSliceStoreRef&) = delete;
    CachingSliceStoreRef& operator=(CachingSliceStoreRef&&) = delete;
    ~CachingSliceStoreRef() override = default;

    nautilus::val<SliceCacheEntry::DataStructure> getDataStructureRef(
        const nautilus::val<Timestamp>& timestamp,
        const nautilus::val<WorkerThreadId>& workerThreadId,
        const nautilus::val<OperatorHandler*>& operatorHandler,
        nautilus::val<AbstractBufferProvider*> bufferProvider) override
    {
        nautilus::val<Store*> sliceStoreVal{sliceStore};
        return sliceCache->getDataStructureRef(
            timestamp,
            workerThreadId,
            [&](const nautilus::val<SliceCacheEntry*>& entryToReplace)
            {
                nautilus::invoke(
                    cacheMissProxy,
                    entryToReplace,
                    operatorHandler,
                    timestamp,
                    workerThreadId,
                    nautilus::val<const CachingSliceStoreRef*>(this),
                    sliceStoreVal,
                    bufferProvider);
            });
    }

    void setupSliceStore(const nautilus::val<PipelineExecutionContext*>& pipelineCtx) override
    {
        nautilus::invoke(setupProxy, nautilus::val<Store*>{sliceStore}, pipelineCtx, nautilus::val<CachingSliceStoreRef*>{this});
    }

    std::unique_ptr<SliceStoreRef> clone() override { return std::make_unique<CachingSliceStoreRef>(*this); }

private:
    /// Cache-miss boundary called from JIT-compiled code via nautilus::invoke. Resolves (or creates) the slice
    /// for `timestamp` and records its data-structure pointer into the cache entry.
    static void cacheMissProxy(
        SliceCacheEntry* entryToReplace,
        OperatorHandler* operatorHandlerPtr,
        const Timestamp timestamp,
        const WorkerThreadId workerThreadId,
        const CachingSliceStoreRef* sliceStoreRef,
        Store* sliceStore,
        AbstractBufferProvider* bufferProvider)
    {
        PRECONDITION(operatorHandlerPtr != nullptr, "The operator handler should not be null");
        PRECONDITION(sliceStoreRef != nullptr, "The slice store ref should not be null");
        PRECONDITION(sliceStore != nullptr, "The slice store should not be null");
        PRECONDITION(bufferProvider != nullptr, "The buffer provider should not be null");

        auto* handler = dynamic_cast<Handler*>(operatorHandlerPtr);
        PRECONDITION(handler != nullptr, "The operator handler has an unexpected type");

        const auto createFunction = sliceStoreRef->createSlicesFunction(*handler, *bufferProvider);
        const auto slices = sliceStore->getSlicesOrCreate(timestamp, createFunction);
        INVARIANT(slices.size() == 1, "Expected exactly one slice for the given timestamp, but got {}", slices.size());

        entryToReplace->sliceStart = slices[0]->getSliceStart().getRawValue();
        entryToReplace->sliceEnd = slices[0]->getSliceEnd().getRawValue();
        entryToReplace->dataStructure = sliceStoreRef->dataStructureExtractor(*slices[0], workerThreadId);
    }

    static void setupProxy(Store* sliceStore, const PipelineExecutionContext* pipelineCtx, CachingSliceStoreRef* self)
    {
        PRECONDITION(sliceStore != nullptr, "The slice store should not be null");
        PRECONDITION(pipelineCtx->getBufferManager() != nullptr, "bufferProvider should not be null!");

        /// Order matters: the cache sizes its per-worker entries off the worker count, so set it first.
        self->sliceCache->setNumberOfWorkerThreads(pipelineCtx->getNumberOfWorkerThreads());
        const auto startOfEntries = sliceStore->allocateSpaceForSliceCache(
            self->sliceCache->getCacheMemorySize(), pipelineCtx->getPipelineId(), *pipelineCtx->getBufferManager());
        self->sliceCache->setStartOfEntries(startOfEntries);
    }

    DataStructureExtractor dataStructureExtractor;
    CreateSlicesFunction createSlicesFunction;

    /// Having these as C++ values is fine, as they do not change between tracing and runtime of the query.
    std::unique_ptr<SliceCache> sliceCache;
    Store* sliceStore;
};

}
