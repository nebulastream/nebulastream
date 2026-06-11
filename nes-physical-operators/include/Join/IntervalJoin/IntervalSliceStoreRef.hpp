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

#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceCache/SliceCache.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <SliceCacheConfiguration.hpp>
#include <val_concepts.hpp>

namespace NES
{

class IntervalSliceStore;
class IntervalJoinOperatorHandler;

/// Concrete SliceStoreRef for IntervalSliceStore. Mirrors
/// DefaultTimeBasedSliceStoreRef in shape — owns a SliceCache, delegates the
/// build hot path through it, and routes cache misses to a proxy that talks
/// to the IntervalJoin-typed operator handler.
///
/// The only meaningful divergence from DefaultTimeBasedSliceStoreRef is the
/// CreateSlicesFunction's handler type. The base SliceStoreRef passes a
/// generic OperatorHandler*; each concrete ref dynamic_casts to its expected
/// handler subtype internally.
class IntervalSliceStoreRef final : public SliceStoreRef
{
public:
    using DataStructureExtractor = std::function<std::span<const std::byte>(Slice&, WorkerThreadId)>;
    using SliceCreateFunction = std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>;
    using CreateSlicesFunction = std::function<SliceCreateFunction(IntervalJoinOperatorHandler&)>;

    explicit IntervalSliceStoreRef(
        SliceCacheConfiguration sliceCacheConfiguration,
        IntervalSliceStore* sliceStore,
        DataStructureExtractor dataStructureExtractor,
        CreateSlicesFunction createSlicesFunction);
    IntervalSliceStoreRef(const IntervalSliceStoreRef& other);
    IntervalSliceStoreRef(IntervalSliceStoreRef&& other) = default;
    IntervalSliceStoreRef& operator=(const IntervalSliceStoreRef&) = delete;
    IntervalSliceStoreRef& operator=(IntervalSliceStoreRef&&) = delete;
    ~IntervalSliceStoreRef() override = default;

    nautilus::val<SliceCacheEntry::DataStructure> getDataStructureRef(
        const nautilus::val<Timestamp>& timestamp,
        const nautilus::val<WorkerThreadId>& workerThreadId,
        const nautilus::val<OperatorHandler*>& operatorHandler) override;

    void setupSliceStore(const nautilus::val<PipelineExecutionContext*>& pipelineCtx) override;
    std::unique_ptr<SliceStoreRef> clone() override;

private:
    friend void
    setupIntervalSliceStoreProxy(IntervalSliceStore* sliceStore, const PipelineExecutionContext* pipelineCtx, IntervalSliceStoreRef* self);
    friend void intervalSliceStoreRefCacheMissProxy(
        SliceCacheEntry* entryToReplace,
        OperatorHandler* operatorHandlerPtr,
        Timestamp timestamp,
        WorkerThreadId workerThreadId,
        const IntervalSliceStoreRef* sliceStoreRef,
        IntervalSliceStore* sliceStore);

    DataStructureExtractor dataStructureExtractor;
    CreateSlicesFunction createSlicesFunction;
    std::unique_ptr<SliceCache> sliceCache;
    IntervalSliceStore* sliceStore;
};

/// Free factory; mirrors DefaultTimeBasedSliceStore::createSliceStoreRef. Defined
/// in the .cpp so this header can stay free of the IntervalSliceStore body.
std::unique_ptr<SliceStoreRef> createIntervalSliceStoreRef(
    IntervalSliceStore& sliceStore,
    IntervalSliceStoreRef::DataStructureExtractor extractor,
    IntervalSliceStoreRef::CreateSlicesFunction creator,
    SliceCacheConfiguration sliceCacheConfiguration);

}
