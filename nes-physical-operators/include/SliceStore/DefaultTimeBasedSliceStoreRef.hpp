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
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SliceCache/SliceCache.hpp>
#include <SliceStore/SliceStoreRef.hpp>
#include <Time/Timestamp.hpp>
#include <SliceCacheConfiguration.hpp>
#include <val_concepts.hpp>

namespace NES
{
class DefaultTimeBasedSliceStore;

/// Concrete SliceStoreRef for the DefaultTimeBasedSliceStore.
/// Maintains a per-pipeline SliceCache to avoid repeated slice store lookups on the hot path.
/// On cache miss, calls the slice store via nautilus::invoke to get or create the slice.
class DefaultTimeBasedSliceStoreRef final : public SliceStoreRef
{
public:
    /// Extracts the operator-specific data structure (HashMap*, PagedVector*) from a Slice.
    using DataStructureExtractor = std::function<std::span<const std::byte>(Slice&, WorkerThreadId)>;

    /// The function that creates new slices given a start and end timestamp.
    using SliceCreateFunction = std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>;
    using CreateSlicesFunction = std::function<SliceCreateFunction(WindowBasedOperatorHandler&)>;

    explicit DefaultTimeBasedSliceStoreRef(
        SliceCacheConfiguration sliceCacheConfiguration,
        DefaultTimeBasedSliceStore* sliceStore,
        DataStructureExtractor dataStructureExtractor,
        CreateSlicesFunction createSlicesFunction);
    DefaultTimeBasedSliceStoreRef(const DefaultTimeBasedSliceStoreRef& other);
    DefaultTimeBasedSliceStoreRef(DefaultTimeBasedSliceStoreRef&& other) = default;
    DefaultTimeBasedSliceStoreRef& operator=(const DefaultTimeBasedSliceStoreRef&) = delete;
    DefaultTimeBasedSliceStoreRef& operator=(DefaultTimeBasedSliceStoreRef&&) = delete;

    nautilus::val<SliceCacheEntry::DataStructure> getDataStructureRef(
        const nautilus::val<Timestamp>& timestamp,
        const nautilus::val<WorkerThreadId>& workerThreadId,
        const nautilus::val<OperatorHandler*>& operatorHandler) override;

    void setupSliceStore(const nautilus::val<PipelineExecutionContext*>& pipelineCtx) override;
    ~DefaultTimeBasedSliceStoreRef() override = default;
    std::unique_ptr<SliceStoreRef> clone() override;

private:
    /// They need access to private members (sliceCaches, sliceCacheConfiguration) to create and look up per-pipeline caches.
    friend void setupSliceStoreProxy(
        DefaultTimeBasedSliceStore* sliceStore, const PipelineExecutionContext* pipelineCtx, DefaultTimeBasedSliceStoreRef* self);
    friend void defaultTimeBasedSliceStoreRefCacheMissProxy(
        SliceCacheEntry* entryToReplace,
        OperatorHandler* operatorHandlerPtr,
        Timestamp timestamp,
        WorkerThreadId workerThreadId,
        const DefaultTimeBasedSliceStoreRef* sliceStoreRef,
        DefaultTimeBasedSliceStore* sliceStore);

    DataStructureExtractor dataStructureExtractor;
    CreateSlicesFunction createSlicesFunction;

    /// Having these as C++ values is fine, as they do not change between tracing and runtime of the query.
    std::unique_ptr<SliceCache> sliceCache;
    DefaultTimeBasedSliceStore* sliceStore;
};

}
