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
#include <Runtime/AbstractBufferProvider.hpp>
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
class PipelineExecutionContext;

template <class SliceType, class DataStructureType>
class DefaultTimeBasedSliceStoreRef;

/// Forward declarations of the cache-miss / setup proxies, so each class specialization can befriend its own.
template <class SliceType, class DataStructureType>
void setupSliceStoreProxy(
    DefaultTimeBasedSliceStore* sliceStore,
    const PipelineExecutionContext* pipelineCtx,
    DefaultTimeBasedSliceStoreRef<SliceType, DataStructureType>* self);
template <class SliceType, class DataStructureType>
void defaultTimeBasedSliceStoreRefCacheMissProxy(
    SliceCacheEntry* entryToReplace,
    OperatorHandler* operatorHandlerPtr,
    Timestamp timestamp,
    WorkerThreadId workerThreadId,
    const DefaultTimeBasedSliceStoreRef<SliceType, DataStructureType>* sliceStoreRef,
    DefaultTimeBasedSliceStore* sliceStore,
    AbstractBufferProvider* bufferProvider);

/// Concrete SliceStoreRef for the DefaultTimeBasedSliceStore, specialized on the operator's slice and data-structure type.
/// @tparam SliceType the concrete Slice subtype this store holds (e.g. AggregationSlice, HJSlice, NLJSlice).
/// @tparam DataStructureType the operator-specific data-structure pointer (e.g. HashMap*, TupleBuffer*).
/// Maintains a per-pipeline SliceCache to avoid repeated slice store lookups on the hot path.
/// On cache miss, calls the slice store via nautilus::invoke to get or create the slice.
template <class SliceType, class DataStructureType>
class DefaultTimeBasedSliceStoreRef final : public SliceStoreRef<DataStructureType>
{
public:
    /// Extracts the operator-specific data structure from a statically typed slice (no runtime dynamic_cast).
    using DataStructureExtractor = std::function<DataStructureType(SliceType&, WorkerThreadId)>;

    /// The function that creates new slices given a start and end timestamp.
    using SliceCreateFunction = std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>;
    using CreateSlicesFunction = std::function<SliceCreateFunction(WindowBasedOperatorHandler&, AbstractBufferProvider&)>;

    explicit DefaultTimeBasedSliceStoreRef(
        SliceCacheConfiguration sliceCacheConfiguration,
        DefaultTimeBasedSliceStore* sliceStore,
        DataStructureExtractor dataStructureExtractor,
        CreateSlicesFunction createSlicesFunction);
    DefaultTimeBasedSliceStoreRef(const DefaultTimeBasedSliceStoreRef& other);
    DefaultTimeBasedSliceStoreRef(DefaultTimeBasedSliceStoreRef&& other) = default;
    DefaultTimeBasedSliceStoreRef& operator=(const DefaultTimeBasedSliceStoreRef&) = delete;
    DefaultTimeBasedSliceStoreRef& operator=(DefaultTimeBasedSliceStoreRef&&) = delete;

    nautilus::val<DataStructureType> getDataStructureRef(
        const nautilus::val<Timestamp>& timestamp,
        const nautilus::val<WorkerThreadId>& workerThreadId,
        const nautilus::val<OperatorHandler*>& operatorHandler,
        nautilus::val<AbstractBufferProvider*> bufferProvider) override;

    void setupSliceStore(const nautilus::val<PipelineExecutionContext*>& pipelineCtx) override;
    ~DefaultTimeBasedSliceStoreRef() override = default;
    std::unique_ptr<SliceStoreRef<DataStructureType>> clone() override;

private:
    /// The proxies need access to private members (sliceCache, extractor, createSlicesFunction) to service cache misses.
    friend void setupSliceStoreProxy<SliceType, DataStructureType>(
        DefaultTimeBasedSliceStore* sliceStore, const PipelineExecutionContext* pipelineCtx, DefaultTimeBasedSliceStoreRef* self);
    friend void defaultTimeBasedSliceStoreRefCacheMissProxy<SliceType, DataStructureType>(
        SliceCacheEntry* entryToReplace,
        OperatorHandler* operatorHandlerPtr,
        Timestamp timestamp,
        WorkerThreadId workerThreadId,
        const DefaultTimeBasedSliceStoreRef* sliceStoreRef,
        DefaultTimeBasedSliceStore* sliceStore,
        AbstractBufferProvider* bufferProvider);

    DataStructureExtractor dataStructureExtractor;
    CreateSlicesFunction createSlicesFunction;

    /// Having these as C++ values is fine, as they do not change between tracing and runtime of the query.
    std::unique_ptr<SliceCache> sliceCache;
    DefaultTimeBasedSliceStore* sliceStore;
};

}
