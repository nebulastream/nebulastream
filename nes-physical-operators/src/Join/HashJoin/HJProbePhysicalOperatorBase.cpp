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
#include <Join/HashJoin/HJProbePhysicalOperatorBase.hpp>

#include <cstdint>
#include <memory>
#include <optional>
#include <utility>
#include <DataTypes/UnboundSchema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/StreamJoinOperatorHandler.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

HJProbePhysicalOperatorBase::HJProbePhysicalOperatorBase(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    JoinSchema joinSchema,
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    ChainedHashMapConfig leftHashMapConfig,
    ChainedHashMapConfig rightHashMapConfig)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), std::move(joinSchema))
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
    , leftHashMapConfig(std::move(leftHashMapConfig))
    , rightHashMapConfig(std::move(rightHashMapConfig))
{
}

namespace
{
/// Loads the PagedVector child buffer referenced by a chained hash map entry's value area (which stores a child-buffer index).
PagedVectorRef
loadEntryPagedVector(const ChainedHashMapRef::ChainedEntryRef& entryRef, const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout)
{
    /// The entry's value area stores the child-buffer index of its paged vector; getChild loads that child as an owned buffer.
    auto pagedVectorBuffer
        = entryRef.hashMapBuffer.getChildFromIndexAddress(static_cast<nautilus::val<uint32_t*>>(entryRef.getValueMemArea()));
    /// Must move the owned buffer into the PagedVectorRef rather than wrap a BorrowedNautilusBuffer around it: a
    /// borrowed view only stores a pointer back to `pagedVectorBuffer`, which would dangle once this function returns.
    return PagedVectorRef{std::move(pagedVectorBuffer), tupleLayout};
}
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) inner join's N x N hash-map iteration is inherently deeply nested
void HJProbePhysicalOperatorBase::performMatchPairsProbe(
    const BorrowedNautilusBuffer& recordBuffer,
    nautilus::val<uint64_t> leftNumberOfHashMaps,
    nautilus::val<uint64_t> rightNumberOfHashMaps,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd,
    const bool iterateLeft) const
{
    if (leftNumberOfHashMaps == 0 or rightNumberOfHashMaps == 0)
    {
        return;
    }

    const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
    const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());

    /// The probe iterates all entries of one side's maps and looks each key up in the other side's maps. Iterating the side with
    /// fewer entries is cheaper; the caller decides which one that is.
    const auto& iteratedConfig = iterateLeft ? leftHashMapConfig : rightHashMapConfig;
    const auto& lookedUpConfig = iterateLeft ? rightHashMapConfig : leftHashMapConfig;
    const auto& iteratedLayout = iterateLeft ? leftTupleLayout : rightTupleLayout;
    const auto& lookedUpLayout = iterateLeft ? rightTupleLayout : leftTupleLayout;
    const auto iteratedNumberOfHashMaps = iterateLeft ? leftNumberOfHashMaps : rightNumberOfHashMaps;
    const auto lookedUpNumberOfHashMaps = iterateLeft ? rightNumberOfHashMaps : leftNumberOfHashMaps;
    /// Left hash map buffers are stored as child buffers first, the right ones right after all of the left ones
    const auto iteratedFirstChild = iterateLeft ? nautilus::val<uint64_t>{0} : leftNumberOfHashMaps;
    const auto lookedUpFirstChild = iterateLeft ? leftNumberOfHashMaps : nautilus::val<uint64_t>{0};

    for (nautilus::val<uint64_t> lookedUpIndex = 0; lookedUpIndex < lookedUpNumberOfHashMaps; ++lookedUpIndex)
    {
        auto lookedUpHashMapBuffer = recordBuffer.getChild(lookedUpFirstChild + lookedUpIndex);
        ChainedHashMapRef lookedUpHashMap{lookedUpHashMapBuffer, lookedUpConfig};
        for (nautilus::val<uint64_t> iteratedIndex = 0; iteratedIndex < iteratedNumberOfHashMaps; ++iteratedIndex)
        {
            auto iteratedHashMapBuffer = recordBuffer.getChild(iteratedFirstChild + iteratedIndex);
            const ChainedHashMapRef iteratedHashMap{iteratedHashMapBuffer, iteratedConfig};
            for (const auto iteratedEntry : iteratedHashMap)
            {
                const ChainedHashMapRef::ChainedEntryRef iteratedEntryRef{
                    iteratedEntry, iteratedHashMapBuffer, iteratedConfig.fieldKeys, iteratedConfig.fieldValues};
                if (const auto lookedUpEntry = lookedUpHashMap.findEntry(iteratedEntryRef.entryRef); lookedUpEntry != nullptr)
                {
                    const ChainedHashMapRef::ChainedEntryRef lookedUpEntryRef{
                        static_cast<nautilus::val<ChainedHashMapEntry*>>(lookedUpEntry),
                        lookedUpHashMapBuffer,
                        lookedUpConfig.fieldKeys,
                        lookedUpConfig.fieldValues};
                    const PagedVectorRef iteratedPagedVector = loadEntryPagedVector(iteratedEntryRef, iteratedLayout);
                    const PagedVectorRef lookedUpPagedVector = loadEntryPagedVector(lookedUpEntryRef, lookedUpLayout);
                    const auto& leftPagedVector = iterateLeft ? iteratedPagedVector : lookedUpPagedVector;
                    const auto& rightPagedVector = iterateLeft ? lookedUpPagedVector : iteratedPagedVector;
                    auto rightItStart = rightPagedVector.begin();
                    auto rightItEnd = rightPagedVector.end();

                    for (auto leftIt = leftPagedVector.begin(); leftIt != leftPagedVector.end(); ++leftIt)
                    {
                        for (auto rightIt = rightItStart; rightIt != rightItEnd; ++rightIt)
                        {
                            const auto leftRecord = *leftIt;
                            const auto rightRecord = *rightIt;
                            auto joinedRecord
                                = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
                            executeChild(executionCtx, joinedRecord);
                        }
                    }
                }
            }
        }
    }
}

namespace
{
/// Appends copies of the pages of the paged vector in a source map entry to the paged vector of a merged map entry, creating the
/// latter if the entry is new. Pages are copied, not shared, as a page stores the cumulative tuple count of its own paged vector.
void appendPagesToMergedEntryProxy(
    TupleBuffer* mergedHashMapBuffer,
    uint32_t* mergedValueMemArea,
    TupleBuffer* sourceHashMapBuffer,
    uint32_t* sourceValueMemArea,
    AbstractBufferProvider* bufferProvider,
    const bool isNewEntry,
    const uint64_t pageSize,
    const uint64_t tupleSize)
{
    if (isNewEntry)
    {
        auto pagedVectorBuffer = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize());
        if (not pagedVectorBuffer)
        {
            throw BufferAllocationFailure("No unpooled TupleBuffer available for a compacted hash join entry's paged vector!");
        }
        PagedVector::init(pagedVectorBuffer.value(), pageSize, tupleSize);
        *mergedValueMemArea = mergedHashMapBuffer->storeChildBuffer(pagedVectorBuffer.value()).getRawValue();
    }
    auto merged = PagedVector::load(mergedHashMapBuffer->loadChildBuffer(ChildBufferIndex{*mergedValueMemArea}));
    const auto source = PagedVector::load(sourceHashMapBuffer->loadChildBuffer(ChildBufferIndex{*sourceValueMemArea}));
    merged.copyPagesFrom(*bufferProvider, source);
}

}

OwnedNautilusBuffer HJProbePhysicalOperatorBase::mergeHashMaps(
    const BorrowedNautilusBuffer& recordBuffer,
    const nautilus::val<uint64_t>& firstChild,
    const nautilus::val<uint64_t>& numberOfHashMaps,
    const ChainedHashMapConfig& config,
    const std::shared_ptr<PagedVectorTupleLayout>& tupleLayout,
    ExecutionContext& executionCtx) const
{
    OwnedNautilusBuffer mergedHashMap;
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider,
            TupleBuffer* mergedHashMapBuffer IF_PRECONDITION(, const uint64_t entrySize),
            const uint64_t numberOfBuckets IF_PRECONDITION(, const uint64_t pageSize),
            const uint64_t bloomBytes)
        {
            const auto bufferSize = ChainedHashMap::calculateBufferSize(numberOfBuckets, bloomBytes);
            auto buffer = bufferProvider->getUnpooledBuffer(bufferSize);
            if (not buffer)
            {
                throw CannotAllocateBuffer("{}B for a compacted hash join map were requested", bufferSize);
            }
            *mergedHashMapBuffer = buffer.value();
            ChainedHashMap::init(
                *mergedHashMapBuffer IF_PRECONDITION(, entrySize), numberOfBuckets IF_PRECONDITION(, pageSize), bloomBytes);
        },
        executionCtx.pipelineMemoryProvider.bufferProvider,
        mergedHashMap.asArg() IF_PRECONDITION(, nautilus::val<uint64_t>{config.entrySize}),
        nautilus::val<uint64_t>{config.numberOfBuckets} IF_PRECONDITION(, nautilus::val<uint64_t>{config.pageSize}),
        nautilus::val<uint64_t>{config.bloomFilterMemAreaSize()});

    const BorrowedNautilusBuffer mergedHashMapRef = mergedHashMap;
    ChainedHashMapRef mergedMap{mergedHashMapRef, config};
    const nautilus::val<uint64_t> tupleSize{getSizeInBytes(tupleLayout->getSchema())};
    const nautilus::val<uint64_t> pageSize{config.pageSize};
    for (nautilus::val<uint64_t> index = 0; index < numberOfHashMaps; ++index)
    {
        auto sourceHashMapBuffer = recordBuffer.getChild(firstChild + index);
        const BorrowedNautilusBuffer sourceHashMapRef = sourceHashMapBuffer;
        const ChainedHashMapRef sourceMap{sourceHashMapRef, config};
        for (const auto entry : sourceMap)
        {
            const ChainedHashMapRef::ChainedEntryRef sourceEntryRef{entry, sourceHashMapRef, config.fieldKeys, config.fieldValues};
            const auto appendPages = [&](const nautilus::val<AbstractHashMapEntry*>& mergedEntry, const bool isNewEntry)
            {
                const ChainedHashMapRef::ChainedEntryRef mergedEntryRef{
                    mergedEntry, mergedHashMapRef, config.fieldKeys, config.fieldValues};
                nautilus::invoke(
                    appendPagesToMergedEntryProxy,
                    mergedHashMap.asArg(),
                    static_cast<nautilus::val<uint32_t*>>(mergedEntryRef.getValueMemArea()),
                    sourceHashMapBuffer.asArg(),
                    static_cast<nautilus::val<uint32_t*>>(sourceEntryRef.getValueMemArea()),
                    executionCtx.pipelineMemoryProvider.bufferProvider,
                    nautilus::val<bool>{isNewEntry},
                    pageSize,
                    tupleSize);
            };
            mergedMap.insertOrUpdateEntry(
                sourceEntryRef.entryRef,
                [&](const nautilus::val<AbstractHashMapEntry*>& mergedEntry) { appendPages(mergedEntry, false); },
                [&](const nautilus::val<AbstractHashMapEntry*>& mergedEntry) { appendPages(mergedEntry, true); },
                executionCtx.pipelineMemoryProvider.bufferProvider);
        }
    }
    return mergedHashMap;
}

void HJProbePhysicalOperatorBase::performCompaction(
    const BorrowedNautilusBuffer& recordBuffer,
    const nautilus::val<uint64_t>& leftNumberOfHashMaps,
    const nautilus::val<uint64_t>& rightNumberOfHashMaps,
    const nautilus::val<uint64_t>& segmentId,
    ExecutionContext& executionCtx) const
{
    /// A side with no maps was left out by the handler because it needs no compaction; merging nothing still yields a valid map,
    /// which the publish ignores.
    auto mergedLeft
        = mergeHashMaps(recordBuffer, nautilus::val<uint64_t>{0}, leftNumberOfHashMaps, leftHashMapConfig, leftTupleLayout, executionCtx);
    auto mergedRight
        = mergeHashMaps(recordBuffer, leftNumberOfHashMaps, rightNumberOfHashMaps, rightHashMapConfig, rightTupleLayout, executionCtx);
    nautilus::invoke(
        publishCompactedJoinStateProxy,
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        segmentId,
        mergedLeft.asArg(),
        leftNumberOfHashMaps > 0,
        mergedRight.asArg(),
        rightNumberOfHashMaps > 0);
}

}
