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
#include <Interface/HashMap/ChainedHashMap/EntryRecordsRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
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
    ChainedHashMapConfig rightHashMapConfig,
    std::optional<HJResidual> residual)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), std::move(joinSchema))
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
    , leftHashMapConfig(std::move(leftHashMapConfig))
    , rightHashMapConfig(std::move(rightHashMapConfig))
    , residual(std::move(residual))
{
}

nautilus::val<bool> HJProbePhysicalOperatorBase::residualHolds(
    const Record& leftRecord,
    const Record& rightRecord,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    if (not residual.has_value())
    {
        return {true};
    }
    const auto residualRecord
        = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, residual->leftFieldNames, residual->rightFieldNames);
    /// Same truth test as the NLJ applies to its join function, so a NULL residual rejects the pair in both.
    nautilus::val<bool> holds{false};
    if (residual->function.execute(residualRecord, executionCtx.pipelineMemoryProvider.arena))
    {
        holds = true;
    }
    return holds;
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
                    const EntryRecordsRef iteratedRecords{
                        iteratedHashMapBuffer,
                        iteratedEntryRef.getValueMemArea(),
                        iteratedLayout,
                        iteratedConfig.inlineRecords};
                    const EntryRecordsRef lookedUpRecords{
                        lookedUpHashMapBuffer,
                        lookedUpEntryRef.getValueMemArea(),
                        lookedUpLayout,
                        lookedUpConfig.inlineRecords};
                    const auto& leftRecords = iterateLeft ? iteratedRecords : lookedUpRecords;
                    const auto& rightRecords = iterateLeft ? lookedUpRecords : iteratedRecords;
                    auto rightItStart = rightRecords.begin();
                    auto rightItEnd = rightRecords.end();
                    const auto leftItEnd = leftRecords.end();

                    for (auto leftIt = leftRecords.begin(); leftIt != leftItEnd; ++leftIt)
                    {
                        for (auto rightIt = rightItStart; rightIt != rightItEnd; ++rightIt)
                        {
                            const auto leftRecord = *leftIt;
                            const auto rightRecord = *rightIt;
                            if (residualHolds(leftRecord, rightRecord, executionCtx, windowStart, windowEnd))
                            {
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
            const uint64_t numberOfBuckets,
            const uint64_t bloomBytes)
        {
            *mergedHashMapBuffer = bufferProvider->getBufferBlocking();
            ChainedHashMap::init(
                *mergedHashMapBuffer IF_PRECONDITION(, entrySize), numberOfBuckets, bloomBytes);
        },
        executionCtx.pipelineMemoryProvider.bufferProvider,
        mergedHashMap.asArg() IF_PRECONDITION(, nautilus::val<uint64_t>{config.entrySize}),
        nautilus::val<uint64_t>{config.numberOfBuckets},
        nautilus::val<uint64_t>{config.bloomFilterMemAreaSize()});

    const BorrowedNautilusBuffer mergedHashMapRef = mergedHashMap;
    ChainedHashMapRef mergedMap{mergedHashMapRef, config};
    for (nautilus::val<uint64_t> index = 0; index < numberOfHashMaps; ++index)
    {
        auto sourceHashMapBuffer = recordBuffer.getChild(firstChild + index);
        const BorrowedNautilusBuffer sourceHashMapRef = sourceHashMapBuffer;
        const ChainedHashMapRef sourceMap{sourceHashMapRef, config};
        for (const auto entry : sourceMap)
        {
            const ChainedHashMapRef::ChainedEntryRef sourceEntryRef{entry, sourceHashMapRef, config.fieldKeys, config.fieldValues};
            /// The records of a key are appended to the merged entry of the key, which is prepared like the build prepares a new entry.
            const auto mergedEntry = mergedMap.findOrCreateEntry(
                sourceEntryRef.getKey(),
                [&](const nautilus::val<AbstractHashMapEntry*>& newEntry)
                {
                    const ChainedHashMapRef::ChainedEntryRef newEntryRef{newEntry, mergedHashMapRef, config.fieldKeys, config.fieldValues};
                    EntryRecordsRef::initialize(
                        mergedHashMapRef,
                        newEntryRef.getValueMemArea(),
                        tupleLayout,
                        config.inlineRecords,
                        executionCtx.pipelineMemoryProvider.bufferProvider);
                },
                executionCtx.pipelineMemoryProvider.bufferProvider);
            const ChainedHashMapRef::ChainedEntryRef mergedEntryRef{mergedEntry, mergedHashMapRef, config.fieldKeys, config.fieldValues};
            EntryRecordsRef mergedRecords{mergedHashMapRef, mergedEntryRef.getValueMemArea(), tupleLayout, config.inlineRecords};
            const EntryRecordsRef sourceRecords{
                sourceHashMapRef, sourceEntryRef.getValueMemArea(), tupleLayout, config.inlineRecords};
            const auto sourceEnd = sourceRecords.end();
            for (auto sourceIt = sourceRecords.begin(); sourceIt != sourceEnd; ++sourceIt)
            {
                mergedRecords.pushBack(*sourceIt, executionCtx.pipelineMemoryProvider.bufferProvider);
            }
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
