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
#include <Join/HashJoin/HJProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

HJProbePhysicalOperator::HJProbePhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    JoinSchema joinSchema,
    std::shared_ptr<TupleBufferRef> leftBufferRef,
    std::shared_ptr<TupleBufferRef> rightBufferRef,
    HashMapOptions leftHashMapBasedOptions,
    HashMapOptions rightHashMapBasedOptions,
    JoinLogicalOperator::JoinType joinType)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), std::move(joinSchema))
    , leftBufferRef(std::move(leftBufferRef))
    , rightBufferRef(std::move(rightBufferRef))
    , leftHashMapOptions(std::move(leftHashMapBasedOptions))
    , rightHashMapOptions(std::move(rightHashMapBasedOptions))
    , joinType(joinType)
{
}

void HJProbePhysicalOperator::performNullFillProbe(
    nautilus::val<HashMap**> outerHashMapRefs,
    nautilus::val<uint64_t> outerNumberOfHashMaps,
    nautilus::val<HashMap**> innerHashMapRefs,
    nautilus::val<uint64_t> innerNumberOfHashMaps,
    const HashMapOptions& outerHashMapOptions,
    const HashMapOptions& innerHashMapOptions,
    const std::shared_ptr<TupleBufferRef>& outerBufferRef,
    const Schema& nullSideSchema,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
    const auto outerFields = outerBufferRef->getAllFieldNames();

    /// Iterate all outer (preserved-side) hash maps
    for (nautilus::val<uint64_t> outerIdx = 0; outerIdx < outerNumberOfHashMaps; ++outerIdx)
    {
        const nautilus::val<HashMap*> outerHashMapPtr = outerHashMapRefs[outerIdx];
        const ChainedHashMapRef outerHashMap{
            outerHashMapPtr,
            outerHashMapOptions.fieldKeys,
            outerHashMapOptions.fieldValues,
            outerHashMapOptions.entriesPerPage,
            outerHashMapOptions.entrySize};

        for (const auto outerEntry : outerHashMap)
        {
            const ChainedHashMapRef::ChainedEntryRef outerEntryRef{
                outerEntry, outerHashMapPtr, outerHashMapOptions.fieldKeys, outerHashMapOptions.fieldValues};
            auto outerPagedVectorMem = outerEntryRef.getValueMemArea();
            const PagedVectorRef outerPagedVector{outerPagedVectorMem, outerBufferRef};

            /// Check all inner hash maps for a matching key
            nautilus::val<bool> matched(false);
            for (nautilus::val<uint64_t> innerIdx = 0; innerIdx < innerNumberOfHashMaps; ++innerIdx)
            {
                const nautilus::val<HashMap*> innerHashMapPtr = innerHashMapRefs[innerIdx];
                ChainedHashMapRef innerHashMap{
                    innerHashMapPtr,
                    innerHashMapOptions.fieldKeys,
                    innerHashMapOptions.fieldValues,
                    innerHashMapOptions.entriesPerPage,
                    innerHashMapOptions.entrySize};

                if (innerHashMap.findEntry(outerEntryRef.entryRef))
                {
                    matched = nautilus::val<bool>(true);
                    break; /// Only need to know a match exists, no need to check remaining hash maps
                }
            }
            if (!matched)
            {
                /// Emit unmatched outer tuples with NULL-filled inner fields
                for (auto outerIt = outerPagedVector.begin(outerFields); outerIt != outerPagedVector.end(outerFields); ++outerIt)
                {
                    const auto outerRecord = *outerIt;
                    auto nullRecord = createNullFilledJoinedRecord(outerRecord, windowStart, windowEnd, outerFields, nullSideSchema);
                    executeChild(executionCtx, nullRecord);
                }
            }
        }
    }
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) inner join's N x N hash-map iteration is inherently deeply nested
void HJProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    StreamJoinProbePhysicalOperator::open(executionCtx, recordBuffer);

    /// Getting number of hash maps and return if there are no hashmaps
    const auto hashJoinWindowRef = static_cast<nautilus::val<EmittedHJWindowTrigger*>>(recordBuffer.getMemArea());
    const auto leftNumberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::leftNumberOfHashMaps));
    const auto rightNumberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::rightNumberOfHashMaps));

    /// Getting necessary values from the record buffer
    const auto windowInfoRef = getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};
    auto leftHashMapRefs = readValueFromMemRef<HashMap**>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::leftHashMaps));
    auto rightHashMapRefs = readValueFromMemRef<HashMap**>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::rightHashMaps));

    /// Read the probe task type from the buffer to determine what work this task should perform
    const auto probeTaskType = readValueFromMemRef<uint64_t>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::probeTaskType));

    if (isOuterJoin(joinType))
    {
        /// Null-fill tasks: iterate one side's keys and emit NULL-filled records for unmatched keys.
        /// Each null-fill task sees one slice's maps on the preserved side and ALL maps on the opposite side.
        if (probeTaskType == static_cast<uint64_t>(ProbeTaskType::LEFT_NULL_FILL))
        {
            if (leftNumberOfHashMaps > 0)
            {
                performNullFillProbe(
                    leftHashMapRefs,
                    leftNumberOfHashMaps,
                    rightHashMapRefs,
                    rightNumberOfHashMaps,
                    leftHashMapOptions,
                    rightHashMapOptions,
                    leftBufferRef,
                    joinSchema.rightSchema,
                    executionCtx,
                    windowStart,
                    windowEnd);
            }
            return;
        }
        if (probeTaskType == static_cast<uint64_t>(ProbeTaskType::RIGHT_NULL_FILL))
        {
            if (rightNumberOfHashMaps > 0)
            {
                performNullFillProbe(
                    rightHashMapRefs,
                    rightNumberOfHashMaps,
                    leftHashMapRefs,
                    leftNumberOfHashMaps,
                    rightHashMapOptions,
                    leftHashMapOptions,
                    rightBufferRef,
                    joinSchema.leftSchema,
                    executionCtx,
                    windowStart,
                    windowEnd);
            }
            return;
        }
        /// Fall through to MATCH_PAIRS: use inner-join-style matching below
    }

    /// MATCH_PAIRS (for both inner and outer joins) and INNER_JOIN / CARTESIAN_PRODUCT:
    /// emit only matched pairs, no null-fill
    if (leftNumberOfHashMaps == 0 or rightNumberOfHashMaps == 0)
    {
        return;
    }

    for (nautilus::val<uint64_t> leftHashMapIndex = 0; leftHashMapIndex < leftNumberOfHashMaps; ++leftHashMapIndex)
    {
        const nautilus::val<HashMap*> leftHashMapPtr = leftHashMapRefs[leftHashMapIndex];
        ChainedHashMapRef leftHashMap{
            leftHashMapPtr,
            leftHashMapOptions.fieldKeys,
            leftHashMapOptions.fieldValues,
            leftHashMapOptions.entriesPerPage,
            leftHashMapOptions.entrySize};
        for (nautilus::val<uint64_t> rightHashMapIndex = 0; rightHashMapIndex < rightNumberOfHashMaps; ++rightHashMapIndex)
        {
            const nautilus::val<HashMap*> rightHashMapPtr = rightHashMapRefs[rightHashMapIndex];
            const ChainedHashMapRef rightHashMap{
                rightHashMapPtr,
                rightHashMapOptions.fieldKeys,
                rightHashMapOptions.fieldValues,
                rightHashMapOptions.entriesPerPage,
                rightHashMapOptions.entrySize};
            for (const auto rightEntry : rightHashMap)
            {
                const ChainedHashMapRef::ChainedEntryRef rightEntryRef{
                    rightEntry, rightHashMapPtr, rightHashMapOptions.fieldKeys, rightHashMapOptions.fieldValues};
                auto rightPagedVectorMem = rightEntryRef.getValueMemArea();
                const PagedVectorRef rightPagedVector{rightPagedVectorMem, rightBufferRef};
                const auto rightFields = rightBufferRef->getAllFieldNames();
                auto rightItStart = rightPagedVector.begin(rightFields);
                auto rightItEnd = rightPagedVector.end(rightFields);

                if (auto leftEntry = leftHashMap.findEntry(rightEntryRef.entryRef))
                {
                    const ChainedHashMapRef::ChainedEntryRef leftEntryRef{
                        leftEntry, leftHashMapPtr, leftHashMapOptions.fieldKeys, leftHashMapOptions.fieldValues};
                    auto leftPagedVectorMem = leftEntryRef.getValueMemArea();
                    const PagedVectorRef leftPagedVector{leftPagedVectorMem, leftBufferRef};
                    const auto leftFields = leftBufferRef->getAllFieldNames();

                    for (auto leftIt = leftPagedVector.begin(leftFields); leftIt != leftPagedVector.end(leftFields); ++leftIt)
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
}
