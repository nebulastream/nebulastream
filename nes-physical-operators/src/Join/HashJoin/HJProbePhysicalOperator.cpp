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
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <function.hpp>
#include <val.hpp>
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
    HashMapOptions rightHashMapBasedOptions)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), std::move(joinSchema))
    , leftBufferRef(std::move(leftBufferRef))
    , rightBufferRef(std::move(rightBufferRef))
    , leftHashMapOptions(std::move(leftHashMapBasedOptions))
    , rightHashMapOptions(std::move(rightHashMapBasedOptions))
{
}

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
    if (leftNumberOfHashMaps == 0 and rightNumberOfHashMaps == 0)
    {
        return;
    }

    /// Getting necessary values from the record buffer
    const auto windowInfoRef = getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};
    auto leftHashMapRefs = readValueFromMemRef<HashMap**>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::leftHashMaps));
    auto rightHashMapRefs = readValueFromMemRef<HashMap**>(getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::rightHashMaps));


    /// We iterate over all "left" hash maps and check if we find a tuple with the same key in the "right" hash maps
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
                const auto rightFields = rightBufferRef->getMemoryLayout()->getSchema().getFieldNames();
                auto rightItStart = rightPagedVector.begin(rightFields);
                auto rightItEnd = rightPagedVector.end(rightFields);

                /// We use here findEntry as the other methods would insert a new entry, which is unnecessary
                if (auto leftEntry = leftHashMap.findEntry(rightEntryRef.entryRef))
                {
                    /// At this moment, we can be sure that both paged vector contain only records that satisfy the join condition
                    const ChainedHashMapRef::ChainedEntryRef leftEntryRef{
                        leftEntry, leftHashMapPtr, leftHashMapOptions.fieldKeys, leftHashMapOptions.fieldValues};
                    auto leftPagedVectorMem = leftEntryRef.getValueMemArea();
                    const PagedVectorRef leftPagedVector{leftPagedVectorMem, leftBufferRef};
                    const auto leftFields = leftBufferRef->getMemoryLayout()->getSchema().getFieldNames();

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
