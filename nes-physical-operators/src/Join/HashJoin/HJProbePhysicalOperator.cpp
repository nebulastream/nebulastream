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
#include <DataTypes/DataTypesUtil.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/BufferRef/TupleBufferRef.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
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
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    HashMapOptions leftHashMapBasedOptions,
    HashMapOptions rightHashMapBasedOptions)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), std::move(joinSchema))
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
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
    if (leftNumberOfHashMaps == 0 or rightNumberOfHashMaps == 0)
    {
        return;
    }

    /// Getting necessary values from the record buffer
    const auto windowInfoRef = getMemberRef(hashJoinWindowRef, &EmittedHJWindowTrigger::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    /// We iterate over all "left" hash maps and check if we find a tuple with the same key in the "right" hash maps
    for (nautilus::val<uint64_t> leftHashMapIndex = 0; leftHashMapIndex < leftNumberOfHashMaps; ++leftHashMapIndex)
    {
        /// Pin the current left hashmap buffer
        OwnedNautilusBuffer leftHashMapBufferRef;
        nautilus::invoke(
            +[](TupleBuffer* parent, uint32_t leftHashMapIndex, TupleBuffer* leftHashMapBuffer)
            {
                INVARIANT(parent != nullptr, "Parent Tuplebuffer MUST NOT be null at this point");
                /// Left buffers start at index 0
                VariableSizedAccess::Index bufferIndex{leftHashMapIndex};
                *leftHashMapBuffer = parent->loadChildBuffer(bufferIndex);
            },
            recordBuffer.getReference(),
            leftHashMapIndex,
            leftHashMapBufferRef.asArg());

        ChainedHashMapRef leftHashMap{
            leftHashMapBufferRef.asArg(),
            leftHashMapOptions.fieldKeys,
            leftHashMapOptions.fieldValues,
            leftHashMapOptions.entriesPerPage,
            leftHashMapOptions.entrySize};


        for (nautilus::val<uint64_t> rightHashMapIndex = 0; rightHashMapIndex < rightNumberOfHashMaps; ++rightHashMapIndex)
        {
            /// Pin the current right hashmap buffer
            /// Right buffers start after all left buffers
            OwnedNautilusBuffer rightHashMapBufferRef;
            nautilus::invoke(
                +[](TupleBuffer* parent, uint32_t leftNumberOfHashMaps, uint32_t rightHashMapIndex, TupleBuffer* rightHashMapBuffer)
                {
                    INVARIANT(parent != nullptr, "Parent Tuplebuffer MUST NOT be null at this point");
                    /// Right buffers start after all left buffers
                    VariableSizedAccess::Index bufferIndex{leftNumberOfHashMaps + rightHashMapIndex};
                    *rightHashMapBuffer = parent->loadChildBuffer(bufferIndex);
                },
                recordBuffer.getReference(),
                leftNumberOfHashMaps,
                rightHashMapIndex,
                rightHashMapBufferRef.asArg());

            const ChainedHashMapRef rightHashMap{
                rightHashMapBufferRef.asArg(),
                rightHashMapOptions.fieldKeys,
                rightHashMapOptions.fieldValues,
                rightHashMapOptions.entriesPerPage,
                rightHashMapOptions.entrySize};


            for (const auto rightEntry : rightHashMap)
            {
                const ChainedHashMapRef::ChainedEntryRef rightEntryRef{
                    rightEntry, rightHashMapBufferRef.asArg(), rightHashMapOptions.fieldKeys, rightHashMapOptions.fieldValues};
                auto rightValueMem = rightEntryRef.getValueMemArea();
                OwnedNautilusBuffer rightPagedVecBuffer;
                nautilus::invoke(
                    +[](TupleBuffer* hashMapBuf, TupleBuffer* out, uint32_t* indexPtr)
                    { *out = hashMapBuf->loadChildBuffer(VariableSizedAccess::Index{*indexPtr}); },
                    rightHashMapBufferRef.asArg(),
                    rightPagedVecBuffer.asArg(),
                    static_cast<nautilus::val<uint32_t*>>(rightValueMem));
                const PagedVectorRef rightPagedVector{BorrowedNautilusBuffer::from(rightPagedVecBuffer.asArg()), rightTupleLayout};
                const auto rightFields = rightTupleLayout->getAllFieldNames();
                auto rightItStart = rightPagedVector.begin();
                auto rightItEnd = rightPagedVector.end();

                /// We use here findEntry as the other methods would insert a new entry, which is unnecessary
                if (auto leftEntry = leftHashMap.findEntry(rightEntryRef.entryRef))
                {
                    /// At this moment, we can be sure that both paged vector contain only records that satisfy the join condition
                    const ChainedHashMapRef::ChainedEntryRef leftEntryRef{
                        leftEntry, leftHashMapBufferRef.asArg(), leftHashMapOptions.fieldKeys, leftHashMapOptions.fieldValues};
                    auto leftValueMem = leftEntryRef.getValueMemArea();
                    OwnedNautilusBuffer leftPagedVecBuffer;
                    nautilus::invoke(
                        +[](TupleBuffer* hashMapBuf, TupleBuffer* out, uint32_t* indexPtr)
                        { *out = hashMapBuf->loadChildBuffer(VariableSizedAccess::Index{*indexPtr}); },
                        leftHashMapBufferRef.asArg(),
                        leftPagedVecBuffer.asArg(),
                        static_cast<nautilus::val<uint32_t*>>(leftValueMem));
                    const PagedVectorRef leftPagedVector{BorrowedNautilusBuffer::from(leftPagedVecBuffer.asArg()), leftTupleLayout};
                    const auto leftFields = leftTupleLayout->getAllFieldNames();

                    for (const auto& leftRecord : leftPagedVector)
                    {
                        for (auto rightIt = rightItStart; rightIt != rightItEnd; ++rightIt)
                        {
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
