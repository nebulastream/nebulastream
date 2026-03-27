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
#include <utility>
#include <Functions/PhysicalFunction.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <Windowing/WindowMetaData.hpp>
#include <ExecutionContext.hpp>
#include <HashMapOptions.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

HJProbePhysicalOperatorBase::HJProbePhysicalOperatorBase(
    const OperatorHandlerId operatorHandlerId,
    PhysicalFunction joinFunction,
    WindowMetaData windowMetaData,
    JoinSchema joinSchema,
    std::shared_ptr<TupleBufferRef> leftBufferRef,
    std::shared_ptr<TupleBufferRef> rightBufferRef,
    HashMapOptions leftHashMapOptions,
    HashMapOptions rightHashMapOptions)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), std::move(joinSchema))
    , leftBufferRef(std::move(leftBufferRef))
    , rightBufferRef(std::move(rightBufferRef))
    , leftHashMapOptions(std::move(leftHashMapOptions))
    , rightHashMapOptions(std::move(rightHashMapOptions))
{
}

/// NOLINTNEXTLINE(readability-function-cognitive-complexity) inner join's N x N hash-map iteration is inherently deeply nested
void HJProbePhysicalOperatorBase::performMatchPairsProbe(
    nautilus::val<HashMap**> leftHashMapRefs,
    nautilus::val<uint64_t> leftNumberOfHashMaps,
    nautilus::val<HashMap**> rightHashMapRefs,
    nautilus::val<uint64_t> rightNumberOfHashMaps,
    ExecutionContext& executionCtx,
    const nautilus::val<Timestamp>& windowStart,
    const nautilus::val<Timestamp>& windowEnd) const
{
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
