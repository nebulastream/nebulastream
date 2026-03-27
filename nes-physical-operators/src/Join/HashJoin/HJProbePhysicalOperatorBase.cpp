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
#include <DataTypes/Schema.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/TimestampRef.hpp>
#include <Join/StreamJoinProbePhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
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
    std::shared_ptr<PagedVectorTupleLayout> leftTupleLayout,
    std::shared_ptr<PagedVectorTupleLayout> rightTupleLayout,
    HashMapOptions leftHashMapOptions,
    HashMapOptions rightHashMapOptions)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), std::move(joinSchema))
    , leftTupleLayout(std::move(leftTupleLayout))
    , rightTupleLayout(std::move(rightTupleLayout))
    , leftHashMapOptions(std::move(leftHashMapOptions))
    , rightHashMapOptions(std::move(rightHashMapOptions))
{
}

ChainedHashMapRef
HJProbePhysicalOperatorBase::makeChainedHashMapRef(const nautilus::val<HashMap*>& hashMapPtr, const HashMapOptions& options)
{
    return ChainedHashMapRef{hashMapPtr, options.fieldKeys, options.fieldValues, options.entriesPerPage, options.entrySize};
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

    const auto leftFields = getOrderedFieldNames(leftTupleLayout->getSchema());
    const auto rightFields = getOrderedFieldNames(rightTupleLayout->getSchema());

    for (nautilus::val<uint64_t> leftHashMapIndex = 0; leftHashMapIndex < leftNumberOfHashMaps; ++leftHashMapIndex)
    {
        const nautilus::val<HashMap*> leftHashMapPtr = leftHashMapRefs[leftHashMapIndex];
        ChainedHashMapRef leftHashMap = makeChainedHashMapRef(leftHashMapPtr, leftHashMapOptions);
        for (nautilus::val<uint64_t> rightHashMapIndex = 0; rightHashMapIndex < rightNumberOfHashMaps; ++rightHashMapIndex)
        {
            const nautilus::val<HashMap*> rightHashMapPtr = rightHashMapRefs[rightHashMapIndex];
            const ChainedHashMapRef rightHashMap = makeChainedHashMapRef(rightHashMapPtr, rightHashMapOptions);
            for (const auto rightEntry : rightHashMap)
            {
                const ChainedHashMapRef::ChainedEntryRef rightEntryRef{
                    rightEntry, rightHashMapPtr, rightHashMapOptions.fieldKeys, rightHashMapOptions.fieldValues};
                auto rightPagedVectorMem = rightEntryRef.getValueMemArea();
                const PagedVectorRef rightPagedVector{BorrowedNautilusBuffer::from(rightPagedVectorMem), rightTupleLayout};
                auto rightItStart = rightPagedVector.begin();
                auto rightItEnd = rightPagedVector.end();

                if (const auto leftEntry = leftHashMap.findEntry(rightEntryRef.entryRef); leftEntry != nullptr)
                {
                    const ChainedHashMapRef::ChainedEntryRef leftEntryRef{
                        static_cast<nautilus::val<ChainedHashMapEntry*>>(leftEntry),
                        leftHashMapPtr,
                        leftHashMapOptions.fieldKeys,
                        leftHashMapOptions.fieldValues};
                    auto leftPagedVectorMem = leftEntryRef.getValueMemArea();
                    const PagedVectorRef leftPagedVector{BorrowedNautilusBuffer::from(leftPagedVectorMem), leftTupleLayout};

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

}
