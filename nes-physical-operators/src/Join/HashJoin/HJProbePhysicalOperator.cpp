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
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/OffsetHashMap/OffsetHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/MemoryProvider/TupleBufferMemoryProvider.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
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
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> leftMemoryProvider,
    std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider> rightMemoryProvider,
    HashMapOptions leftHashMapBasedOptions,
    HashMapOptions rightHashMapBasedOptions,
    bool useSerializableJoinVectors)
    : StreamJoinProbePhysicalOperator(operatorHandlerId, std::move(joinFunction), std::move(windowMetaData), std::move(joinSchema))
    , leftMemoryProvider(std::move(leftMemoryProvider))
    , rightMemoryProvider(std::move(rightMemoryProvider))
    , leftHashMapOptions(std::move(leftHashMapBasedOptions))
    , rightHashMapOptions(std::move(rightHashMapBasedOptions))
    , useSerializableJoinVectors(useSerializableJoinVectors)
{
}

namespace
{
Interface::HashMap* getHashMapPtrProxy(Interface::HashMap** hashMaps, const uint64_t hashMapIndex)
{
    PRECONDITION(hashMaps != nullptr, "HashMaps MUST NOT be null");
    auto* const hashMapPtr = hashMaps[hashMapIndex];
    return hashMapPtr;
}
}

void HJProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    StreamJoinProbePhysicalOperator::open(executionCtx, recordBuffer);

    /// Getting necessary values from the record buffer
    const auto hashJoinWindowRef = static_cast<nautilus::val<EmittedHJWindowTrigger*>>(recordBuffer.getBuffer());
    const auto leftNumberOfHashMaps = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedHjWindowTrigger) { return emittedHjWindowTrigger->leftNumberOfHashMaps; },
        hashJoinWindowRef);
    const auto rightNumberOfHashMaps = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedHjWindowTrigger) { return emittedHjWindowTrigger->rightNumberOfHashMaps; },
        hashJoinWindowRef);
    const auto windowStart = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedJoinWindow) { return emittedJoinWindow->windowInfo.windowStart; }, hashJoinWindowRef);
    const auto windowEnd = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedJoinWindow) { return emittedJoinWindow->windowInfo.windowEnd; }, hashJoinWindowRef);
    const auto leftHashMapRefs = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedJoinWindow) { return emittedJoinWindow->leftHashMaps; }, hashJoinWindowRef);
    const auto rightHashMapRefs = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedJoinWindow) { return emittedJoinWindow->rightHashMaps; }, hashJoinWindowRef);


    if (useSerializableJoinVectors)
    {
        // Offset-based path
        // Convert field offsets for OffsetHashMap
        auto toOffset = [](const auto& fields){
            std::vector<Interface::MemoryProvider::OffsetFieldOffsets> out;
            out.reserve(fields.size());
            for (const auto& f : fields) out.emplace_back(Interface::MemoryProvider::OffsetFieldOffsets{f.fieldIdentifier, f.type, f.fieldOffset});
            return out;
        };
        auto leftKeys = toOffset(leftHashMapOptions.fieldKeys);
        auto leftVals = toOffset(leftHashMapOptions.fieldValues);
        auto rightKeys = toOffset(rightHashMapOptions.fieldKeys);
        auto rightVals = toOffset(rightHashMapOptions.fieldValues);

        for (nautilus::val<uint64_t> lIdx = 0; lIdx < leftNumberOfHashMaps; ++lIdx)
        {
            const auto leftPtr = nautilus::invoke(getHashMapPtrProxy, leftHashMapRefs, lIdx);
            Interface::OffsetHashMapRef leftMap(static_cast<nautilus::val<NES::DataStructures::OffsetHashMapWrapper*>>(static_cast<nautilus::val<void*>>(leftPtr)), leftKeys, leftVals);
            for (nautilus::val<uint64_t> rIdx = 0; rIdx < rightNumberOfHashMaps; ++rIdx)
            {
                const auto rightPtr = nautilus::invoke(getHashMapPtrProxy, rightHashMapRefs, rIdx);
                Interface::OffsetHashMapRef rightMap(static_cast<nautilus::val<NES::DataStructures::OffsetHashMapWrapper*>>(static_cast<nautilus::val<void*>>(rightPtr)), rightKeys, rightVals);
                for (const auto rightEntry : rightMap)
                {
                    const Interface::OffsetHashMapRef::OffsetEntryRef rightRef(
                        rightEntry,
                        static_cast<nautilus::val<NES::DataStructures::OffsetHashMapWrapper*>>(static_cast<nautilus::val<void*>>(rightPtr)),
                        rightKeys,
                        rightVals);
                    if (auto leftEntry = leftMap.findEntry(static_cast<nautilus::val<Interface::AbstractHashMapEntry*>>(static_cast<nautilus::val<void*>>(rightEntry))))
                    {
                        const Interface::OffsetHashMapRef::OffsetEntryRef leftRef(
                            static_cast<nautilus::val<NES::DataStructures::OffsetEntry*>>(static_cast<nautilus::val<void*>>(leftEntry)),
                            static_cast<nautilus::val<NES::DataStructures::OffsetHashMapWrapper*>>(static_cast<nautilus::val<void*>>(leftPtr)),
                            leftKeys,
                            leftVals);
                        auto leftMem = leftRef.getValueMemArea();
                        auto rightMem = rightRef.getValueMemArea();
                        const Interface::PagedVectorRef leftVec(
                            static_cast<nautilus::val<NES::DataStructures::SerializablePagedVector*>>(static_cast<nautilus::val<void*>>(leftMem)),
                            leftMemoryProvider);
                        const Interface::PagedVectorRef rightVec(
                            static_cast<nautilus::val<NES::DataStructures::SerializablePagedVector*>>(static_cast<nautilus::val<void*>>(rightMem)),
                            rightMemoryProvider);
                        const auto leftFields = leftMemoryProvider->getMemoryLayout()->getSchema().getFieldNames();
                        const auto rightFields = rightMemoryProvider->getMemoryLayout()->getSchema().getFieldNames();
                        for (auto li = leftVec.begin(leftFields); li != leftVec.end(leftFields); ++li)
                        {
                            for (auto ri = rightVec.begin(rightFields); ri != rightVec.end(rightFields); ++ri)
                            {
                                auto joinedRecord = createJoinedRecord(*li, *ri, windowStart, windowEnd, leftFields, rightFields);
                                executeChild(executionCtx, joinedRecord);
                            }
                        }
                    }
                }
            }
        }
    }
    else
    {
        /// Chained hash map path (existing)
        for (nautilus::val<uint64_t> leftHashMapIndex = 0; leftHashMapIndex < leftNumberOfHashMaps; ++leftHashMapIndex)
        {
            const auto leftHashMapPtr = nautilus::invoke(getHashMapPtrProxy, leftHashMapRefs, leftHashMapIndex);
            Interface::ChainedHashMapRef leftHashMap{
                leftHashMapPtr,
                leftHashMapOptions.fieldKeys,
                leftHashMapOptions.fieldValues,
                leftHashMapOptions.entriesPerPage,
                leftHashMapOptions.entrySize};
            for (nautilus::val<uint64_t> rightHashMapIndex = 0; rightHashMapIndex < rightNumberOfHashMaps; ++rightHashMapIndex)
            {
                const auto rightHashMapPtr = nautilus::invoke(getHashMapPtrProxy, rightHashMapRefs, rightHashMapIndex);
                const Interface::ChainedHashMapRef rightHashMap{
                    rightHashMapPtr,
                    rightHashMapOptions.fieldKeys,
                    rightHashMapOptions.fieldValues,
                    rightHashMapOptions.entriesPerPage,
                    rightHashMapOptions.entrySize};
                for (const auto rightEntry : rightHashMap)
                {
                    const Interface::ChainedHashMapRef::ChainedEntryRef rightEntryRef{
                        rightEntry, rightHashMapPtr, rightHashMapOptions.fieldKeys, rightHashMapOptions.fieldValues};

                    if (auto leftEntry = leftHashMap.findEntry(rightEntryRef.entryRef))
                    {
                        const Interface::ChainedHashMapRef::ChainedEntryRef leftEntryRef{
                            leftEntry, leftHashMapPtr, leftHashMapOptions.fieldKeys, leftHashMapOptions.fieldValues};
                        auto leftPagedVectorMem = leftEntryRef.getValueMemArea();
                        auto rightPagedVectorMem = rightEntryRef.getValueMemArea();
                        const Interface::PagedVectorRef leftPagedVector = useSerializableJoinVectors
                            ? Interface::PagedVectorRef(static_cast<nautilus::val<NES::DataStructures::SerializablePagedVector*>>(static_cast<nautilus::val<void*>>(leftPagedVectorMem)), leftMemoryProvider)
                            : Interface::PagedVectorRef(leftPagedVectorMem, leftMemoryProvider);
                        const Interface::PagedVectorRef rightPagedVector = useSerializableJoinVectors
                            ? Interface::PagedVectorRef(static_cast<nautilus::val<NES::DataStructures::SerializablePagedVector*>>(static_cast<nautilus::val<void*>>(rightPagedVectorMem)), rightMemoryProvider)
                            : Interface::PagedVectorRef(rightPagedVectorMem, rightMemoryProvider);
                        const auto leftFields = leftMemoryProvider->getMemoryLayout()->getSchema().getFieldNames();
                        const auto rightFields = rightMemoryProvider->getMemoryLayout()->getSchema().getFieldNames();
                        for (auto leftIt = leftPagedVector.begin(leftFields); leftIt != leftPagedVector.end(leftFields); ++leftIt)
                        {
                            for (auto rightIt = rightPagedVector.begin(rightFields); rightIt != rightPagedVector.end(rightFields); ++rightIt)
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
}
