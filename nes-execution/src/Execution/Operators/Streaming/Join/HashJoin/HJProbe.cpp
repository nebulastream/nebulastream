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


#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Join/HashJoin/HJProbe.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Util.hpp>


namespace NES::Runtime::Execution::Operators
{

HJProbe::HJProbe(
    const uint64_t operatorHandlerIndex,
    const std::shared_ptr<Functions::Function>& joinFunction,
    const WindowMetaData& windowMetaData,
    const JoinSchema& joinSchema,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& leftMemoryProvider,
    const std::shared_ptr<Interface::MemoryProvider::TupleBufferMemoryProvider>& rightMemoryProvider,
    HashMapOptions&& leftHashMapBasedOptions,
    HashMapOptions&& rightHashMapBasedOptions)
    : StreamJoinProbe(operatorHandlerIndex, joinFunction, windowMetaData, joinSchema)
    , leftMemoryProvider(leftMemoryProvider)
    , rightMemoryProvider(rightMemoryProvider)
    , leftHashMapBasedOptions(std::move(leftHashMapBasedOptions))
    , rightHashMapBasedOptions(std::move(rightHashMapBasedOptions))
{
}

namespace
{
Interface::HashMap* getHashMapPtrProxy(Interface::HashMap** hashMaps, const uint64_t hashMapIndex)
{
    PRECONDITION(hashMaps != nullptr, "HashMaps MUST NOT be null");
    const auto hashMapPtr = hashMaps[hashMapIndex];
    return hashMapPtr;
}
}

void HJProbe::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    StreamJoinProbe::open(executionCtx, recordBuffer);

    /// Getting necessary values from the record buffer
    const auto hashJoinWindowRef = static_cast<nautilus::val<EmittedHJWindowTrigger*>>(recordBuffer.getBuffer());
    const auto leftNumberOfHashMaps = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedHjWindowTrigger) { return emittedHjWindowTrigger->leftNumberOfHashMaps; },
        hashJoinWindowRef);
    const auto rightNumberOfHashMaps = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedHjWindowTrigger) { return emittedHjWindowTrigger->rightNumberOfHashMaps; },
        hashJoinWindowRef);
    const auto windowStart = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedAggregationWindow) { return emittedAggregationWindow->windowInfo.windowStart; },
        hashJoinWindowRef);
    const auto windowEnd = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedAggregationWindow) { return emittedAggregationWindow->windowInfo.windowEnd; },
        hashJoinWindowRef);
    const auto leftHashMapRefs = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedAggregationWindow) { return emittedAggregationWindow->leftHashMaps; }, hashJoinWindowRef);
    const auto rightHashMapRefs = nautilus::invoke(
        +[](const EmittedHJWindowTrigger* emittedAggregationWindow) { return emittedAggregationWindow->rightHashMaps; }, hashJoinWindowRef);


    /// We iterate over all "left" hash maps and check if we find a tuple with the same key in the "right" hash maps
    for (nautilus::val<uint64_t> leftHashMapIndex = 0; leftHashMapIndex < leftNumberOfHashMaps; ++leftHashMapIndex)
    {
        const auto leftHashMapPtr = nautilus::invoke(getHashMapPtrProxy, leftHashMapRefs, leftHashMapIndex);
        Interface::ChainedHashMapRef leftHashMap(
            leftHashMapPtr,
            leftHashMapBasedOptions.fieldKeys,
            leftHashMapBasedOptions.fieldValues,
            leftHashMapBasedOptions.entriesPerPage,
            leftHashMapBasedOptions.entrySize);
        for (nautilus::val<uint64_t> rightHashMapIndex = 0; rightHashMapIndex < rightNumberOfHashMaps; ++rightHashMapIndex)
        {
            const auto rightHashMapPtr = nautilus::invoke(getHashMapPtrProxy, rightHashMapRefs, rightHashMapIndex);
            const Interface::ChainedHashMapRef rightHashMap(
                rightHashMapPtr,
                rightHashMapBasedOptions.fieldKeys,
                rightHashMapBasedOptions.fieldValues,
                rightHashMapBasedOptions.entriesPerPage,
                rightHashMapBasedOptions.entrySize);
            for (const auto rightEntry : rightHashMap)
            {
                const Interface::ChainedHashMapRef::ChainedEntryRef rightEntryRef(
                    rightEntry, rightHashMapBasedOptions.fieldKeys, rightHashMapBasedOptions.fieldValues);

                /// We use here findEntry as the other methods would insert a new entry, which is unnecessary
                if (auto leftEntry = leftHashMap.findEntry(rightEntryRef.entryRef))
                {
                    /// At this moment, we can be sure that both paged vector contain only records that satisfy the join condition
                    const Interface::ChainedHashMapRef::ChainedEntryRef leftEntryRef(
                        leftEntry, leftHashMapBasedOptions.fieldKeys, leftHashMapBasedOptions.fieldValues);
                    auto leftPagedVectorMem = leftEntryRef.getValueMemArea();
                    auto rightPagedVectorMem = rightEntryRef.getValueMemArea();
                    Interface::PagedVectorRef leftPagedVector(leftPagedVectorMem, leftMemoryProvider);
                    Interface::PagedVectorRef rightPagedVector(rightPagedVectorMem, rightMemoryProvider);
                    const auto leftFields = leftMemoryProvider->getMemoryLayout()->getSchema()->getFieldNames();
                    const auto rightFields = rightMemoryProvider->getMemoryLayout()->getSchema()->getFieldNames();
                    for (auto leftIt = leftPagedVector.begin(leftFields); leftIt != leftPagedVector.end(leftFields); ++leftIt)
                    {
                        for (auto rightIt = rightPagedVector.begin(rightFields); rightIt != rightPagedVector.end(rightFields); ++rightIt)
                        {
                            const auto leftRecord = *leftIt;
                            const auto rightRecord = *rightIt;
                            auto joinedRecord
                                = createJoinedRecord(leftRecord, rightRecord, windowStart, windowEnd, leftFields, rightFields);
                            child->execute(executionCtx, joinedRecord);
                        }
                    }
                }
            }
        }
    }
}
}
