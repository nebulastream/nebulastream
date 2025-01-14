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

#include <cstdint>
#include <memory>
#include <vector>
#include <Execution/Operators/ExecutableOperator.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationProbe.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationValue.hpp>
#include <Execution/Operators/Streaming/Aggregation/WindowAggregationOperator.hpp>
#include <Execution/Operators/Streaming/WindowOperatorProbe.hpp>
#include <Nautilus/Interface/Hash/HashFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedEntryMemoryProvider.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMapInterface.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::Operators
{

Interface::HashMapInterface* getHashMapPtrProxy(const EmittedAggregationWindow* emittedAggregationWindow, const uint64_t curHashMapVal)
{
    PRECONDITION(emittedAggregationWindow != nullptr, "EmittedAggregationWindow must not be nullptr");
    PRECONDITION(curHashMapVal < emittedAggregationWindow->numberOfHashMaps, "curHashMapVal must be smaller than the number of hash maps");
    return emittedAggregationWindow->hashMaps[curHashMapVal];
}

void AggregationProbe::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    Operator::open(executionCtx, recordBuffer);

    /// Getting necessary values from the record buffer
    const auto aggregationWindowRef = static_cast<nautilus::val<EmittedAggregationWindow*>>(recordBuffer.getBuffer());
    const auto windowStart = invoke(
        +[](const EmittedAggregationWindow* emittedAggregationWindow) { return emittedAggregationWindow->windowInfo.windowStart; },
        aggregationWindowRef);
    const auto windowEnd = invoke(
        +[](const EmittedAggregationWindow* emittedAggregationWindow) { return emittedAggregationWindow->windowInfo.windowEnd; },
        aggregationWindowRef);
    const auto numberOfHashMaps = invoke(
        +[](const EmittedAggregationWindow* emittedAggregationWindow) { return emittedAggregationWindow->numberOfHashMaps; },
        aggregationWindowRef);
    auto finalHashMapPtr = invoke(
        +[](const EmittedAggregationWindow* emittedAggregationWindow) { return emittedAggregationWindow->finalHashMap.get(); },
        aggregationWindowRef);


    /// Combining all keys from all hash maps in the final hash map, and then iterating over the final hash map once to lower the aggregation states
    Interface::ChainedHashMapRef finalHashMap(finalHashMapPtr, fieldKeys, fieldValues, entriesPerPage, entrySize);
    for (nautilus::val<uint64_t> curHashMap = 0; curHashMap < numberOfHashMaps; ++curHashMap)
    {
        const auto hashMapPtr = nautilus::invoke(getHashMapPtrProxy, aggregationWindowRef, curHashMap);
        Interface::ChainedHashMapRef const currentMap(hashMapPtr, fieldKeys, fieldValues, entriesPerPage, entrySize);
        for (const auto entry : currentMap)
        {
            const Interface::ChainedEntryRef entryRef(entry, fieldKeys, fieldValues);
            const auto tmpRecordKey = entryRef.getKey();
            finalHashMap.insertOrUpdateEntry(
                entryRef.entryRef,
                [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entryOnUpdate)
                {
                    ((void)entryOnUpdate);
                    const Interface::ChainedEntryRef entryRefOnInsert(entryOnUpdate, fieldKeys, fieldValues);
                    auto finalState = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefState = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
                    {
                        aggFunction->combine(finalState, entryRefState, executionCtx.bufferProvider);
                        finalState = finalState + aggFunction->getStateSizeInBytes();
                        entryRefState = entryRefState + aggFunction->getStateSizeInBytes();
                    }
                },
                [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entryOnInsert)
                {
                    ((void)entryOnInsert);
                    /// If the entry for the provided key has not been seen by this hash map / worker thread, we need
                    /// to create a new one and initialize the aggregation states. After that, we can combine the aggregation states.
                    const Interface::ChainedEntryRef entryRefOnInsert(entryOnInsert, fieldKeys, fieldValues);
                    auto finalState = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefState = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
                    {
                        aggFunction->reset(finalState, executionCtx.bufferProvider);
                        aggFunction->combine(finalState, entryRefState, executionCtx.bufferProvider);
                        finalState = finalState + aggFunction->getStateSizeInBytes();
                        entryRefState = entryRefState + aggFunction->getStateSizeInBytes();
                    }
                },
                executionCtx.bufferProvider);
        }
    }

    /// Lowering, each aggregation state in the final hash map and passing the record to the child
    for (const auto entry : finalHashMap)
    {
        const Interface::ChainedEntryRef entryRef(entry, fieldKeys, fieldValues);
        const auto recordKey = entryRef.getKey();
        Record outputRecord;
        auto state = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRef.getValueMemArea());
        for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
        {
            outputRecord.combineRecords(aggFunction->lower(state, executionCtx.bufferProvider));
            state = state + aggFunction->getStateSizeInBytes();
        }

        /// Adding the window start and end to the output record and then passing the record to the child
        outputRecord.combineRecords(recordKey);
        outputRecord.write(windowMetaData.windowStartFieldName, windowStart.convertToValue());
        outputRecord.write(windowMetaData.windowEndFieldName, windowEnd.convertToValue());
        child->execute(executionCtx, outputRecord);
    }
}

AggregationProbe::AggregationProbe(
    std::vector<std::unique_ptr<Aggregation::AggregationFunction>> aggregationFunctions,
    std::unique_ptr<Interface::HashFunction> hashFunction,
    std::vector<Interface::MemoryProvider::FieldOffsets> fieldKeys,
    std::vector<Interface::MemoryProvider::FieldOffsets> fieldValues,
    const uint64_t entriesPerPage,
    const uint64_t entrySize,
    const uint64_t operatorHandlerIndex,
    WindowMetaData windowMetaData)
    : WindowAggregationOperator(
          std::move(aggregationFunctions), std::move(hashFunction), std::move(fieldKeys), std::move(fieldValues), entriesPerPage, entrySize)
    , WindowOperatorProbe(operatorHandlerIndex, std::move(windowMetaData))
{
}

}
