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
#include <Aggregation/AggregationProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

Interface::HashMap* getHashMapPtrProxy(const EmittedAggregationWindow* emittedAggregationWindow, const uint64_t currentHashMapVal)
{
    PRECONDITION(emittedAggregationWindow != nullptr, "EmittedAggregationWindow must not be nullptr");
    PRECONDITION(
        currentHashMapVal < emittedAggregationWindow->numberOfHashMaps, "curHashMapVal must be smaller than the number of hash maps");
    return emittedAggregationWindow->hashMaps[currentHashMapVal];
}

void AggregationProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

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
    Interface::ChainedHashMapRef finalHashMap(
        finalHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);
    for (nautilus::val<uint64_t> curHashMap = 0; curHashMap < numberOfHashMaps; ++curHashMap)
    {
        const auto hashMapPtr = nautilus::invoke(getHashMapPtrProxy, aggregationWindowRef, curHashMap);
        const Interface::ChainedHashMapRef currentMap(
            hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);
        for (const auto entry : currentMap)
        {
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(
                entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            const auto tmpRecordKey = entryRef.getKey();

            /// Inserting the record key into the final/global hash map. If an entry for the key already exists, we have to combine the aggregation states
            /// We do this by iterating over the aggregation functions and combining all aggregation states into a global state.
            finalHashMap.insertOrUpdateEntry(
                entryRef.entryRef,
                [fieldKeys = hashMapOptions.fieldKeys,
                 fieldValues = hashMapOptions.fieldValues,
                 &executionCtx,
                 &entryRef,
                 &aggregationPhysicalFunctions = aggregationPhysicalFunctions,
                 hashMapPtr = hashMapPtr](const nautilus::val<Interface::AbstractHashMapEntry*>& entryOnUpdate)
                {
                    /// Combining the aggregation states of the current entry with the aggregation states of the final hash map
                    const Interface::ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(entryOnUpdate, hashMapPtr, fieldKeys, fieldValues);
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        aggFunction->combine(globalState, entryRefState, executionCtx.pipelineMemoryProvider);
                        globalState = globalState + aggFunction->getSizeOfStateInBytes();
                        entryRefState = entryRefState + aggFunction->getSizeOfStateInBytes();
                    }
                },
                [fieldKeys = hashMapOptions.fieldKeys,
                 fieldValues = hashMapOptions.fieldValues,
                 &executionCtx,
                 &entryRef,
                 &aggregationPhysicalFunctions = aggregationPhysicalFunctions,
                 hashMapPtr = hashMapPtr](const nautilus::val<Interface::AbstractHashMapEntry*>& entryOnInsert)
                {
                    /// If the entry for the provided key has not been seen by this hash map / worker thread, we need
                    /// to create a new one and initialize the aggregation states. After that, we can combine the aggregation states.
                    const Interface::ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(entryOnInsert, hashMapPtr, fieldKeys, fieldValues);
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        /// In contrast to the lambda method above, we have to reset the aggregation state before combining it with the other state
                        aggFunction->reset(globalState, executionCtx.pipelineMemoryProvider);
                        aggFunction->combine(globalState, entryRefStatePtr, executionCtx.pipelineMemoryProvider);
                        globalState = globalState + aggFunction->getSizeOfStateInBytes();
                        entryRefStatePtr = entryRefStatePtr + aggFunction->getSizeOfStateInBytes();
                    }
                },
                executionCtx.pipelineMemoryProvider.bufferProvider);
        }
    }

    /// Lowering, each aggregation state in the final hash map and passing the record to the child
    for (const auto entry : finalHashMap)
    {
        const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(
            entry, finalHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
        const auto recordKey = entryRef.getKey();
        Record outputRecord;

        for (auto finalStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
             const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            outputRecord.reassignFields(aggFunction->lower(finalStatePtr, executionCtx.pipelineMemoryProvider));
            finalStatePtr = finalStatePtr + aggFunction->getSizeOfStateInBytes();
        }

        /// Adding the window start and end to the output record and then passing the record to the child
        outputRecord.reassignFields(recordKey);
        outputRecord.write(windowMetaData.windowStartFieldName, windowStart.convertToValue());
        outputRecord.write(windowMetaData.windowEndFieldName, windowEnd.convertToValue());
        executeChild(executionCtx, outputRecord);

        for (auto finalStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
             const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            aggFunction->cleanup(finalStatePtr);
            finalStatePtr = finalStatePtr + aggFunction->getSizeOfStateInBytes();
        }
    }

    /// As we are creating a new hash map for the probe operator, we have to reset/destroy the final hash map of the emitted aggregation window
    nautilus::invoke(
        +[](EmittedAggregationWindow* emittedAggregationWindow)
        {
            NES_TRACE(
                "Resetting final hash map of emitted aggregation window start at {} and end at {}",
                emittedAggregationWindow->windowInfo.windowStart,
                emittedAggregationWindow->windowInfo.windowEnd);
            emittedAggregationWindow->finalHashMap.reset();
        },
        aggregationWindowRef);
}

AggregationProbePhysicalOperator::AggregationProbePhysicalOperator(
    HashMapOptions hashMapOptions,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , aggregationPhysicalFunctions(std::move(aggregationPhysicalFunctions))
    , hashMapOptions(std::move(hashMapOptions))
{
}
}
