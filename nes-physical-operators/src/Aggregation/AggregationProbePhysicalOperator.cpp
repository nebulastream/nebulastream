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
#include <Aggregation/SerializableAggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/OffsetHashMap/OffsetHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/DataStructures/OffsetHashMapWrapper.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Util.hpp>
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

Interface::HashMap* getSerializableHashMapPtrProxy(const EmittedSerializableAggregationWindow* emittedWindow, const uint64_t currentHashMapVal)
{
    PRECONDITION(emittedWindow != nullptr, "EmittedSerializableAggregationWindow must not be nullptr");
    PRECONDITION(
        currentHashMapVal < emittedWindow->numberOfHashMaps, "curHashMapVal must be smaller than the number of hash maps");
    return emittedWindow->hashMaps[currentHashMapVal];
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


    // Determine if we're dealing with serializable aggregation by checking the operator handler
    auto operatorHandler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    
    // Avoid EXEC logging in compiled mode to prevent tracing crashes
    
    // Decide based on static flag set by rewrite rule to avoid JIT ambiguity
    bool isSerializable = useSerializableAggregation;
    
    // no printf logging in compiled mode


    if (isSerializable)
    {
        // Handle serializable aggregation with proper OffsetHashMapRef
        const auto serializableWindowRef = static_cast<nautilus::val<EmittedSerializableAggregationWindow*>>(recordBuffer.getBuffer());
        const auto windowStart = invoke(
            +[](const EmittedSerializableAggregationWindow* emittedWindow) { return emittedWindow->windowInfo.windowStart; },
            serializableWindowRef);
        const auto windowEnd = invoke(
            +[](const EmittedSerializableAggregationWindow* emittedWindow) { return emittedWindow->windowInfo.windowEnd; },
            serializableWindowRef);
        const auto numberOfHashMaps = invoke(
            +[](const EmittedSerializableAggregationWindow* emittedWindow) { return emittedWindow->numberOfHashMaps; },
            serializableWindowRef);
        auto finalHashMapPtr = invoke(
            +[](const EmittedSerializableAggregationWindow* emittedWindow) { return emittedWindow->finalHashMap; },
            serializableWindowRef);
            
        NES_TRACE("SerializableAggregation window: numberOfHashMaps extracted from window");

        // Convert field offsets for OffsetHashMap
        auto convertedKeys = std::vector<NES::Nautilus::Interface::MemoryProvider::OffsetFieldOffsets>();
        auto convertedValues = std::vector<NES::Nautilus::Interface::MemoryProvider::OffsetFieldOffsets>();
        for (const auto& field : hashMapOptions.fieldKeys) {
            convertedKeys.emplace_back(NES::Nautilus::Interface::MemoryProvider::OffsetFieldOffsets{
                field.fieldIdentifier, field.type, field.fieldOffset
            });
        }
        // omit field debug prints in compiled runs
        for (const auto& field : hashMapOptions.fieldValues) {
            convertedValues.emplace_back(NES::Nautilus::Interface::MemoryProvider::OffsetFieldOffsets{
                field.fieldIdentifier, field.type, field.fieldOffset
            });
        }

        // Cast to OffsetHashMapWrapper and use OffsetHashMapRef
        auto finalWrapper = nautilus::invoke(
            +[](Interface::HashMap* hashMap) -> DataStructures::OffsetHashMapWrapper* {
                return static_cast<DataStructures::OffsetHashMapWrapper*>(hashMap);
            },
            finalHashMapPtr);
        Interface::OffsetHashMapRef finalHashMap(finalWrapper, convertedKeys, convertedValues);
        
        NES_TRACE("Created OffsetHashMapRef for final hashmap, now processing individual hashmaps");
        
        // Process each serializable hashmap
        NES_DEBUG_EXEC("PROBE_DEBUG: Processing serializable hashmaps");
        
        for (nautilus::val<uint64_t> curHashMap = 0; curHashMap < numberOfHashMaps; ++curHashMap)
        {
            NES_TRACE("Processing serializable hashmap in loop");
            NES_DEBUG_EXEC("PROBE_DEBUG: Processing hashmap iteration");
            
            const auto hashMapPtr = nautilus::invoke(getSerializableHashMapPtrProxy, serializableWindowRef, curHashMap);
            auto currentWrapper = nautilus::invoke(
                +[](Interface::HashMap* hashMap) -> DataStructures::OffsetHashMapWrapper* {
                    return static_cast<DataStructures::OffsetHashMapWrapper*>(hashMap);
                },
                hashMapPtr);
            const Interface::OffsetHashMapRef currentMap(currentWrapper, convertedKeys, convertedValues);
            
            // Debug: Check tuple count in current map
            auto tupleCount = nautilus::invoke(
                +[](DataStructures::OffsetHashMapWrapper* wrapper) -> uint64_t {
                    return wrapper->getNumberOfTuples();
                },
                currentWrapper);
            NES_DEBUG_EXEC("PROBE_DEBUG: Checking current hashmap tuple count");
            
            uint64_t entryCount = 0;
            for (const auto entry : currentMap)
            {
                entryCount++;
                NES_DEBUG_EXEC("PROBE_DEBUG: Processing entry from current map");
                
                const Interface::OffsetHashMapRef::OffsetEntryRef entryRef(
                    static_cast<nautilus::val<DataStructures::OffsetEntry*>>(static_cast<nautilus::val<void*>>(entry)), 
                    currentWrapper, convertedKeys, convertedValues);

                // Combine into final hashmap using OffsetHashMapRef logic
                finalHashMap.insertOrUpdateEntry(
                    entryRef.entryRef,
                    [convertedKeys, convertedValues, &executionCtx, &entryRef, &aggregationPhysicalFunctions = aggregationPhysicalFunctions, finalWrapper]
                    (const nautilus::val<Interface::AbstractHashMapEntry*>& entryOnUpdate)
                    {
                        const Interface::OffsetHashMapRef::OffsetEntryRef entryRefOnInsert(
                            static_cast<nautilus::val<DataStructures::OffsetEntry*>>(static_cast<nautilus::val<void*>>(entryOnUpdate)), 
                            finalWrapper, convertedKeys, convertedValues);
                        auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                        auto entryRefState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                // no extra dumps in compiled mode
                        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                        {
                            auto globalBefore = nautilus::invoke(+[](AggregationState* ptr) -> uint64_t {
                                return *reinterpret_cast<uint64_t*>(ptr);
                            }, globalState);
                            auto entryBefore = nautilus::invoke(+[](AggregationState* ptr) -> uint64_t {
                                return *reinterpret_cast<uint64_t*>(ptr);
                            }, entryRefState);
                            NES_DEBUG_EXEC("COMBINE_UPDATE_DEBUG: GlobalState=" << globalBefore << ", EntryState=" << entryBefore << " before combine");
                            aggFunction->combine(globalState, entryRefState, executionCtx.pipelineMemoryProvider);
                            auto globalAfter = nautilus::invoke(+[](AggregationState* ptr) -> uint64_t {
                                return *reinterpret_cast<uint64_t*>(ptr);
                            }, globalState);
                            NES_DEBUG_EXEC("COMBINE_UPDATE_DEBUG: GlobalAfter=" << globalAfter << " after combine");
                            globalState = globalState + aggFunction->getSizeOfStateInBytes();
                            entryRefState = entryRefState + aggFunction->getSizeOfStateInBytes();
                        }
                    },
                    [convertedKeys, convertedValues, &executionCtx, &entryRef, &aggregationPhysicalFunctions = aggregationPhysicalFunctions, finalWrapper]
                    (const nautilus::val<Interface::AbstractHashMapEntry*>& entryOnInsert)
                    {
                        const Interface::OffsetHashMapRef::OffsetEntryRef entryRefOnInsert(
                            static_cast<nautilus::val<DataStructures::OffsetEntry*>>(static_cast<nautilus::val<void*>>(entryOnInsert)), 
                            finalWrapper, convertedKeys, convertedValues);
                        auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                        auto entryRefStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                        // no extra dumps in compiled mode
                        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                        {
                            aggFunction->reset(globalState, executionCtx.pipelineMemoryProvider);
                            aggFunction->combine(globalState, entryRefStatePtr, executionCtx.pipelineMemoryProvider);
                            globalState = globalState + aggFunction->getSizeOfStateInBytes();
                            entryRefStatePtr = entryRefStatePtr + aggFunction->getSizeOfStateInBytes();
                        }
                    },
                    executionCtx.pipelineMemoryProvider.bufferProvider);
            }
        }

        // Lower final results from serializable aggregation
        NES_DEBUG_EXEC("PROBE_DEBUG: Starting final result lowering from finalHashMap");
        
        uint64_t finalEntryCount = 0;
        for (const auto entry : finalHashMap)
        {
            finalEntryCount++;
            NES_DEBUG_EXEC("PROBE_DEBUG: Lowering final entry");
            
            const Interface::OffsetHashMapRef::OffsetEntryRef entryRef(
                static_cast<nautilus::val<DataStructures::OffsetEntry*>>(static_cast<nautilus::val<void*>>(entry)), 
                finalWrapper, convertedKeys, convertedValues);
            const auto recordKey = entryRef.getKey();
            Record outputRecord;
            // no extra dumps in compiled mode
            for (auto finalStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                 const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                outputRecord.reassignFields(aggFunction->lower(finalStatePtr, executionCtx.pipelineMemoryProvider));
                finalStatePtr = finalStatePtr + aggFunction->getSizeOfStateInBytes();
            }

            outputRecord.reassignFields(recordKey);
            outputRecord.write(windowMetaData.windowStartFieldName, windowStart.convertToValue());
            outputRecord.write(windowMetaData.windowEndFieldName, windowEnd.convertToValue());
            
            NES_DEBUG_EXEC("PROBE_DEBUG: Executing child with serializable output record");
            executeChild(executionCtx, outputRecord);

            // Cleanup serializable aggregation states
            for (auto finalStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                 const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                aggFunction->cleanup(finalStatePtr);
                finalStatePtr = finalStatePtr + aggFunction->getSizeOfStateInBytes();
            }
        }

        // Reset final hashmap for serializable aggregation
        nautilus::invoke(
            +[](EmittedSerializableAggregationWindow* emittedWindow)
            {
                NES_TRACE(
                    "Resetting final hash map of emitted serializable aggregation window start at {} and end at {}",
                    emittedWindow->windowInfo.windowStart,
                    emittedWindow->windowInfo.windowEnd);
                delete emittedWindow->finalHashMap; 
                emittedWindow->finalHashMap = nullptr;
            },
            serializableWindowRef);
    }
    else
    {
        // Handle regular aggregation window (existing logic)
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

        /// Use ChainedHashMapRef for regular aggregation
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

                finalHashMap.insertOrUpdateEntry(
                    entryRef.entryRef,
                    [fieldKeys = hashMapOptions.fieldKeys,
                     fieldValues = hashMapOptions.fieldValues,
                     &executionCtx,
                     &entryRef,
                     &aggregationPhysicalFunctions = aggregationPhysicalFunctions,
                     hashMapPtr = hashMapPtr](const nautilus::val<Interface::AbstractHashMapEntry*>& entryOnUpdate)
                    {
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
                        const Interface::ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(entryOnInsert, hashMapPtr, fieldKeys, fieldValues);
                        auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                        auto entryRefStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                        {
                            aggFunction->reset(globalState, executionCtx.pipelineMemoryProvider);
                            aggFunction->combine(globalState, entryRefStatePtr, executionCtx.pipelineMemoryProvider);
                            globalState = globalState + aggFunction->getSizeOfStateInBytes();
                            entryRefStatePtr = entryRefStatePtr + aggFunction->getSizeOfStateInBytes();
                        }
                    },
                    executionCtx.pipelineMemoryProvider.bufferProvider);
            }
        }

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
}

AggregationProbePhysicalOperator::AggregationProbePhysicalOperator(
    HashMapOptions hashMapOptions,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData,
    bool useSerializableAggregation)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , aggregationPhysicalFunctions(std::move(aggregationPhysicalFunctions))
    , hashMapOptions(std::move(hashMapOptions))
    , useSerializableAggregation(useSerializableAggregation)
{
}
}
