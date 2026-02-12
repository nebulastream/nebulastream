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
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
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

void AggregationProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    /// Getting necessary values from the record buffer
    const auto aggregationWindowRef = static_cast<nautilus::val<EmittedAggregationWindow*>>(recordBuffer.getMemArea());
    const auto numberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::numberOfHashMaps));
    const auto windowInfoRef = getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    /// create final hash map and pin it
    nautilus::val<TupleBuffer*> pinnedFinalBuffer = nautilus::invoke(
        +[](const TupleBuffer* parent, PipelineExecutionContext* pec, AbstractBufferProvider* bufferProvider)
        {
            INVARIANT(parent != nullptr, "Parent Tuplebuffer MUST NOT be null at this point");
            /// load the first hash map
            VariableSizedAccess::Index bufferIndex{0};
            auto buffer = parent->loadChildBuffer(bufferIndex);
            const ChainedHashMap chm = ChainedHashMap::load(buffer);
            /// get a buffer and for the final hash map with the same config
            auto neededFinalBufferSize = ChainedHashMap::calculateBufferSizeFromChains(chm.numberOfChains());
            std::optional<TupleBuffer> finalHashMapTupleBuffer = bufferProvider->getUnpooledBuffer(neededFinalBufferSize);
            if (not finalHashMapTupleBuffer.has_value())
            {
                throw CannotAllocateBuffer("{}B for the hash join window trigger were requested", neededFinalBufferSize);
            }
            /// initialize a view over the final hash map tuple buffer
            ChainedHashMap finalChainedHashMap
                = ChainedHashMap::init(finalHashMapTupleBuffer.value(), chm.entrySize(), chm.numberOfBuckets(), chm.pageSize());
            /// pin and return the address
            return std::addressof(pec->pinBuffer(std::move(finalHashMapTupleBuffer.value())));
        },
        recordBuffer.getReference(),
        executionCtx.pipelineContext,
        executionCtx.pipelineMemoryProvider.bufferProvider);

    /// Combining all keys from all hash maps in the final hash map, and then iterating over the final hash map once to lower the aggregation states
    ChainedHashMapRef finalHashMap(
        pinnedFinalBuffer, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);

    for (nautilus::val<uint64_t> curHashMap = 0; curHashMap < numberOfHashMaps; ++curHashMap)
    {
        /// Pin the current hashmap buffer
        auto pinnedCurrentBuffer = nautilus::invoke(
            +[](const TupleBuffer* parent, uint32_t curHashMap, PipelineExecutionContext* pec)
            {
                INVARIANT(parent != nullptr, "Parent Tuplebuffer MUST NOT be null at this point");
                VariableSizedAccess::Index bufferIndex{curHashMap};
                auto buffer = parent->loadChildBuffer(bufferIndex);
                return std::addressof(pec->pinBuffer(std::move(buffer)));
            },
            recordBuffer.getReference(),
            curHashMap,
            executionCtx.pipelineContext);

        const ChainedHashMapRef currentMap(
            pinnedCurrentBuffer,
            hashMapOptions.fieldKeys,
            hashMapOptions.fieldValues,
            hashMapOptions.entriesPerPage,
            hashMapOptions.entrySize);

        for (const auto entry : currentMap)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRef(
                entry, pinnedCurrentBuffer, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
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
                 pinnedCurrentBuffer = pinnedCurrentBuffer](const nautilus::val<AbstractHashMapEntry*>& entryOnUpdate)
                {
                    /// Combining the aggregation states of the current entry with the aggregation states of the final hash map
                    const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(entryOnUpdate, pinnedCurrentBuffer, fieldKeys, fieldValues);
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
                 pinnedCurrentBuffer = pinnedCurrentBuffer](const nautilus::val<AbstractHashMapEntry*>& entryOnInsert)
                {
                    /// If the entry for the provided key has not been seen by this hash map / worker thread, we need
                    /// to create a new one and initialize the aggregation states. After that, we can combine the aggregation states.
                    const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(entryOnInsert, pinnedCurrentBuffer, fieldKeys, fieldValues);
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
        const ChainedHashMapRef::ChainedEntryRef entryRef(entry, pinnedFinalBuffer, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
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
            emittedAggregationWindow->~EmittedAggregationWindow();
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
