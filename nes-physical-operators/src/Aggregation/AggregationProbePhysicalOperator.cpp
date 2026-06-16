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
#include <optional>
#include <tuple>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/VariableSizedAccess.hpp>
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
    OwnedNautilusBuffer finalHashMapNautilusBuffer;
    nautilus::invoke(
        +[](const TupleBuffer* parent, AbstractBufferProvider* bufferProvider, TupleBuffer* finalHashMapBuffer)
        {
            INVARIANT(parent != nullptr, "Parent Tuplebuffer MUST NOT be null at this point");
            /// load the first hash map
            const VariableSizedAccess::Index bufferIndex{0};
            auto buffer = parent->loadChildBuffer(bufferIndex);
            const ChainedHashMap chm = ChainedHashMap::load(buffer);
            /// get a buffer and for the final hash map with the same config
            auto neededFinalBufferSize = ChainedHashMap::calculateBufferSizeFromChains(chm.getNumberOfChains());
            std::optional<TupleBuffer> finalHashMapTupleBuffer = bufferProvider->getUnpooledBuffer(neededFinalBufferSize);
            if (not finalHashMapTupleBuffer.has_value())
            {
                throw CannotAllocateBuffer("{}B for the hash join window trigger were requested", neededFinalBufferSize);
            }
            /// initialize the final hash map tuple buffer
            *finalHashMapBuffer = finalHashMapTupleBuffer.value();
            std::ignore = ChainedHashMap::init(*finalHashMapBuffer, chm.getEntrySize(), chm.getNumberOfBuckets(), chm.getPageSize());
        },
        recordBuffer.getReference(),
        executionCtx.pipelineMemoryProvider.bufferProvider,
        finalHashMapNautilusBuffer.asArg());
    /// Wrap the pinned final hash map buffer as a typed NautilusBuffer so it can be handed to the aggregation functions
    NautilusBuffer finalHashMapBuffer{std::move(finalHashMapNautilusBuffer)};

    /// Combining all keys from all hash maps in the final hash map, and then iterating over the final hash map once to lower the aggregation states
    ChainedHashMapRef finalHashMap(
        finalHashMapBuffer.asArg(),
        hashMapOptions.fieldKeys,
        hashMapOptions.fieldValues,
        hashMapOptions.entriesPerPage,
        hashMapOptions.entrySize);

    for (nautilus::val<uint64_t> curHashMapIdx = 0; curHashMapIdx < numberOfHashMaps; ++curHashMapIdx)
    {
        /// Use NautilusBuffer to persist the chained hash map buffer
        OwnedNautilusBuffer hashMapNautilusBuffer;
        nautilus::invoke(
            +[](TupleBuffer* parent, uint32_t curHashMapIdx, TupleBuffer* hashMapBuffer)
            {
                INVARIANT(parent != nullptr, "Parent Tuplebuffer MUST NOT be null at this point");
                const VariableSizedAccess::Index bufferIndex{curHashMapIdx};
                *hashMapBuffer = parent->loadChildBuffer(bufferIndex);
            },
            recordBuffer.getReference(),
            curHashMapIdx,
            hashMapNautilusBuffer.asArg());
        /// Wrap the pinned worker hash map buffer as a typed NautilusBuffer so it can be handed to the aggregation functions
        NautilusBuffer currentHashMapBuffer{std::move(hashMapNautilusBuffer)};
        const ChainedHashMapRef currentMap(
            currentHashMapBuffer.asArg(),
            hashMapOptions.fieldKeys,
            hashMapOptions.fieldValues,
            hashMapOptions.entriesPerPage,
            hashMapOptions.entrySize);
        for (const auto entry : currentMap)
        {
            const auto entryRef = currentMap.getEntryRef(entry);
            const auto tmpRecordKey = entryRef.getKey();

            /// Inserting the record key into the final/global hash map. If an entry for the key already exists, we have to combine the aggregation states
            /// We do this by iterating over the aggregation functions and combining all aggregation states into a global state.
            /// The buffers are captured by reference: insertOrUpdateEntry runs the callbacks synchronously while both buffers are alive.
            finalHashMap.insertOrUpdateEntry(
                entryRef.entryRef,
                [&executionCtx,
                 &entryRef,
                 &aggregationPhysicalFunctions = aggregationPhysicalFunctions,
                 &finalHashMap,
                 &finalHashMapBuffer,
                 &currentHashMapBuffer](const nautilus::val<AbstractHashMapEntry*>& entryOnUpdate)
                {
                    /// Combining the aggregation states of the current entry with the aggregation states of the final hash map
                    const auto entryRefOnInsert = finalHashMap.getEntryRef(entryOnUpdate);
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto currentState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        aggFunction->combine(
                            AggregationStateRef{globalState, finalHashMapBuffer},
                            AggregationStateRef{currentState, currentHashMapBuffer},
                            executionCtx.pipelineMemoryProvider);
                        globalState = globalState + aggFunction->getSizeOfStateInBytes();
                        currentState = currentState + aggFunction->getSizeOfStateInBytes();
                    }
                },
                [&executionCtx,
                 &entryRef,
                 &aggregationPhysicalFunctions = aggregationPhysicalFunctions,
                 &finalHashMap,
                 &finalHashMapBuffer,
                 &currentHashMapBuffer](const nautilus::val<AbstractHashMapEntry*>& entryOnInsert)
                {
                    /// If the entry for the provided key has not been seen by this hash map / worker thread, we need
                    /// to create a new one and initialize the aggregation states. After that, we can combine the aggregation states.
                    const auto entryRefOnInsert = finalHashMap.getEntryRef(entryOnInsert);
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto currentState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        /// In contrast to the lambda method above, we have to reset the aggregation state before combining it with the other state
                        const AggregationStateRef globalStateRef{globalState, finalHashMapBuffer};
                        aggFunction->reset(globalStateRef, executionCtx.pipelineMemoryProvider);
                        aggFunction->combine(
                            globalStateRef, AggregationStateRef{currentState, currentHashMapBuffer}, executionCtx.pipelineMemoryProvider);
                        globalState = globalState + aggFunction->getSizeOfStateInBytes();
                        currentState = currentState + aggFunction->getSizeOfStateInBytes();
                    }
                },
                executionCtx.pipelineMemoryProvider.bufferProvider);
        }
    }

    /// Lowering, each aggregation state in the final hash map and passing the record to the child
    for (const auto entry : finalHashMap)
    {
        const auto entryRef = finalHashMap.getEntryRef(entry);
        const auto recordKey = entryRef.getKey();
        Record outputRecord;
        for (auto finalState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
             const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            outputRecord.reassignFields(
                aggFunction->lower(AggregationStateRef{finalState, finalHashMapBuffer}, executionCtx.pipelineMemoryProvider));
            finalState = finalState + aggFunction->getSizeOfStateInBytes();
        }

        /// Adding the window start and end to the output record and then passing the record to the child
        outputRecord.reassignFields(recordKey);
        outputRecord.write(windowMetaData.windowStartFieldName, windowStart.convertToValue());
        outputRecord.write(windowMetaData.windowEndFieldName, windowEnd.convertToValue());
        executeChild(executionCtx, outputRecord);
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
