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
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
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
    executionCtx.originEpoch = executionCtx.currentEpoch;
    openChild(executionCtx, recordBuffer);

    /// Getting necessary values from the record buffer
    const auto aggregationWindowRef = static_cast<nautilus::val<EmittedAggregationWindow*>>(recordBuffer.getMemArea());
    const auto numberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::numberOfHashMaps));
    const auto windowInfoRef = getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    /// create final hash map and pin it. Its sizing is the same as the per-worker maps it merges, and that is
    /// a query-compile-time constant, so it comes from hashMapConfig rather than being read back out of one
    /// of the input maps. The proxy takes the numbers individually: a non-capturing lambda cannot receive the
    /// config itself.
    OwnedNautilusBuffer finalHashMapNautilusBuffer;
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider,
            TupleBuffer* finalHashMapBuffer IF_PRECONDITION(, const uint64_t entrySize),
            const uint64_t numberOfBuckets IF_PRECONDITION(, const uint64_t pageSize),
            const uint64_t bloomBytes)
        {
            const auto neededFinalBufferSize = ChainedHashMap::calculateBufferSize(numberOfBuckets, bloomBytes);
            std::optional<TupleBuffer> finalHashMapTupleBuffer = bufferProvider->getUnpooledBuffer(neededFinalBufferSize);
            if (not finalHashMapTupleBuffer.has_value())
            {
                throw CannotAllocateBuffer("{}B for the hash join window trigger were requested", neededFinalBufferSize);
            }
            /// initialize the final hash map tuple buffer
            *finalHashMapBuffer = finalHashMapTupleBuffer.value();
            ChainedHashMap::init(*finalHashMapBuffer IF_PRECONDITION(, entrySize), numberOfBuckets IF_PRECONDITION(, pageSize), bloomBytes);
        },
        executionCtx.pipelineMemoryProvider.bufferProvider,
        finalHashMapNautilusBuffer.asArg() IF_PRECONDITION(, nautilus::val<uint64_t>{hashMapConfig.entrySize}),
        nautilus::val<uint64_t>{hashMapConfig.numberOfBuckets} IF_PRECONDITION(, nautilus::val<uint64_t>{hashMapConfig.pageSize}),
        nautilus::val<uint64_t>{hashMapConfig.bloomFilterMemAreaSize()});
    /// get the reference to the final hash map buffer
    auto finalHashMapBufferRef = finalHashMapNautilusBuffer.asArg();

    /// Combining all keys from all hash maps in the final hash map, and then iterating over the final hash map once to lower the aggregation states
    ChainedHashMapRef finalHashMap{finalHashMapBufferRef, hashMapConfig};

    for (nautilus::val<uint64_t> curHashMapIdx = 0; curHashMapIdx < numberOfHashMaps; ++curHashMapIdx)
    {
        /// Use NautilusBuffer to persist the chained hash map buffer
        OwnedNautilusBuffer hashMapNautilusBuffer;
        nautilus::invoke(
            +[](TupleBuffer* parent, uint32_t curHashMapIdx, TupleBuffer* hashMapBuffer)
            {
                INVARIANT(parent != nullptr, "Parent Tuplebuffer MUST NOT be null at this point");
                const ChildBufferIndex bufferIndex{curHashMapIdx};
                *hashMapBuffer = parent->loadChildBuffer(bufferIndex);
            },
            recordBuffer.getReference(),
            curHashMapIdx,
            hashMapNautilusBuffer.asArg());
        auto hashMapBufferRef = hashMapNautilusBuffer.asArg();
        const ChainedHashMapRef currentMap{hashMapBufferRef, hashMapConfig};
        for (const auto entry : currentMap)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRef{entry, hashMapBufferRef, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
            const auto tmpRecordKey = entryRef.getKey();

            /// Inserting the record key into the final/global hash map. If an entry for the key already exists, we have to combine the aggregation states
            /// We do this by iterating over the aggregation functions and combining all aggregation states into a global state.
            finalHashMap.insertOrUpdateEntry(
                entryRef.entryRef,
                [fieldKeys = hashMapConfig.fieldKeys,
                 fieldValues = hashMapConfig.fieldValues,
                 &executionCtx,
                 &entryRef,
                 &aggregationPhysicalFunctions = aggregationPhysicalFunctions,
                 pinnedFinalBuffer = finalHashMapBufferRef,
                 hashMapBufferRef = hashMapBufferRef](const nautilus::val<AbstractHashMapEntry*>& entryOnUpdate)
                {
                    /// Combining the aggregation states of the current entry with the aggregation states of the final hash map
                    const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert{entryOnUpdate, pinnedFinalBuffer, fieldKeys, fieldValues};
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        aggFunction->combine(
                            globalState, pinnedFinalBuffer, entryRefState, hashMapBufferRef, executionCtx.pipelineMemoryProvider);
                        globalState = globalState + aggFunction->getSizeOfStateInBytes();
                        entryRefState = entryRefState + aggFunction->getSizeOfStateInBytes();
                    }
                },
                [fieldKeys = hashMapConfig.fieldKeys,
                 fieldValues = hashMapConfig.fieldValues,
                 &executionCtx,
                 &entryRef,
                 &aggregationPhysicalFunctions = aggregationPhysicalFunctions,
                 pinnedFinalBuffer = finalHashMapBufferRef,
                 hashMapBufferRef = hashMapBufferRef](const nautilus::val<AbstractHashMapEntry*>& entryOnInsert)
                {
                    /// If the entry for the provided key has not been seen by this hash map / worker thread, we need
                    /// to create a new one and initialize the aggregation states. After that, we can combine the aggregation states.
                    const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert{entryOnInsert, pinnedFinalBuffer, fieldKeys, fieldValues};
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        /// In contrast to the lambda method above, we have to reset the aggregation state before combining it with the other state
                        aggFunction->reset(globalState, pinnedFinalBuffer, executionCtx.pipelineMemoryProvider);
                        aggFunction->combine(
                            globalState, pinnedFinalBuffer, entryRefStatePtr, hashMapBufferRef, executionCtx.pipelineMemoryProvider);
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
        const ChainedHashMapRef::ChainedEntryRef entryRef{entry, finalHashMapBufferRef, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
        const auto recordKey = entryRef.getKey();
        Record outputRecord;
        for (auto finalStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
             const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            outputRecord.reassignFields(aggFunction->lower(finalStatePtr, finalHashMapBufferRef, executionCtx.pipelineMemoryProvider));
            finalStatePtr = finalStatePtr + aggFunction->getSizeOfStateInBytes();
        }

        /// Adding the window start and end to the output record and then passing the record to the child
        outputRecord.reassignFields(recordKey);
        outputRecord.write(windowMetaData.startField.getFullyQualifiedName(), windowStart.convertToValue());
        outputRecord.write(windowMetaData.endField.getFullyQualifiedName(), windowEnd.convertToValue());
        executeChild(executionCtx, outputRecord);

        for (auto finalStatePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
             const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
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
    ChainedHashMapConfig hashMapConfig,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , aggregationPhysicalFunctions(std::move(aggregationPhysicalFunctions))
    , hashMapConfig(std::move(hashMapConfig))
{
}
}
