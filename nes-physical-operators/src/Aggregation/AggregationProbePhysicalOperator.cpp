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
#include <Aggregation/AggregationHashMapMerge.hpp>
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
#include <SliceStore/SlicedWindowStoreInterface.hpp>
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

void AggregationProbePhysicalOperator::probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Getting necessary values from the record buffer
    const auto aggregationWindowRef = static_cast<nautilus::val<EmittedAggregationWindow*>>(recordBuffer.getMemArea());
    const auto numberOfHashMaps
        = readValueFromMemRef<uint64_t>(getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::numberOfHashMaps));
    const auto windowInfoRef = getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::windowInfo);
    const nautilus::val<Timestamp> windowStart{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowStart))};
    const nautilus::val<Timestamp> windowEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    /// Create the final hash map that combines the per-worker maps.
    OwnedNautilusBuffer finalHashMapNautilusBuffer;
    createAggregationHashMap(executionCtx, hashMapConfig, finalHashMapNautilusBuffer);

    /// Borrow the pinned final hash map buffer for the ChainedHashMapRef/entry views below (implicit Owned->Borrowed conversion).
    const BorrowedNautilusBuffer finalHashMapBufferRef = finalHashMapNautilusBuffer;

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
        const BorrowedNautilusBuffer hashMapBufferRef = hashMapNautilusBuffer;
        combineAggregationHashMap(executionCtx, hashMapConfig, aggregationPhysicalFunctions, finalHashMapBufferRef, hashMapBufferRef);
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

    /// The probe has consumed the emitted aggregation window (its per-thread hash maps were combined into the final hash map
    /// above), so destroy it here to release the child buffers it holds.
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
