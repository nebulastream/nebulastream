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

#include <Aggregation/UnboundedAggregationProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Aggregation/AggregationHashMapMerge.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/UnboundedAggregationOperatorHandler.hpp>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/SlicedWindowStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/RowKind.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>
#include <select.hpp>

namespace NES
{

namespace
{
UnboundedAggregationOperatorHandler& asHandler(OperatorHandler* handler)
{
    PRECONDITION(handler != nullptr, "The operator handler must not be null");
    return dynamic_cast<UnboundedAggregationOperatorHandler&>(*handler);
}

void parkDeltaProxy(OperatorHandler* handler, const SequenceNumber sequenceNumber, const TupleBuffer* delta, const Timestamp sliceEnd)
{
    asHandler(handler).parkDelta(sequenceNumber, *delta, sliceEnd);
}

bool arriveAndTryToApplyProxy(OperatorHandler* handler, const SequenceNumber sequenceNumber, const Timestamp watermark)
{
    return asHandler(handler).arriveAndTryToApply(sequenceNumber, watermark);
}

uint64_t partitionOfProxy(OperatorHandler* handler, const SequenceNumber sequenceNumber)
{
    return asHandler(handler).partitionOf(sequenceNumber);
}

uint8_t claimNextProxy(OperatorHandler* handler, const uint64_t partition)
{
    return static_cast<uint8_t>(asHandler(handler).claimNext(partition));
}

void loadClaimedDeltaProxy(OperatorHandler* handler, TupleBuffer* delta, const uint64_t partition)
{
    *delta = asHandler(handler).claimedDelta(partition);
}

uint64_t claimedSliceEndProxy(OperatorHandler* handler, const uint64_t partition)
{
    return asHandler(handler).claimedSliceEnd(partition).getRawValue();
}

bool hasTableProxy(OperatorHandler* handler, const uint64_t partition)
{
    return asHandler(handler).hasTable(partition);
}

void setTableProxy(OperatorHandler* handler, const TupleBuffer* table, const uint64_t partition)
{
    asHandler(handler).setTable(*table, partition);
}

void loadTableProxy(OperatorHandler* handler, TupleBuffer* table, const uint64_t partition)
{
    *table = asHandler(handler).getTable(partition);
}
}

UnboundedAggregationProbePhysicalOperator::UnboundedAggregationProbePhysicalOperator(
    ChainedHashMapConfig hashMapConfig,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData,
    Record::RecordFieldIdentifier rowCountField,
    const uint64_t numberOfPartitions)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , aggregationPhysicalFunctions(std::move(aggregationPhysicalFunctions))
    , hashMapConfig(std::move(hashMapConfig))
    , rowCountField(std::move(rowCountField))
    , numberOfPartitions(numberOfPartitions)
{
    PRECONDITION(
        ChainedHashMap::calculateNumberOfChains(this->hashMapConfig.numberOfBuckets) % numberOfPartitions == 0,
        "The {} partitions of an unbounded aggregation must divide the number of chains of its hash maps",
        numberOfPartitions);
}

void UnboundedAggregationProbePhysicalOperator::probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    const auto emittedWindow = static_cast<nautilus::val<EmittedAggregationWindow*>>(recordBuffer.getMemArea());
    const auto numberOfHashMaps = readValueFromMemRef<uint64_t>(getMemberRef(emittedWindow, &EmittedAggregationWindow::numberOfHashMaps));
    const auto windowInfoRef = getMemberRef(emittedWindow, &EmittedAggregationWindow::windowInfo);
    const nautilus::val<Timestamp> sliceEnd{readValueFromMemRef<uint64_t>(getMemberRef(windowInfoRef, &WindowInfo::windowEnd))};

    /// Merge the per-thread hash maps of the slice into its delta. With several partitions, only the keys of the chains of this
    /// buffer's partition.
    const auto chainsPerPartition = ChainedHashMap::calculateNumberOfChains(hashMapConfig.numberOfBuckets) / numberOfPartitions;
    nautilus::val<uint64_t> firstChain{0};
    if (numberOfPartitions > 1)
    {
        firstChain = nautilus::invoke(partitionOfProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerId), executionCtx.sequenceNumber)
            * nautilus::val<uint64_t>{chainsPerPartition};
    }
    OwnedNautilusBuffer delta;
    createAggregationHashMap(executionCtx, hashMapConfig, delta);
    const BorrowedNautilusBuffer deltaRef = delta;
    for (nautilus::val<uint64_t> hashMapIdx = 0; hashMapIdx < numberOfHashMaps; ++hashMapIdx)
    {
        OwnedNautilusBuffer hashMap;
        nautilus::invoke(
            +[](TupleBuffer* parent, uint32_t hashMapIdx, TupleBuffer* hashMapBuffer)
            {
                INVARIANT(parent != nullptr, "Parent Tuplebuffer MUST NOT be null at this point");
                *hashMapBuffer = parent->loadChildBuffer(ChildBufferIndex{hashMapIdx});
            },
            recordBuffer.getReference(),
            hashMapIdx,
            hashMap.asArg());
        const BorrowedNautilusBuffer hashMapRef = hashMap;
        if (numberOfPartitions > 1)
        {
            combineAggregationHashMapChains(
                executionCtx,
                hashMapConfig,
                aggregationPhysicalFunctions,
                deltaRef,
                hashMapRef,
                firstChain,
                firstChain + nautilus::val<uint64_t>{chainsPerPartition});
        }
        else
        {
            combineAggregationHashMap(executionCtx, hashMapConfig, aggregationPhysicalFunctions, deltaRef, hashMapRef);
        }
    }
    nautilus::invoke(
        parkDeltaProxy, executionCtx.getGlobalOperatorHandler(operatorHandlerId), executionCtx.sequenceNumber, delta.asArg(), sliceEnd);

    /// The delta holds everything the probe needs, so the emitted window and the child buffers it holds can go.
    nautilus::invoke(
        +[](EmittedAggregationWindow* emittedAggregationWindow) { emittedAggregationWindow->~EmittedAggregationWindow(); },
        emittedWindow);
}

void UnboundedAggregationProbePhysicalOperator::afterBuffer(ExecutionContext& executionCtx, RecordBuffer&) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    if (nautilus::invoke(arriveAndTryToApplyProxy, handler, executionCtx.sequenceNumber, executionCtx.watermarkTs))
    {
        nautilus::val<uint64_t> partition{0};
        if (numberOfPartitions > 1)
        {
            partition = nautilus::invoke(partitionOfProxy, handler, executionCtx.sequenceNumber);
        }
        if (not nautilus::invoke(hasTableProxy, handler, partition))
        {
            OwnedNautilusBuffer newTable;
            createAggregationHashMap(executionCtx, hashMapConfig, newTable);
            nautilus::invoke(setTableProxy, handler, newTable.asArg(), partition);
        }
        OwnedNautilusBuffer table;
        nautilus::invoke(loadTableProxy, handler, table.asArg(), partition);
        const BorrowedNautilusBuffer tableRef = table;

        constexpr auto nothing = static_cast<uint8_t>(UnboundedAggregationOperatorHandler::Claimed::NOTHING);
        constexpr auto deltaClaimed = static_cast<uint8_t>(UnboundedAggregationOperatorHandler::Claimed::DELTA);
        for (auto claimed = nautilus::invoke(claimNextProxy, handler, partition); claimed != nothing;
             claimed = nautilus::invoke(claimNextProxy, handler, partition))
        {
            if (claimed == deltaClaimed)
            {
                OwnedNautilusBuffer delta;
                nautilus::invoke(loadClaimedDeltaProxy, handler, delta.asArg(), partition);
                /// The applier may apply the delta of an earlier slice than its own, whose end lies behind the watermark of the output.
                nautilus::val<Timestamp> updateTimestamp{nautilus::invoke(claimedSliceEndProxy, handler, partition)};
                if (updateTimestamp < executionCtx.watermarkTs)
                {
                    updateTimestamp = executionCtx.watermarkTs;
                }
                applyDelta(executionCtx, tableRef, delta, updateTimestamp);
            }
        }
    }
}

void UnboundedAggregationProbePhysicalOperator::applyDelta(
    ExecutionContext& executionCtx,
    const BorrowedNautilusBuffer& table,
    const BorrowedNautilusBuffer& delta,
    const nautilus::val<Timestamp>& updateTimestamp) const
{
    const auto endField = windowMetaData.endField.getFullyQualifiedName();
    const Record::RecordFieldIdentifier rowKindField{rowKindIdentifier()};
    ChainedHashMapRef tableMap{table, hashMapConfig};
    const ChainedHashMapRef deltaMap{delta, hashMapConfig};
    for (const auto deltaEntry : deltaMap)
    {
        const ChainedHashMapRef::ChainedEntryRef deltaEntryRef{deltaEntry, delta, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
        const auto keys = deltaEntryRef.getKey();

        /// A key the table does not have yet starts from reset states, whose row count of zero makes it absent.
        const auto tableEntry = tableMap.findOrCreateEntry(
            keys,
            [&](const nautilus::val<AbstractHashMapEntry*>& entry)
            {
                const ChainedHashMapRef::ChainedEntryRef newEntryRef{entry, table, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
                auto state = static_cast<nautilus::val<AggregationState*>>(newEntryRef.getValueMemArea());
                for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                {
                    aggFunction->reset(state, table, executionCtx.pipelineMemoryProvider);
                    state = state + aggFunction->getSizeOfStateInBytes();
                }
            },
            executionCtx.pipelineMemoryProvider.bufferProvider);
        const ChainedHashMapRef::ChainedEntryRef tableEntryRef{tableEntry, table, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
        const auto tableStates = static_cast<nautilus::val<AggregationState*>>(tableEntryRef.getValueMemArea());

        auto oldRecord = lowerStates(executionCtx, tableStates, table);
        const auto wasPresent = oldRecord.read(rowCountField).getRawValueAs<nautilus::val<uint64_t>>() != nautilus::val<uint64_t>{0};

        auto tableState = tableStates;
        auto deltaState = static_cast<nautilus::val<AggregationState*>>(deltaEntryRef.getValueMemArea());
        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            aggFunction->combine(tableState, table, deltaState, delta, executionCtx.pipelineMemoryProvider);
            tableState = tableState + aggFunction->getSizeOfStateInBytes();
            deltaState = deltaState + aggFunction->getSizeOfStateInBytes();
        }

        auto newRecord = lowerStates(executionCtx, tableStates, table);
        const auto isPresent = newRecord.read(rowCountField).getRawValueAs<nautilus::val<uint64_t>>() != nautilus::val<uint64_t>{0};

        /// A present key withdraws the row it emitted last: as an update if it stays present, as a delete otherwise.
        if (wasPresent)
        {
            const auto oldKind = nautilus::select(
                isPresent,
                nautilus::val<uint8_t>{static_cast<uint8_t>(RowKind::UPDATE_BEFORE)},
                nautilus::val<uint8_t>{static_cast<uint8_t>(RowKind::DELETE)});
            oldRecord.reassignFields(keys);
            oldRecord.write(endField, updateTimestamp.convertToValue());
            oldRecord.write(rowKindField, VarVal{oldKind});
            executeChild(executionCtx, oldRecord);
        }
        /// A key that is present afterwards emits its new row: as an update if it was present, as an insert otherwise.
        if (isPresent)
        {
            const auto newKind = nautilus::select(
                wasPresent,
                nautilus::val<uint8_t>{static_cast<uint8_t>(RowKind::UPDATE_AFTER)},
                nautilus::val<uint8_t>{static_cast<uint8_t>(RowKind::INSERT)});
            newRecord.reassignFields(keys);
            newRecord.write(endField, updateTimestamp.convertToValue());
            newRecord.write(rowKindField, VarVal{newKind});
            executeChild(executionCtx, newRecord);
        }
    }
}

Record UnboundedAggregationProbePhysicalOperator::lowerStates(
    ExecutionContext& executionCtx, nautilus::val<AggregationState*> states, const BorrowedNautilusBuffer& hashMapBuffer) const
{
    Record record;
    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
    {
        record.reassignFields(aggFunction->lower(states, hashMapBuffer, executionCtx.pipelineMemoryProvider));
        states = states + aggFunction->getSizeOfStateInBytes();
    }
    return record;
}

}
