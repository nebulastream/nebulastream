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


#include <Aggregation/SessionWindowProbePhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Aggregation/AggregationHashMapMerge.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/SessionWindowOperatorHandler.hpp>
#include <Aggregation/UnboundedAggregationOperatorHandler.hpp>
#include <Aggregation/UnboundedAggregationProbePhysicalOperator.hpp>
#include <DataTypes/VarVal.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
SessionWindowOperatorHandler& asHandler(OperatorHandler* handler)
{
    PRECONDITION(handler != nullptr, "The operator handler must not be null");
    return dynamic_cast<SessionWindowOperatorHandler&>(*handler);
}

bool arriveAndTryToApplyProxy(OperatorHandler* handler, const SequenceNumber sequenceNumber, const Timestamp watermark)
{
    return asHandler(handler).arriveAndTryToApply(sequenceNumber, watermark);
}

uint8_t claimNextProxy(OperatorHandler* handler)
{
    return static_cast<uint8_t>(asHandler(handler).claimNext());
}

void loadClaimedDeltaProxy(OperatorHandler* handler, TupleBuffer* delta)
{
    *delta = asHandler(handler).claimedDelta();
}

uint64_t claimedFrontierProxy(OperatorHandler* handler)
{
    return asHandler(handler).claimedFrontier().getRawValue();
}

bool hasTableProxy(OperatorHandler* handler)
{
    return asHandler(handler).hasTable();
}

void setTableProxy(OperatorHandler* handler, const TupleBuffer* table)
{
    asHandler(handler).setTable(*table);
}

void loadTableProxy(OperatorHandler* handler, TupleBuffer* table)
{
    *table = asHandler(handler).getTable();
}

bool hasOpenSessionsProxy(OperatorHandler* handler)
{
    return asHandler(handler).hasOpenSessions();
}

uint64_t openSessionsSliceEndProxy(OperatorHandler* handler)
{
    return asHandler(handler).getOpenSessionsSliceEnd().getRawValue();
}

void loadOpenSessionsProxy(OperatorHandler* handler, TupleBuffer* delta)
{
    *delta = asHandler(handler).getOpenSessions();
}

void setOpenSessionsProxy(OperatorHandler* handler, const TupleBuffer* delta, const uint64_t sliceEnd)
{
    asHandler(handler).setOpenSessions(*delta, Timestamp{sliceEnd});
}

void clearOpenSessionsProxy(OperatorHandler* handler)
{
    asHandler(handler).clearOpenSessions();
}
}

SessionWindowProbePhysicalOperator::SessionWindowProbePhysicalOperator(
    ChainedHashMapConfig hashMapConfig,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData,
    Record::RecordFieldIdentifier rowCountField,
    Record::RecordFieldIdentifier firstRecordField,
    Record::RecordFieldIdentifier lastRecordField,
    const uint64_t gapInMs)
    : UnboundedAggregationProbePhysicalOperator(
          std::move(hashMapConfig), std::move(aggregationPhysicalFunctions), operatorHandlerId, std::move(windowMetaData), rowCountField)
    , rowCountField(std::move(rowCountField))
    , firstRecordField(std::move(firstRecordField))
    , lastRecordField(std::move(lastRecordField))
    , gapInMs(gapInMs)
{
    PRECONDITION(this->aggregationPhysicalFunctions.size() >= 3, "A session window needs its three hidden aggregation functions");
    PRECONDITION(gapInMs > 0, "A session window needs a gap greater than zero");
    size_t offset = 0;
    for (const auto& function : this->aggregationPhysicalFunctions)
    {
        stateOffsets.push_back(offset);
        offset += function->getSizeOfStateInBytes();
    }
}

void SessionWindowProbePhysicalOperator::afterBuffer(ExecutionContext& executionCtx, RecordBuffer&) const
{
    const auto handler = executionCtx.getGlobalOperatorHandler(operatorHandlerId);
    if (nautilus::invoke(arriveAndTryToApplyProxy, handler, executionCtx.sequenceNumber, executionCtx.watermarkTs))
    {
        if (not nautilus::invoke(hasTableProxy, handler))
        {
            OwnedNautilusBuffer newTable;
            createAggregationHashMap(executionCtx, hashMapConfig, newTable);
            nautilus::invoke(setTableProxy, handler, newTable.asArg());
        }
        OwnedNautilusBuffer table;
        nautilus::invoke(loadTableProxy, handler, table.asArg());
        const BorrowedNautilusBuffer tableRef = table;

        constexpr auto nothing = static_cast<uint8_t>(UnboundedAggregationOperatorHandler::Claimed::NOTHING);
        constexpr auto deltaClaimed = static_cast<uint8_t>(UnboundedAggregationOperatorHandler::Claimed::DELTA);
        for (auto claimed = nautilus::invoke(claimNextProxy, handler); claimed != nothing;
             claimed = nautilus::invoke(claimNextProxy, handler))
        {
            const auto frontier = nautilus::invoke(claimedFrontierProxy, handler);
            if (claimed == deltaClaimed)
            {
                OwnedNautilusBuffer delta;
                nautilus::invoke(loadClaimedDeltaProxy, handler, delta.asArg());
                mergeDelta(executionCtx, tableRef, delta);
                /// Every session of the previous delta ends before the end of its slice plus the gap, which is at most the end of
                /// this slice: it is complete unless this delta extended it.
                if (nautilus::invoke(hasOpenSessionsProxy, handler))
                {
                    OwnedNautilusBuffer openSessions;
                    nautilus::invoke(loadOpenSessionsProxy, handler, openSessions.asArg());
                    closeSessions(executionCtx, tableRef, openSessions, frontier);
                    nautilus::invoke(clearOpenSessionsProxy, handler);
                }
                nautilus::invoke(setOpenSessionsProxy, handler, delta.asArg(), frontier);
            }
            else if (nautilus::invoke(hasOpenSessionsProxy, handler))
            {
                /// Frontiers and slice ends are multiples of the gap, so a watermark either stays at the end of the open sessions'
                /// slice or reaches the end plus the gap, where all of them are complete.
                if (frontier >= nautilus::invoke(openSessionsSliceEndProxy, handler) + nautilus::val<uint64_t>{gapInMs})
                {
                    OwnedNautilusBuffer openSessions;
                    nautilus::invoke(loadOpenSessionsProxy, handler, openSessions.asArg());
                    closeSessions(executionCtx, tableRef, openSessions, frontier);
                    nautilus::invoke(clearOpenSessionsProxy, handler);
                }
            }
        }
    }
}

void SessionWindowProbePhysicalOperator::mergeDelta(
    ExecutionContext& executionCtx, const BorrowedNautilusBuffer& table, const BorrowedNautilusBuffer& delta) const
{
    const auto countIndex = aggregationPhysicalFunctions.size() - 3;
    const auto firstIndex = aggregationPhysicalFunctions.size() - 2;
    const auto lastIndex = aggregationPhysicalFunctions.size() - 1;
    ChainedHashMapRef tableMap{table, hashMapConfig};
    const ChainedHashMapRef deltaMap{delta, hashMapConfig};
    for (const auto deltaEntry : deltaMap)
    {
        const ChainedHashMapRef::ChainedEntryRef deltaEntryRef{deltaEntry, delta, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
        const auto keys = deltaEntryRef.getKey();

        /// A key the table does not have yet starts from reset states, whose count of zero means that it has no open session.
        const auto tableEntry = tableMap.findOrCreateEntry(
            keys,
            [&](const nautilus::val<AbstractHashMapEntry*>& entry)
            {
                const ChainedHashMapRef::ChainedEntryRef newEntryRef{entry, table, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
                resetStates(executionCtx, static_cast<nautilus::val<AggregationState*>>(newEntryRef.getValueMemArea()), table);
            },
            executionCtx.pipelineMemoryProvider.bufferProvider);
        const ChainedHashMapRef::ChainedEntryRef tableEntryRef{tableEntry, table, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
        const auto tableStates = static_cast<nautilus::val<AggregationState*>>(tableEntryRef.getValueMemArea());
        const auto deltaStates = static_cast<nautilus::val<AggregationState*>>(deltaEntryRef.getValueMemArea());

        /// The delta's records are all later than the open session's, so the session continues iff the delta's first record lies
        /// at most the gap after the session's last record, i.e. at or before the session's end. Like Flink, a record exactly at the
        /// end continues the session.
        const auto isOpen = lowerHidden(executionCtx, countIndex, tableStates, table) != nautilus::val<uint64_t>{0};
        const auto sessionEnd = lowerHidden(executionCtx, lastIndex, tableStates, table) + nautilus::val<uint64_t>{gapInMs};
        const auto deltaFirst = lowerHidden(executionCtx, firstIndex, deltaStates, delta);
        if (isOpen and deltaFirst > sessionEnd)
        {
            emitSession(executionCtx, keys, tableStates, table);
            resetStates(executionCtx, tableStates, table);
        }

        auto tableState = tableStates;
        auto deltaState = deltaStates;
        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            aggFunction->combine(tableState, table, deltaState, delta, executionCtx.pipelineMemoryProvider);
            tableState = tableState + aggFunction->getSizeOfStateInBytes();
            deltaState = deltaState + aggFunction->getSizeOfStateInBytes();
        }
    }
}

void SessionWindowProbePhysicalOperator::closeSessions(
    ExecutionContext& executionCtx,
    const BorrowedNautilusBuffer& table,
    const BorrowedNautilusBuffer& delta,
    const nautilus::val<uint64_t>& frontier) const
{
    const auto countIndex = aggregationPhysicalFunctions.size() - 3;
    const auto lastIndex = aggregationPhysicalFunctions.size() - 1;
    ChainedHashMapRef tableMap{table, hashMapConfig};
    const ChainedHashMapRef deltaMap{delta, hashMapConfig};
    for (const auto deltaEntry : deltaMap)
    {
        const ChainedHashMapRef::ChainedEntryRef deltaEntryRef{deltaEntry, delta, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
        const auto keys = deltaEntryRef.getKey();
        /// Merging the delta created the entry of each of its keys in the table, so nothing is inserted here.
        const auto tableEntry = tableMap.findOrCreateEntry(
            keys, [](const nautilus::val<AbstractHashMapEntry*>&) { }, executionCtx.pipelineMemoryProvider.bufferProvider);
        const ChainedHashMapRef::ChainedEntryRef tableEntryRef{tableEntry, table, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
        const auto tableStates = static_cast<nautilus::val<AggregationState*>>(tableEntryRef.getValueMemArea());
        /// All records before the frontier have been applied, so no record can extend a session that ends before it.
        const auto isOpen = lowerHidden(executionCtx, countIndex, tableStates, table) != nautilus::val<uint64_t>{0};
        const auto sessionEnd = lowerHidden(executionCtx, lastIndex, tableStates, table) + nautilus::val<uint64_t>{gapInMs};
        if (isOpen and sessionEnd < frontier)
        {
            emitSession(executionCtx, keys, tableStates, table);
            resetStates(executionCtx, tableStates, table);
        }
    }
}

void SessionWindowProbePhysicalOperator::emitSession(
    ExecutionContext& executionCtx,
    const Record& keys,
    const nautilus::val<AggregationState*>& states,
    const BorrowedNautilusBuffer& table) const
{
    auto record = lowerStates(executionCtx, states, table);
    record.reassignFields(keys);
    const auto first = record.read(firstRecordField).getRawValueAs<nautilus::val<uint64_t>>();
    const auto last = record.read(lastRecordField).getRawValueAs<nautilus::val<uint64_t>>();
    record.write(windowMetaData.startField.getFullyQualifiedName(), VarVal{first});
    record.write(windowMetaData.endField.getFullyQualifiedName(), VarVal{last + nautilus::val<uint64_t>{gapInMs}});
    executeChild(executionCtx, record);
}

void SessionWindowProbePhysicalOperator::resetStates(
    ExecutionContext& executionCtx, nautilus::val<AggregationState*> states, const BorrowedNautilusBuffer& table) const
{
    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
    {
        aggFunction->reset(states, table, executionCtx.pipelineMemoryProvider);
        states = states + aggFunction->getSizeOfStateInBytes();
    }
}

nautilus::val<uint64_t> SessionWindowProbePhysicalOperator::lowerHidden(
    ExecutionContext& executionCtx,
    const size_t functionIndex,
    const nautilus::val<AggregationState*>& states,
    const BorrowedNautilusBuffer& hashMapBuffer) const
{
    const auto& function = aggregationPhysicalFunctions.at(functionIndex);
    const auto fieldIdentifier = functionIndex == aggregationPhysicalFunctions.size() - 3 ? rowCountField
        : functionIndex == aggregationPhysicalFunctions.size() - 2                        ? firstRecordField
                                                                                          : lastRecordField;
    const auto record = function->lower(states + stateOffsets.at(functionIndex), hashMapBuffer, executionCtx.pipelineMemoryProvider);
    return record.read(fieldIdentifier).getRawValueAs<nautilus::val<uint64_t>>();
}

}
