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


#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/UnboundedAggregationProbePhysicalOperator.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Probe of an aggregation with a SESSION window (see SessionWindowOperatorHandler). Like the unbounded aggregation, it merges the
/// per-thread hash maps of a triggered slice into a delta, parks it, and lets one applier at a time apply the deltas in slice order
/// to a table with one open session per key. The last three aggregation functions are hidden: the COUNT of the session's records,
/// which is zero while a key has no open session, and the MIN and MAX of their timestamps, the first and last record of the session.
///
/// For a claimed slice, the applier
///  1. merges its delta into the table: a key whose open session ends before the first record of the delta emits it and starts a
///     new one, otherwise the delta extends the session,
///  2. emits the sessions of the previous delta that end before the end of the slice, i.e. those the delta did not extend,
///  3. keeps the delta as the open sessions.
/// For a claimed watermark, it emits the open sessions once the watermark reaches the end of their slice plus the gap.
/// A session emits one row with its aggregates, the window start (its first record) and the window end (its last record plus the gap).
class SessionWindowProbePhysicalOperator final : public UnboundedAggregationProbePhysicalOperator
{
public:
    SessionWindowProbePhysicalOperator(
        ChainedHashMapConfig hashMapConfig,
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
        OperatorHandlerId operatorHandlerId,
        WindowMetaData windowMetaData,
        Record::RecordFieldIdentifier rowCountField,
        Record::RecordFieldIdentifier firstRecordField,
        Record::RecordFieldIdentifier lastRecordField,
        uint64_t gapInMs);

protected:
    void afterBuffer(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    /// Merges every key of the delta into its open session, emitting the session first if the delta does not continue it.
    void mergeDelta(ExecutionContext& executionCtx, const BorrowedNautilusBuffer& table, const BorrowedNautilusBuffer& delta) const;

    /// Emits and resets the session of every key of the delta that is still open in the table and ends before the frontier.
    void closeSessions(
        ExecutionContext& executionCtx,
        const BorrowedNautilusBuffer& table,
        const BorrowedNautilusBuffer& delta,
        const nautilus::val<uint64_t>& frontier) const;

    void emitSession(
        ExecutionContext& executionCtx,
        const Record& keys,
        const nautilus::val<AggregationState*>& states,
        const BorrowedNautilusBuffer& table) const;

    void resetStates(ExecutionContext& executionCtx, nautilus::val<AggregationState*> states, const BorrowedNautilusBuffer& table) const;

    /// Lowers the hidden aggregation function at the given index into its UINT64 value.
    [[nodiscard]] nautilus::val<uint64_t> lowerHidden(
        ExecutionContext& executionCtx,
        size_t functionIndex,
        const nautilus::val<AggregationState*>& states,
        const BorrowedNautilusBuffer& hashMapBuffer) const;

    Record::RecordFieldIdentifier rowCountField;
    Record::RecordFieldIdentifier firstRecordField;
    Record::RecordFieldIdentifier lastRecordField;
    uint64_t gapInMs;
    /// Byte offset of every aggregation function's state within an entry's states.
    std::vector<size_t> stateOffsets;
};

}
