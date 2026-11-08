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

#include <memory>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <WindowProbePhysicalOperator.hpp>

namespace NES
{

/// Probe of an aggregation with an UNBOUNDED window (see UnboundedAggregationOperatorHandler). For a triggered slice, it merges the
/// per-thread hash maps into a delta and parks it. For every buffer, it then applies the parked deltas in slice order, if no other
/// probe does already. A hidden COUNT of the key's records, the last aggregation function, tells whether a key is present.
/// Applying a delta emits, per key of the delta, +I for a key that becomes present, -U with the old and +U with the new
/// aggregates for a key that stays present, and -D for a key whose last record was withdrawn; the row kind is in $ROWKIND.
/// Every row also carries the window end field: the end of the slice whose delta produced it, but not below the watermark of the
/// output, so that a downstream unbounded operator can use it as its time field (WINDOW UNBOUNDED(end)).
class UnboundedAggregationProbePhysicalOperator final : public WindowProbePhysicalOperator
{
public:
    UnboundedAggregationProbePhysicalOperator(
        ChainedHashMapConfig hashMapConfig,
        std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
        OperatorHandlerId operatorHandlerId,
        WindowMetaData windowMetaData,
        Record::RecordFieldIdentifier rowCountField);

protected:
    void probeWindow(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;
    void afterBuffer(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const override;

private:
    /// Combines every key of the delta into the table and emits the changes.
    void applyDelta(
        ExecutionContext& executionCtx,
        const BorrowedNautilusBuffer& table,
        const BorrowedNautilusBuffer& delta,
        const nautilus::val<Timestamp>& updateTimestamp) const;
    /// Lowers all aggregation states of one entry into a record.
    [[nodiscard]] Record
    lowerStates(ExecutionContext& executionCtx, nautilus::val<AggregationState*> states, const BorrowedNautilusBuffer& hashMapBuffer) const;

    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions;
    ChainedHashMapConfig hashMapConfig;
    /// Result field of the hidden COUNT of every key's records.
    Record::RecordFieldIdentifier rowCountField;
};

}
