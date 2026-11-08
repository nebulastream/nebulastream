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
#include <Aggregation/Function/TopKAggregationPhysicalFunction.hpp>
#include <Aggregation/UnboundedAggregationProbePhysicalOperator.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/RowHeap/RowHeapRef.hpp>
#include <CompilationContext.hpp>
#include <Interface/Record.hpp>
#include <Operators/Windows/WindowMetaData.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>

namespace NES
{

/// Probe of ORDER BY ... LIMIT k BY ... WINDOW UNBOUNDED. It parks and applies the deltas of the slices in order like the unbounded
/// aggregation, but a delta holds the top rows of every key in the slice (see TopKAggregationPhysicalFunction). Applying a delta
/// sorts its rows and inserts them into the key's top rows of the table in their order, best first: a row that enters the top rows
/// then cannot be displaced by a later row of the same delta. A row that enters emits +I, and the last top row it replaces emits -D.
class UnboundedTopKProbePhysicalOperator final : public UnboundedAggregationProbePhysicalOperator
{
public:
    UnboundedTopKProbePhysicalOperator(
        ChainedHashMapConfig hashMapConfig,
        std::shared_ptr<TopKAggregationPhysicalFunction> topK,
        OperatorHandlerId operatorHandlerId,
        WindowMetaData windowMetaData);

    void setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const override;

protected:
    void applyDelta(
        ExecutionContext& executionCtx,
        const BorrowedNautilusBuffer& table,
        const BorrowedNautilusBuffer& delta,
        const nautilus::val<Timestamp>& updateTimestamp) const override;

private:
    /// Inserts the row into the top rows of a key, emitting the row as +I if it enters and the row it displaces as -D.
    void insertAndEmit(
        ExecutionContext& executionCtx, const RowHeapRef& topRows, const Record& row, const nautilus::val<Timestamp>& updateTimestamp) const;

    std::shared_ptr<TopKAggregationPhysicalFunction> topK;
};

}
