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

#include <Aggregation/UnboundedTopKProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/TopKAggregationPhysicalFunction.hpp>
#include <Aggregation/UnboundedAggregationProbePhysicalOperator.hpp>
#include <DataTypes/VarVal.hpp>
#include <Identifiers/Identifier.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/RowHeap/RowHeapRef.hpp>
#include <CompilationContext.hpp>
#include <Interface/Record.hpp>
#include <Time/Timestamp.hpp>
#include <Util/RowKind.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

UnboundedTopKProbePhysicalOperator::UnboundedTopKProbePhysicalOperator(
    ChainedHashMapConfig hashMapConfig,
    std::shared_ptr<TopKAggregationPhysicalFunction> topK,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData)
    : UnboundedAggregationProbePhysicalOperator(
          std::move(hashMapConfig),
          std::vector<std::shared_ptr<AggregationPhysicalFunction>>{topK},
          operatorHandlerId,
          std::move(windowMetaData),
          Record::RecordFieldIdentifier{Identifier::parse("$ROWCOUNT")})
    , topK(std::move(topK))
{
}

void UnboundedTopKProbePhysicalOperator::applyDelta(
    ExecutionContext& executionCtx,
    const BorrowedNautilusBuffer& table,
    const BorrowedNautilusBuffer& delta,
    const nautilus::val<Timestamp>& updateTimestamp) const
{
    ChainedHashMapRef tableMap{table, hashMapConfig};
    const ChainedHashMapRef deltaMap{delta, hashMapConfig};
    for (const auto deltaEntry : deltaMap)
    {
        const ChainedHashMapRef::ChainedEntryRef deltaEntryRef{deltaEntry, delta, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
        const auto keys = deltaEntryRef.getKey();
        const auto tableEntry = tableMap.findOrCreateEntry(
            keys,
            [&](const nautilus::val<AbstractHashMapEntry*>& entry)
            {
                const ChainedHashMapRef::ChainedEntryRef newEntryRef{entry, table, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
                topK->reset(static_cast<nautilus::val<AggregationState*>>(newEntryRef.getValueMemArea()), table, executionCtx.pipelineMemoryProvider);
            },
            executionCtx.pipelineMemoryProvider.bufferProvider);
        const ChainedHashMapRef::ChainedEntryRef tableEntryRef{tableEntry, table, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};

        auto tableRowsBuffer
            = TopKAggregationPhysicalFunction::loadRows(static_cast<nautilus::val<AggregationState*>>(tableEntryRef.getValueMemArea()), table);
        const RowHeapRef topRows(BorrowedNautilusBuffer::from(tableRowsBuffer.asArg()), topK->getRowLayout());
        auto deltaRowsBuffer
            = TopKAggregationPhysicalFunction::loadRows(static_cast<nautilus::val<AggregationState*>>(deltaEntryRef.getValueMemArea()), delta);
        const RowHeapRef deltaRows(BorrowedNautilusBuffer::from(deltaRowsBuffer.asArg()), topK->getRowLayout());

        /// The delta is discarded after it is applied, so its rows can be sorted in place.
        deltaRows.sortRows(topK->getComparator());
        const auto numberOfDeltaRows = deltaRows.size();
        for (nautilus::val<uint64_t> index = 0; index < numberOfDeltaRows; ++index)
        {
            insertAndEmit(executionCtx, topRows, deltaRows.at(index), updateTimestamp);
        }
    }
}

void UnboundedTopKProbePhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    topK->setup(compilationContext);
    UnboundedAggregationProbePhysicalOperator::setup(executionCtx, compilationContext);
}

void UnboundedTopKProbePhysicalOperator::insertAndEmit(
    ExecutionContext& executionCtx, const RowHeapRef& topRows, const Record& row, const nautilus::val<Timestamp>& updateTimestamp) const
{
    const Record::RecordFieldIdentifier rowKindField{rowKindIdentifier()};
    const auto endField = windowMetaData.endField.getFullyQualifiedName();
    nautilus::val<bool> entered = false;
    if (topRows.size() < nautilus::val<uint64_t>{topK->getLimit()})
    {
        topRows.push(row, executionCtx.pipelineMemoryProvider.bufferProvider, topK->getComparator());
        entered = true;
    }
    else
    {
        /// The root is the last of the top rows, which a better row replaces.
        auto replaced = topRows.at(0);
        if (topK->comesBefore(row, replaced))
        {
            replaced.write(endField, updateTimestamp.convertToValue());
            replaced.write(rowKindField, VarVal{nautilus::val<uint8_t>{static_cast<uint8_t>(RowKind::DELETE)}});
            executeChild(executionCtx, replaced);
            topRows.replaceTop(row, executionCtx.pipelineMemoryProvider.bufferProvider, topK->getComparator());
            entered = true;
        }
    }
    if (entered)
    {
        auto added = row;
        added.write(endField, updateTimestamp.convertToValue());
        added.write(rowKindField, VarVal{nautilus::val<uint8_t>{static_cast<uint8_t>(RowKind::INSERT)}});
        executeChild(executionCtx, added);
    }
}

}
