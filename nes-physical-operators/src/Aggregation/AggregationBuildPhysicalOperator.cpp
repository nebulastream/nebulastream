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
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Aggregation/Function/TopKAggregationPhysicalFunction.hpp>
#include <DataTypes/VarVal.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapSlice.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES
{

void AggregationBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    for (const auto& aggFunction : aggregationPhysicalFunctions)
    {
        aggFunction->setup(compilationContext);
    }
    WindowBuildPhysicalOperator::setup(executionCtx, compilationContext);
}

void AggregationBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Getting the corresponding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    failIfOutOfOrder(ctx, timestamp);
    const auto hashMapBuffer
        = sliceStoreRef->getDataStructureRef(timestamp, ctx.workerThreadId, operatorHandler, ctx.pipelineMemoryProvider.bufferProvider);
    const auto borrowedHashMapBuffer = BorrowedNautilusBuffer::from(hashMapBuffer.asArg());
    ChainedHashMapRef hashMap{borrowedHashMapBuffer, hashMapConfig};

    if (timestampField.has_value())
    {
        record.write(*timestampField, VarVal{timestamp.convertToValue()});
    }

    if (writeArrivalOrder)
    {
        /// The sequence and chunk number order the buffers of an origin. Within a buffer, a counter of the worker thread, which
        /// processes the buffer's records in order, orders the records.
        const auto arrivalFields = TopKAggregationPhysicalFunction::arrivalFields();
        record.write(
            arrivalFields[0],
            VarVal{nautilus::invoke(+[](const SequenceNumber sequenceNumber) { return sequenceNumber.getRawValue(); }, ctx.sequenceNumber)});
        record.write(
            arrivalFields[1],
            VarVal{nautilus::invoke(+[](const ChunkNumber chunkNumber) { return static_cast<uint64_t>(chunkNumber.getRawValue()); }, ctx.chunkNumber)});
        record.write(
            arrivalFields[2],
            VarVal{nautilus::invoke(
                +[]() -> uint64_t
                {
                    thread_local uint64_t position = 0;
                    return position++;
                })});
    }

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapConfig.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapConfig.fieldKeys[i];
        const auto& function = keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const ChainedHashMapRef::ChainedEntryRef entryRefReset{
                entry, borrowedHashMapBuffer, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
            auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                aggFunction->reset(state, borrowedHashMapBuffer, ctx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.pipelineMemoryProvider.bufferProvider);

    /// Updating the aggregation states
    const ChainedHashMapRef::ChainedEntryRef entryRef{
        hashMapEntry, borrowedHashMapBuffer, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
    auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
    if (rowKindFunction.has_value())
    {
        /// INSERT and UPDATE_AFTER add the record, UPDATE_BEFORE and DELETE withdraw it (see rowKindSign).
        const auto kind = rowKindFunction->execute(record, ctx.pipelineMemoryProvider.arena).getRawValueAs<nautilus::val<uint8_t>>();
        const auto weight = nautilus::val<int64_t>{1} - (nautilus::val<int64_t>{2} * static_cast<nautilus::val<int64_t>>(kind & nautilus::val<uint8_t>{1}));
        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            aggFunction->liftWeighted(state, borrowedHashMapBuffer, ctx.pipelineMemoryProvider, record, weight);
            state = state + aggFunction->getSizeOfStateInBytes();
        }
    }
    else
    {
        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            aggFunction->lift(state, borrowedHashMapBuffer, ctx.pipelineMemoryProvider, record);
            state = state + aggFunction->getSizeOfStateInBytes();
        }
    }
}

AggregationBuildPhysicalOperator::AggregationBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::unique_ptr<SliceStoreRef> sliceStoreRef,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
    ChainedHashMapConfig hashMapConfig,
    std::vector<PhysicalFunction> keyFunctions,
    std::optional<PhysicalFunction> rowKindFunction,
    const bool writeArrivalOrder,
    std::optional<Record::RecordFieldIdentifier> timestampField)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction), std::move(sliceStoreRef))
    , aggregationPhysicalFunctions(std::move(aggregationFunctions))
    , hashMapConfig(std::move(hashMapConfig))
    , keyFunctions(std::move(keyFunctions))
    , rowKindFunction(std::move(rowKindFunction))
    , writeArrivalOrder(writeArrivalOrder)
    , timestampField(std::move(timestampField))
{
}

}
