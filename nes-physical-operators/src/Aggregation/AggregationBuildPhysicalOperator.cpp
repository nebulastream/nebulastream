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
#include <ranges>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/Record.hpp>
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
    WindowBuildPhysicalOperator::setup(executionCtx, compilationContext);
}

void AggregationBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Getting the corresponding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapBuffer
        = sliceStoreRef->getDataStructureRef(timestamp, ctx.workerThreadId, operatorHandler, ctx.pipelineMemoryProvider.bufferProvider);
    ChainedHashMapRef hashMap(
        hashMapBuffer.asArg(),
        hashMapOptions.fieldKeys,
        hashMapOptions.fieldValues,
        hashMapOptions.entriesPerPage,
        hashMapOptions.entrySize,
        hashMapOptions.bloomFilter);

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapOptions.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapOptions.fieldKeys[i];
        const auto& function = hashMapOptions.keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashMapOptions.hashFunction,
        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const ChainedHashMapRef::ChainedEntryRef entryRefReset(
                entry, hashMapBuffer.asArg(), hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                aggFunction->reset(state, hashMapBuffer.asArg(), ctx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.pipelineMemoryProvider.bufferProvider);


    /// Updating the aggregation states
    const ChainedHashMapRef::ChainedEntryRef entryRef(
        hashMapEntry, hashMapBuffer.asArg(), hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
    auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
    {
        aggFunction->lift(state, hashMapBuffer.asArg(), ctx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

AggregationBuildPhysicalOperator::AggregationBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::unique_ptr<SliceStoreRef> sliceStoreRef,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
    HashMapOptions hashMapOptions)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction), std::move(sliceStoreRef))
    , aggregationPhysicalFunctions(std::move(aggregationFunctions))
    , hashMapOptions(std::move(hashMapOptions))
{
}

}
