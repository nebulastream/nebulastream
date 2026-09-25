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
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <nautilus/region.hpp>
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
    /// Traced as an isolated region for the same reason as the hash-map lookup below: the slice lookup's paths (cache hit, cache
    /// miss) keep different values alive and would otherwise duplicate everything after them. The slice cache lends the hash map
    /// out of its cache entry, so only the pointer has to leave the region.
    nautilus::val<const TupleBuffer*> hashMapBufferPtr = nullptr;
    nautilus::region(
        "GetSliceDataStructure",
        [&]
        {
            const auto hashMapBuffer = sliceStoreRef->getDataStructureRef(
                timestamp, ctx.workerThreadId, operatorHandler, ctx.pipelineMemoryProvider.bufferProvider);
            INVARIANT(
                !hashMapBuffer.isOwned(), "The slice cache must lend its data structure, as only a borrowed buffer may leave the region");
            hashMapBufferPtr = hashMapBuffer.asArg();
        });
    const auto borrowedHashMapBuffer = BorrowedNautilusBuffer::from(hashMapBufferPtr);
    ChainedHashMapRef hashMap{borrowedHashMapBuffer, hashMapConfig};

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapConfig.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapConfig.fieldKeys[i];
        const auto& function = keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record. Traced as an isolated region: its paths (entry found, entry
    /// created, walking the chain) keep different values alive, which stops the tracer from merging them, so everything after
    /// the lookup would be traced once per path. Values created inside a region die at its end, so all paths leave the region
    /// in the same state and merge there. The entry is carried out through a value declared outside the region.
    nautilus::val<AbstractHashMapEntry*> hashMapEntry = nullptr;
    nautilus::region(
        "FindOrCreateEntry",
        [&]
        {
            hashMapEntry = hashMap.findOrCreateEntry(
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
        });

    /// Updating the aggregation states
    const ChainedHashMapRef::ChainedEntryRef entryRef{
        hashMapEntry, borrowedHashMapBuffer, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
    auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
    {
        aggFunction->lift(state, borrowedHashMapBuffer, ctx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

AggregationBuildPhysicalOperator::AggregationBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::unique_ptr<SliceStoreRef> sliceStoreRef,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
    ChainedHashMapConfig hashMapConfig,
    std::vector<PhysicalFunction> keyFunctions)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction), std::move(sliceStoreRef))
    , aggregationPhysicalFunctions(std::move(aggregationFunctions))
    , hashMapConfig(std::move(hashMapConfig))
    , keyFunctions(std::move(keyFunctions))
{
}

}
