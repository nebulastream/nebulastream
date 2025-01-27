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

#include <cstdint>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationBuild.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationOperatorHandler.hpp>
#include <Execution/Operators/Streaming/Aggregation/AggregationSlice.hpp>
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/Aggregation/WindowAggregationOperator.hpp>
#include <Execution/Operators/Streaming/WindowOperatorBuild.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::Operators
{

Interface::HashMap* getHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const Memory::AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(bufferProvider != nullptr, "The buffer provider should not be null");

    const auto createFunction = operatorHandler->getCreateNewSlicesFunction(bufferProvider);
    const auto hashMap = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, createFunction);
    INVARIANT(
        hashMap.size() == 1,
        "We expect exactly one slice for the given timestamp during the AggregationBuild, as we currently solely support slicing");

    /// Converting the slice to an AggregationSlice and returning the pointer to the hashmap
    const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(hashMap[0]);
    INVARIANT(aggregationSlice != nullptr, "The slice should be an AggregationSlice in an AggregationBuild");
    return aggregationSlice->getHashMapPtr(workerThreadId);
}


void AggregationBuild::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the correspinding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr = invoke(
        getHashMapProxy, ctx.getGlobalOperatorHandler(operatorHandlerIndex), timestamp, ctx.getWorkerThreadId(), ctx.bufferProvider);
    Interface::ChainedHashMapRef hashMap(hashMapPtr, fieldKeys, fieldValues, entriesPerPage, entrySize);

    /// Calling the key functions to add/update the keys to the record
    for (const auto& [field, function] : std::views::zip(fieldKeys, keyFunctions))
    {
        const auto value = function->execute(record);
        record.write(field.fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashFunction,
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, fieldKeys, fieldValues);
            auto state = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
            {
                aggFunction->reset(state, ctx.bufferProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.bufferProvider);


    /// Updating the aggregation states
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(hashMapEntry, fieldKeys, fieldValues);
    auto state = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
    {
        aggFunction->lift(state, ctx.bufferProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

AggregationBuild::AggregationBuild(
    const uint64_t operatorHandlerIndex,
    std::unique_ptr<TimeFunction> timeFunction,
    std::vector<std::unique_ptr<Functions::Function>> keyFunctions,
    WindowAggregationOperator windowAggregationOperator)
    : WindowAggregationOperator(std::move(windowAggregationOperator))
    , WindowOperatorBuild(operatorHandlerIndex, std::move(timeFunction))
    , keyFunctions(std::move(keyFunctions))
{
}

}
