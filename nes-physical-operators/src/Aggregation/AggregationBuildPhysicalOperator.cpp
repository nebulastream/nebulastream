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
#include <functional>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationFunction.hpp>
#include <Aggregation/WindowAggregation.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
Interface::HashMap* getHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    /// If a new aggregation slice is created, we need to set the cleanup function for the aggregation states
    auto wrappedCreateFunction(
        [createFunction = operatorHandler->getCreateNewSlicesFunction(),
         buildOperator](const SliceStart sliceStart, const SliceEnd sliceEnd)
        {
            const auto createdSlices = createFunction(sliceStart, sliceEnd);
            for (const auto& slice : createdSlices)
            {
                const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(slice);
                INVARIANT(aggregationSlice != nullptr, "The slice should be an AggregationSlice in an AggregationBuild");
                aggregationSlice->setCleanupFunction(buildOperator->getStateCleanupFunction());
            }
            return createdSlices;
        });

    const auto hashMap = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, wrappedCreateFunction);
    INVARIANT(
        hashMap.size() == 1,
        "We expect exactly one slice for the given timestamp during the AggregationBuild, as we currently solely support "
        "slicing, but got {}",
        hashMap.size());

    /// Converting the slice to an AggregationSlice and returning the pointer to the hashmap
    const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(hashMap[0]);
    INVARIANT(aggregationSlice != nullptr, "The slice should be an AggregationSlice in an AggregationBuild");
    return aggregationSlice->getHashMapPtr(workerThreadId);
}
}

void AggregationBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the correspinding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr = invoke(
        getHashMapProxy,
        ctx.getGlobalOperatorHandler(operatorHandlerId),
        timestamp,
        ctx.getWorkerThreadId(),
        nautilus::val<const AggregationBuildPhysicalOperator*>(this));
    Interface::ChainedHashMapRef hashMap(hashMapPtr, fieldKeys, fieldValues, entriesPerPage, entrySize);

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = fieldKeys[i];
        const auto& function = keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashFunction,
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, fieldKeys, fieldValues);
            auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
            {
                aggFunction->reset(state, ctx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.pipelineMemoryProvider.bufferProvider);


    /// Updating the aggregation states
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(hashMapEntry, fieldKeys, fieldValues);
    auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
    {
        aggFunction->lift(state, ctx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

std::function<void(const std::vector<std::unique_ptr<Interface::HashMap>>&)>
AggregationBuildPhysicalOperator::getStateCleanupFunction() const
{
    return [copyOfFieldKeys = fieldKeys,
            copyOfFieldValues = fieldValues,
            copyOfAggregationFunctions = aggregationFunctions,
            copyOfEntriesPerPage = entriesPerPage,
            copyOfEntrySize = entrySize](const std::vector<std::unique_ptr<Interface::HashMap>>& hashMaps)
    {
        for (const auto& hashMap :
             hashMaps | std::views::filter([](const auto& hashMapPtr) { return hashMapPtr->getNumberOfTuples() > 0; }))
        {
            {
                /// Using here the .get() is fine, as we are not moving the hashMap pointer.
                const Interface::ChainedHashMapRef hashMapRef(
                    hashMap.get(), copyOfFieldKeys, copyOfFieldValues, copyOfEntriesPerPage, copyOfEntrySize);
                for (const auto entry : hashMapRef)
                {
                    const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, copyOfFieldKeys, copyOfFieldValues);
                    auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(copyOfAggregationFunctions))
                    {
                        aggFunction->cleanup(state);
                        state = state + aggFunction->getSizeOfStateInBytes();
                    }
                }
            }
        }
    };
}

AggregationBuildPhysicalOperator::AggregationBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::vector<PhysicalFunction> keyFunctions,
    const std::shared_ptr<WindowAggregation>& windowAggregationOperator)
    : WindowAggregation(windowAggregationOperator)
    , WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction))
    , keyFunctions(std::move(keyFunctions))
{
}

}
