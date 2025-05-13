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
#include <Execution/Operators/Streaming/Aggregation/Function/AggregationFunction.hpp>
#include <Execution/Operators/Streaming/HashMapOptions.hpp>
#include <Execution/Operators/Streaming/HashMapSlice.hpp>
#include <Execution/Operators/Streaming/WindowOperatorBuild.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Time/Timestamp.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES::Runtime::Execution::Operators
{
Interface::HashMap* getAggHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuild* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    /// If a new hashmap slice is created, we need to set the cleanup function for the aggregation states
    auto wrappedCreateFunction(
        [createFunction = operatorHandler->getCreateNewSlicesFunction(),
        cleanupStateNautilusFunction = operatorHandler->cleanupStateNautilusFunction,
         buildOperator](const SliceStart sliceStart, const SliceEnd sliceEnd)
        {
            const auto createdSlices = createFunction(sliceStart, sliceEnd);
            for (const auto& slice : createdSlices)
            {
                const auto aggregationSlice = std::dynamic_pointer_cast<HashMapSlice>(slice);
                INVARIANT(aggregationSlice != nullptr, "The slice should be an AggregationSlice in an AggregationBuild");
                aggregationSlice->setCleanupFunction(buildOperator->getSliceCleanupFunction(cleanupStateNautilusFunction));
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
    const auto aggregationSlice = std::dynamic_pointer_cast<HashMapSlice>(hashMap[0]);
    INVARIANT(aggregationSlice != nullptr, "The slice should be an AggregationSlice in an AggregationBuild");
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        buildOperator->keySize, buildOperator->valueSize, buildOperator->pageSize, buildOperator->numberOfBuckets};
    return aggregationSlice->getHashMapPtrOrCreate(workerThreadId, hashMapSliceArgs);
}

void AggregationBuild::setup(ExecutionContext& executionCtx) const
{
    WindowOperatorBuild::setup(executionCtx);

    /// Creating the cleanup function for the slice of current stream
    nautilus::invoke(
        +[](AggregationOperatorHandler* operatorHandler, const AggregationBuild* buildOperator)
        {
            nautilus::engine::Options options;
            options.setOption("engine.Compilation", true);
            const nautilus::engine::NautilusEngine nautilusEngine(options);
            const auto cleanupStateNautilusFunction
                = std::make_shared<AggregationOperatorHandler::NautilusCleanupExec>(nautilusEngine.registerFunction(
                std::function(
                    [copyOfFieldKeys = buildOperator->fieldKeys,
                     copyOfFieldValues = buildOperator->fieldValues,
                     copyOfEntriesPerPage = buildOperator->entriesPerPage,
                     copyOfEntrySize = buildOperator->entrySize,
                     copyOfAggregationFunctions = buildOperator->aggregationFunctions](nautilus::val<Nautilus::Interface::HashMap*> hashMap)
                    {
                        const Interface::ChainedHashMapRef hashMapRef(
                            hashMap, copyOfFieldKeys, copyOfFieldValues, copyOfEntriesPerPage, copyOfEntrySize);
                        for (const auto entry : hashMapRef)
                        {
                            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, copyOfFieldKeys, copyOfFieldValues);
                            auto state = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRefReset.getValueMemArea());
                            for (const auto& aggFunction : nautilus::static_iterable(copyOfAggregationFunctions))
                            {
                                aggFunction->cleanup(state);
                                state = state + aggFunction->getSizeOfStateInBytes();
                            }
                        }
                    })));
            operatorHandler->cleanupStateNautilusFunction = std::move(cleanupStateNautilusFunction);
        },
        executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
        nautilus::val<const AggregationBuild*>(this));
}

void AggregationBuild::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the correspinding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr = invoke(
        getAggHashMapProxy,
        ctx.getGlobalOperatorHandler(operatorHandlerIndex),
        timestamp,
        ctx.getWorkerThreadId(),
        nautilus::val<const AggregationBuild*>(this));
    Interface::ChainedHashMapRef hashMap(hashMapPtr, fieldKeys, fieldValues, entriesPerPage, entrySize);

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = fieldKeys[i];
        const auto& function = keyFunctions[i];
        const auto value = function->execute(record, ctx.pipelineMemoryProvider.arena);
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
            auto state = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
            {
                aggFunction->reset(state, ctx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.pipelineMemoryProvider.bufferProvider);


    /// Updating the aggregation states
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(hashMapEntry, fieldKeys, fieldValues);
    auto state = static_cast<nautilus::val<Aggregation::AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationFunctions))
    {
        aggFunction->lift(state, ctx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

AggregationBuild::AggregationBuild(
    const uint64_t operatorHandlerIndex,
    std::unique_ptr<TimeFunction> timeFunction,
    std::vector<std::shared_ptr<Aggregation::AggregationFunction>> aggregationFunctions,
    HashMapOptions hashMapOptions)
    : HashMapOptions(std::move(hashMapOptions))
    , WindowOperatorBuild(operatorHandlerIndex, std::move(timeFunction))
    , aggregationFunctions(std::move(aggregationFunctions))
{
    PRECONDITION(
        this->aggregationFunctions.size() == this->fieldValues.size(),
        "The number of aggregation functions must match the number of field values");
}

}
