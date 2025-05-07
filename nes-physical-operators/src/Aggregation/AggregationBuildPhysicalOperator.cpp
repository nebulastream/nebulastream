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
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Engine.hpp>
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
Interface::HashMap* getAggHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    /// If a new hashmap slice is created, we need to set the cleanup function for the aggregation states
    auto wrappedCreateFunction(
        [createFunction = operatorHandler->getCreateNewSlicesFunction(),
         cleanupStateNautilusFunction = operatorHandler->cleanupStateNautilusFunction](const SliceStart sliceStart, const SliceEnd sliceEnd)
        {
            const auto createdSlices = createFunction(sliceStart, sliceEnd);
            for (const auto& slice : createdSlices)
            {
                const auto aggregationSlice = std::dynamic_pointer_cast<HashMapSlice>(slice);
                INVARIANT(aggregationSlice != nullptr, "The slice should be an AggregationSlice in an AggregationBuild");
                aggregationSlice->setCleanupFunction(
                    [cleanupStateNautilusFunction](const std::vector<std::unique_ptr<Interface::HashMap>>& hashMaps)
                    {
                        for (const auto& hashMap : hashMaps
                                 | std::views::filter([](const auto& hashMapPtr)
                                                      { return hashMapPtr and hashMapPtr->getNumberOfTuples() > 0; }))
                        {
                            /// Calling the compiled nautilus function
                            cleanupStateNautilusFunction->operator()(hashMap.get());
                        }
                    });
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
        buildOperator->hashMapOptions.keySize,
        buildOperator->hashMapOptions.valueSize,
        buildOperator->hashMapOptions.pageSize,
        buildOperator->hashMapOptions.numberOfBuckets};
    return aggregationSlice->getHashMapPtrOrCreate(workerThreadId, hashMapSliceArgs);
}

void AggregationBuildPhysicalOperator::setup(ExecutionContext& executionCtx) const
{
    WindowBuildPhysicalOperator::setup(executionCtx);

    /// Creating the cleanup function for the slice of current stream
    nautilus::invoke(
        +[](AggregationOperatorHandler* operatorHandler, const AggregationBuildPhysicalOperator* buildOperator)
        {
            nautilus::engine::Options options;
            options.setOption("engine.Compilation", true);
            const nautilus::engine::NautilusEngine nautilusEngine(options);

            /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
            /// ReSharper disable once CppPassValueParameterByConstReference
            /// NOLINTBEGIN(performance-unnecessary-value-param)
            const auto cleanupStateNautilusFunction
                = std::make_shared<AggregationOperatorHandler::NautilusCleanupExec>(nautilusEngine.registerFunction(std::function(
                    [copyOfFieldKeys = buildOperator->hashMapOptions.fieldKeys,
                     copyOfFieldValues = buildOperator->hashMapOptions.fieldValues,
                     copyOfEntriesPerPage = buildOperator->hashMapOptions.entriesPerPage,
                     copyOfEntrySize = buildOperator->hashMapOptions.entrySize,
                     copyOfAggregationFunctions
                     = buildOperator->aggregationPhysicalFunctions](nautilus::val<Nautilus::Interface::HashMap*> hashMap)
                    {
                        const Interface::ChainedHashMapRef hashMapRef(
                            hashMap, copyOfFieldKeys, copyOfFieldValues, copyOfEntriesPerPage, copyOfEntrySize);
                        for (const auto entry : hashMapRef)
                        {
                            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(
                                entry, hashMap, copyOfFieldKeys, copyOfFieldValues);
                            auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
                            for (const auto& aggFunction : nautilus::static_iterable(copyOfAggregationFunctions))
                            {
                                aggFunction->cleanup(state);
                                state = state + aggFunction->getSizeOfStateInBytes();
                            }
                        }
                    })));
            /// NOLINTEND(performance-unnecessary-value-param)
            operatorHandler->cleanupStateNautilusFunction = cleanupStateNautilusFunction;
        },
        executionCtx.getGlobalOperatorHandler(operatorHandlerId),
        nautilus::val<const AggregationBuildPhysicalOperator*>(this));
}

void AggregationBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the correspinding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr = invoke(
        getAggHashMapProxy,
        ctx.getGlobalOperatorHandler(operatorHandlerId),
        timestamp,
        ctx.workerThreadId,
        nautilus::val<const AggregationBuildPhysicalOperator*>(this));
    Interface::ChainedHashMapRef hashMap(
        hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);

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
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(
                entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                aggFunction->reset(state, ctx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.pipelineMemoryProvider.bufferProvider);


    /// Updating the aggregation states
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(
        hashMapEntry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
    auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
    {
        aggFunction->lift(state, ctx, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

AggregationBuildPhysicalOperator::AggregationBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
    HashMapOptions hashMapOptions)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction))
    , aggregationPhysicalFunctions(std::move(aggregationFunctions))
    , hashMapOptions(std::move(hashMapOptions))
{
}

}
