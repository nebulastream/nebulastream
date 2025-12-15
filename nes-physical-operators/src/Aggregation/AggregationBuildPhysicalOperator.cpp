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
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <ranges>
#include <string_view>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <fmt/format.h>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <SliceStore/Slice.hpp>
#include <Util/Logger/Logger.hpp>
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
namespace
{
constexpr std::string_view AggregationCheckpointFileName = "aggregation_hashmap.bin";
std::mutex AggregationCheckpointMutex;

std::filesystem::path getAggregationCheckpointFile()
{
    auto baseDir = CheckpointManager::getCheckpointDirectory();
    baseDir /= AggregationCheckpointFileName;
    return baseDir;
}

void ensureCheckpointDirectory(const std::filesystem::path& checkpointFile)
{
    const auto checkpointDir = checkpointFile.parent_path();
    if (!checkpointDir.empty() && !std::filesystem::exists(checkpointDir))
    {
        std::filesystem::create_directories(checkpointDir);
    }
}
}

Interface::HashMap* getAggHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    /// If a new hashmap slice is created, we need to set the cleanup function for the aggregation states
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        {operatorHandler->cleanupStateNautilusFunction},
        buildOperator->hashMapOptions.keySize,
        buildOperator->hashMapOptions.valueSize,
        buildOperator->hashMapOptions.pageSize,
        buildOperator->hashMapOptions.numberOfBuckets};
    auto wrappedCreateFunction(
        [createFunction = operatorHandler->getCreateNewSlicesFunction(hashMapSliceArgs),
         cleanupStateNautilusFunction = operatorHandler->cleanupStateNautilusFunction](const SliceStart sliceStart, const SliceEnd sliceEnd)
        {
            const auto createdSlices = createFunction(sliceStart, sliceEnd);
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

    const std::size_t numTuples = aggregationSlice->getHashMapPtrOrCreate(workerThreadId)->getNumberOfTuples();
    return aggregationSlice->getHashMapPtrOrCreate(workerThreadId);
}

void serializeHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    Timestamp timestamp,
    WorkerThreadId workerThreadId,
    [[maybe_unused]] AbstractBufferProvider* bufferProvider,
    const AggregationBuildPhysicalOperator* buildOperator)
{
    if (!CheckpointManager::isCheckpointingEnabled())
    {
        return;
    }
    auto* const hashMap = getAggHashMapProxy(operatorHandler, timestamp, workerThreadId, buildOperator);
    const auto serializationOptions = buildOperator->getHashMapOptions().getSerializationOptions();
    INVARIANT(
        !serializationOptions.valuesContainPagedVectors,
        "Aggregation checkpointing expects raw value serialization but received paged vectors");
    NES_INFO(
        "Aggregations serialize checkpoint | entries={} keySize={} valueSize={}",
        hashMap->getNumberOfTuples(),
        serializationOptions.keySize,
        serializationOptions.valueSize);
    const auto checkpointFile = getAggregationCheckpointFile();
    ensureCheckpointDirectory(checkpointFile);
    std::lock_guard lock(AggregationCheckpointMutex);
    std::ofstream out(checkpointFile, std::ios::binary | std::ios::trunc);
    if (!out.is_open())
    {
        throw CheckpointError("Cannot open aggregation checkpoint {}", checkpointFile.string());
    }
    hashMap->serialize(out, serializationOptions);
}

Interface::HashMap* deserializeHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    Timestamp timestamp,
    WorkerThreadId workerThreadId,
    [[maybe_unused]] AbstractBufferProvider* bufferProvider,
    const AggregationBuildPhysicalOperator* buildOperator)
{
    if (!CheckpointManager::shouldRecoverFromCheckpoint())
    {
        return nullptr;
    }
    bool expected = false;
    if (!operatorHandler->checkpointStateRestored.compare_exchange_strong(expected, true))
    {
        return nullptr;
    }
    auto* const hashMap = getAggHashMapProxy(operatorHandler, timestamp, workerThreadId, buildOperator);
    const auto checkpointFile = getAggregationCheckpointFile();
    if (!std::filesystem::exists(checkpointFile))
    {
        return hashMap;
    }
    ensureCheckpointDirectory(checkpointFile);
    std::lock_guard lock(AggregationCheckpointMutex);
    std::ifstream in(checkpointFile, std::ios::binary);
    if (!in.is_open())
    {
        throw CheckpointError("Cannot open aggregation checkpoint {}", checkpointFile.string());
    }
    const auto serializationOptions = buildOperator->getHashMapOptions().getSerializationOptions();
    INVARIANT(
        !serializationOptions.valuesContainPagedVectors,
        "Aggregation checkpointing expects raw value serialization but received paged vectors");
    hashMap->deserialize(in, serializationOptions, bufferProvider);
    NES_INFO("Aggregations deserialized checkpoint | entries={}"
             , hashMap->getNumberOfTuples());
    return hashMap;
}

void AggregationBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    WindowBuildPhysicalOperator::setup(executionCtx, compilationContext);

    /// Creating the cleanup function for the slice of current stream
    /// As the setup function does not get traced, we do not need to have any nautilus::invoke calls to jump to the C++ runtime
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    auto* const operatorHandler = dynamic_cast<AggregationOperatorHandler*>(executionCtx.getGlobalOperatorHandler(operatorHandlerId).value);
    operatorHandler->cleanupStateNautilusFunction
        = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(compilationContext.registerFunction(std::function(
            [copyOfHashMapOptions = hashMapOptions,
             copyOfAggregationFunctions = aggregationPhysicalFunctions](nautilus::val<Nautilus::Interface::HashMap*> hashMap)
            {
                const Interface::ChainedHashMapRef hashMapRef(
                    hashMap,
                    copyOfHashMapOptions.fieldKeys,
                    copyOfHashMapOptions.fieldValues,
                    copyOfHashMapOptions.entriesPerPage,
                    copyOfHashMapOptions.entrySize);
                for (const auto entry : hashMapRef)
                {
                    const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(
                        entry, hashMap, copyOfHashMapOptions.fieldKeys, copyOfHashMapOptions.fieldValues);
                    auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(copyOfAggregationFunctions))
                    {
                        aggFunction->cleanup(state);
                        state = state + aggFunction->getSizeOfStateInBytes();
                    }
                }
            })));

    if (CheckpointManager::isCheckpointingEnabled() && not operatorHandler->hasCheckpointCallback())
    {
        const auto callbackId = fmt::format(
            "aggregation_state_{}_{}",
            operatorHandlerId.getRawValue(),
            reinterpret_cast<uintptr_t>(operatorHandler));
        operatorHandler->setCheckpointCallbackId(callbackId);
        CheckpointManager::registerCallback(callbackId, [handler = operatorHandler]() { handler->requestCheckpoint(); });
    }


    /// NOLINTEND(performance-unnecessary-value-param)
}

void AggregationBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Getting the corresponding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);

    nautilus::invoke(
        deserializeHashMapProxy,
        operatorHandler,
        timestamp,
        ctx.workerThreadId,
        ctx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<const AggregationBuildPhysicalOperator*>(this));

    const auto hashMapPtr = invoke(
        getAggHashMapProxy, operatorHandler, timestamp, ctx.workerThreadId, nautilus::val<const AggregationBuildPhysicalOperator*>(this));
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
        aggFunction->lift(state, ctx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
    const auto checkpointRequested = invoke(
        +[](OperatorHandler* handler) -> bool
        {
            if (auto* aggHandler = dynamic_cast<AggregationOperatorHandler*>(handler))
            {
                return aggHandler->consumeCheckpointRequest();
            }
            return false;
        },
        operatorHandler);
    if (checkpointRequested)
    {
        nautilus::invoke(
            serializeHashMapProxy,
            operatorHandler,
            timestamp,
            ctx.workerThreadId,
            ctx.pipelineMemoryProvider.bufferProvider,
            nautilus::val<const AggregationBuildPhysicalOperator*>(this));
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
    NES_INFO(
        "AggregationBuildPhysicalOperator initialized (valuesContainPagedVectors={})",
        this->hashMapOptions.valuesContainPagedVectors);
}

}
