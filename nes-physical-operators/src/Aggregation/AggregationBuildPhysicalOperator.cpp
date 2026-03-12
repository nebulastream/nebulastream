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

#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <mutex>
#include <string_view>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <fmt/format.h>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
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
#include <scope_guard.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
constexpr std::string_view AggregationCheckpointFileNamePrefix = "aggregation_hashmap";
constexpr std::array<char, 8> AggregationCheckpointMagic{'N', 'E', 'S', 'A', 'G', 'G', '2', '\0'};
std::mutex AggregationCheckpointMutex;

struct AggregationSliceCheckpointSnapshot
{
    std::shared_ptr<AggregationSlice> slice;
    std::unique_ptr<HashMap> hashMap;
};

template <typename T>
void writeCheckpointValue(std::ostream& out, const T& value)
{
    out.write(reinterpret_cast<const char*>(&value), static_cast<std::streamsize>(sizeof(T)));
    if (!out)
    {
        throw CheckpointError("Failed to write aggregation checkpoint");
    }
}

template <typename T>
T readCheckpointValue(std::istream& in)
{
    T value{};
    in.read(reinterpret_cast<char*>(&value), static_cast<std::streamsize>(sizeof(T)));
    if (!in)
    {
        throw CheckpointError("Failed to read aggregation checkpoint");
    }
    return value;
}

PipelineMemoryProvider createPipelineMemoryProvider(AbstractBufferProvider* bufferProvider)
{
    // We only need the buffer provider for merging hash map entries during checkpointing.
    // An empty arena is sufficient because the aggregation functions do not allocate from it when combining states.
    return PipelineMemoryProvider(nautilus::val<Arena*>{nullptr}, nautilus::val<AbstractBufferProvider*>{bufferProvider});
}

std::filesystem::path getAggregationCheckpointFile(const std::filesystem::path& checkpointDirectory, const OperatorHandlerId handlerId)
{
    auto baseDir = checkpointDirectory;
    baseDir /= fmt::format("{}_{}.bin", AggregationCheckpointFileNamePrefix, handlerId.getRawValue());
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

void writeCheckpointHeader(std::ostream& out)
{
    out.write(AggregationCheckpointMagic.data(), static_cast<std::streamsize>(AggregationCheckpointMagic.size()));
    if (!out)
    {
        throw CheckpointError("Failed to write aggregation checkpoint header");
    }
}

void validateCheckpointHeader(std::istream& in, const std::filesystem::path& checkpointFile)
{
    std::array<char, AggregationCheckpointMagic.size()> actualMagic{};
    in.read(actualMagic.data(), static_cast<std::streamsize>(actualMagic.size()));
    if (!in)
    {
        throw CheckpointError("Failed to read aggregation checkpoint header from {}", checkpointFile.string());
    }
    if (actualMagic != AggregationCheckpointMagic)
    {
        throw CheckpointError("Unsupported aggregation checkpoint format in {}", checkpointFile.string());
    }
}

void removeCheckpointFileIfPresent(const std::filesystem::path& checkpointFile)
{
    std::error_code ec;
    std::filesystem::remove(checkpointFile, ec);
    if (ec)
    {
        NES_WARNING("Failed to remove aggregation checkpoint {}: {}", checkpointFile.string(), ec.message());
    }
}

std::filesystem::path getTempCheckpointFile(const std::filesystem::path& checkpointFile)
{
    return checkpointFile.string() + ".tmp";
}

void replaceCheckpointFileAtomically(const std::filesystem::path& tempCheckpointFile, const std::filesystem::path& checkpointFile)
{
    std::error_code ec;
    std::filesystem::rename(tempCheckpointFile, checkpointFile, ec);
    if (ec)
    {
        std::filesystem::remove(tempCheckpointFile, ec);
        throw CheckpointError("Cannot replace aggregation checkpoint {}; {}", checkpointFile.string(), ec.message());
    }
}

CreateNewHashMapSliceArgs createAggregationHashMapSliceArgs(const AggregationOperatorHandler* operatorHandler)
{
    const auto& hashMapOptions = operatorHandler->getHashMapOptions();
    return {
        {operatorHandler->cleanupStateNautilusFunction},
        hashMapOptions.keySize,
        hashMapOptions.valueSize,
        hashMapOptions.pageSize,
        hashMapOptions.numberOfBuckets};
}

std::shared_ptr<AggregationSlice>
getOrCreateAggregationSlice(const AggregationOperatorHandler* operatorHandler, const SliceStart sliceStart, const SliceEnd sliceEnd)
{
    auto createFunction = operatorHandler->getCreateNewSlicesFunction(createAggregationHashMapSliceArgs(operatorHandler));
    auto slices = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(
        sliceStart,
        [createFunction = std::move(createFunction), sliceStart, sliceEnd](const SliceStart assignedSliceStart, const SliceEnd assignedSliceEnd)
        {
            INVARIANT(
                assignedSliceStart == sliceStart && assignedSliceEnd == sliceEnd,
                "Recovered aggregation slice {}-{} does not match assigned slice {}-{}",
                sliceStart,
                sliceEnd,
                assignedSliceStart,
                assignedSliceEnd);
            return createFunction(assignedSliceStart, assignedSliceEnd);
        });
    INVARIANT(
        slices.size() == 1,
        "We expect exactly one slice for recovered aggregation state, but got {}",
        slices.size());
    const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(slices[0]);
    INVARIANT(aggregationSlice != nullptr, "Recovered slice must be an AggregationSlice");
    return aggregationSlice;
}

std::vector<std::shared_ptr<AggregationSlice>> getActiveAggregationSlices(const AggregationOperatorHandler* operatorHandler)
{
    std::vector<std::shared_ptr<AggregationSlice>> aggregationSlices;
    for (const auto& slice : operatorHandler->getSliceAndWindowStore().getActiveSlices())
    {
        const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(slice);
        INVARIANT(aggregationSlice != nullptr, "Aggregation checkpointing expects aggregation slices only");
        aggregationSlices.emplace_back(aggregationSlice);
    }
    return aggregationSlices;
}

[[noreturn]] void throwAggregationRecoveryFailure(
    const AggregationOperatorHandler* operatorHandler, const std::filesystem::path& checkpointFile, const std::string_view reason)
{
    operatorHandler->getSliceAndWindowStore().deleteState();
    throw CheckpointError(
        "Cannot recover aggregation checkpoint {}: {}. Replay fallback is unavailable because source replay storage may already "
        "have pruned checkpoint-covered buffers.",
        checkpointFile.string(),
        reason);
}

bool restoreAggregationCheckpointState(
    const AggregationOperatorHandler* operatorHandler, AbstractBufferProvider* bufferProvider, const WorkerThreadId targetWorkerThreadId)
{
    if (!CheckpointManager::shouldRecoverFromCheckpoint())
    {
        return true;
    }

    bool expected = false;
    if (!operatorHandler->checkpointStateRestored.compare_exchange_strong(expected, true))
    {
        return true;
    }

    const auto checkpointFile = getAggregationCheckpointFile(
        operatorHandler->getCheckpointRecoveryDirectory(), operatorHandler->getOperatorHandlerId());
    if (!std::filesystem::exists(checkpointFile))
    {
        return true;
    }

    try
    {
        const auto serializationOptions = operatorHandler->getHashMapOptions().getSerializationOptions();
        INVARIANT(
            !serializationOptions.valuesContainPagedVectors,
            "Aggregation checkpointing expects raw value serialization but received paged vectors");

        std::lock_guard lock(AggregationCheckpointMutex);
        std::ifstream in(checkpointFile, std::ios::binary);
        if (!in.is_open())
        {
            throw CheckpointError("Cannot open aggregation checkpoint {}", checkpointFile.string());
        }

        validateCheckpointHeader(in, checkpointFile);
        const auto sliceCount = readCheckpointValue<uint64_t>(in);
        uint64_t totalEntries = 0;
        for (uint64_t sliceIdx = 0; sliceIdx < sliceCount; ++sliceIdx)
        {
            const auto sliceStart = SliceStart(readCheckpointValue<uint64_t>(in));
            const auto sliceEnd = SliceEnd(readCheckpointValue<uint64_t>(in));
            auto aggregationSlice = getOrCreateAggregationSlice(operatorHandler, sliceStart, sliceEnd);
            auto* localHashMap = aggregationSlice->getHashMapPtrOrCreate(targetWorkerThreadId);
            localHashMap->deserialize(in, serializationOptions, bufferProvider);
            totalEntries += localHashMap->getNumberOfTuples();
        }

        NES_INFO("Aggregations deserialized checkpoint | slices={} entries={}", sliceCount, totalEntries);
        return true;
    }
    catch (const Exception& exception)
    {
        throwAggregationRecoveryFailure(operatorHandler, checkpointFile, exception.what());
    }
    catch (const std::exception& exception)
    {
        throwAggregationRecoveryFailure(operatorHandler, checkpointFile, exception.what());
    }
}

void restoreAggregationCheckpointStateProxy(
    OperatorHandler* ptrOpHandler, AbstractBufferProvider* bufferProvider, const WorkerThreadId workerThreadId)
{
    auto* operatorHandler = dynamic_cast<AggregationOperatorHandler*>(ptrOpHandler);
    if (operatorHandler == nullptr || bufferProvider == nullptr || !CheckpointManager::shouldRecoverFromCheckpoint()
        || operatorHandler->checkpointStateRestored.load(std::memory_order_acquire))
    {
        return;
    }

    auto checkpointStateLock = operatorHandler->acquireCheckpointStateWriteLock();
    if (!operatorHandler->checkpointStateRestored.load(std::memory_order_acquire))
    {
        restoreAggregationCheckpointState(operatorHandler, bufferProvider, workerThreadId);
    }
}

void lockCheckpointStateSharedProxy(OperatorHandler* ptrOpHandler)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    if (!CheckpointManager::isCheckpointingEnabled() && !CheckpointManager::shouldRecoverFromCheckpoint())
    {
        return;
    }
    ptrOpHandler->lockCheckpointStateShared();
}

void unlockCheckpointStateSharedProxy(OperatorHandler* ptrOpHandler)
{
    PRECONDITION(ptrOpHandler != nullptr, "opHandler context should not be null!");
    if (!CheckpointManager::isCheckpointingEnabled() && !CheckpointManager::shouldRecoverFromCheckpoint())
    {
        return;
    }
    ptrOpHandler->unlockCheckpointStateShared();
}
}

HashMap* getAggHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");

    /// If a new hashmap slice is created, we need to set the cleanup function for the aggregation states
    const auto& hashMapOptions = operatorHandler->getHashMapOptions();
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        {operatorHandler->cleanupStateNautilusFunction},
        hashMapOptions.keySize,
        hashMapOptions.valueSize,
        hashMapOptions.pageSize,
        hashMapOptions.numberOfBuckets};
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

std::unique_ptr<HashMap> mergeHashMapsForCheckpoint(
    const AggregationOperatorHandler* operatorHandler,
    const AggregationSlice& aggregationSlice,
    AbstractBufferProvider* bufferProvider)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");

    const auto& hashMapOptions = operatorHandler->getHashMapOptions();
    const auto& aggregationPhysicalFunctions = operatorHandler->getAggregationFunctions();

    auto pipelineMemoryProvider = createPipelineMemoryProvider(bufferProvider);

    HashMap* referenceMap = nullptr;
    for (uint64_t mapIdx = 0; mapIdx < aggregationSlice.getNumberOfHashMaps(); ++mapIdx)
    {
        referenceMap = aggregationSlice.getHashMapPtr(WorkerThreadId(mapIdx));
        if (referenceMap != nullptr)
        {
            break;
        }
    }
    if (referenceMap == nullptr)
    {
        return nullptr;
    }

    auto* chainedReference = dynamic_cast<ChainedHashMap*>(referenceMap);
    INVARIANT(chainedReference != nullptr, "Aggregation checkpoints expect a chained hash map implementation");
    auto snapshot = ChainedHashMap::createNewMapWithSameConfiguration(*chainedReference);

    auto snapshotMapVal = nautilus::val<HashMap*>{snapshot.get()};
    ChainedHashMapRef snapshotRef(
        snapshotMapVal,
        hashMapOptions.fieldKeys,
        hashMapOptions.fieldValues,
        nautilus::val<uint64_t>{hashMapOptions.entriesPerPage},
        nautilus::val<uint64_t>{hashMapOptions.entrySize});

    for (uint64_t mapIdx = 0; mapIdx < aggregationSlice.getNumberOfHashMaps(); ++mapIdx)
    {
        auto* sourceMap = aggregationSlice.getHashMapPtr(WorkerThreadId(mapIdx));
        if ((sourceMap == nullptr) || sourceMap->getNumberOfTuples() == 0)
        {
            continue;
        }

        ChainedHashMapRef sourceRef(
            nautilus::val<HashMap*>{sourceMap},
            hashMapOptions.fieldKeys,
            hashMapOptions.fieldValues,
            nautilus::val<uint64_t>{hashMapOptions.entriesPerPage},
            nautilus::val<uint64_t>{hashMapOptions.entrySize});

        for (const auto entry : sourceRef)
        {
            const ChainedHashMapRef::ChainedEntryRef entryRef(
                entry,
                nautilus::val<HashMap*>{sourceMap},
                hashMapOptions.fieldKeys,
                hashMapOptions.fieldValues);
            snapshotRef.insertOrUpdateEntry(
                entryRef.entryRef,
                [fieldKeys = hashMapOptions.fieldKeys,
                 fieldValues = hashMapOptions.fieldValues,
                 &pipelineMemoryProvider,
                 entryRef,
                 &aggregationPhysicalFunctions,
                 snapshotMapVal](const nautilus::val<AbstractHashMapEntry*>& entryOnUpdate)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(
                        entryOnUpdate, snapshotMapVal, fieldKeys, fieldValues);
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        aggFunction->combine(globalState, entryRefState, pipelineMemoryProvider);
                        globalState = globalState + aggFunction->getSizeOfStateInBytes();
                        entryRefState = entryRefState + aggFunction->getSizeOfStateInBytes();
                    }
                },
                [fieldKeys = hashMapOptions.fieldKeys,
                 fieldValues = hashMapOptions.fieldValues,
                 &pipelineMemoryProvider,
                 entryRef,
                 &aggregationPhysicalFunctions,
                 snapshotMapVal](const nautilus::val<AbstractHashMapEntry*>& entryOnInsert)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRefOnInsert(
                        entryOnInsert, snapshotMapVal, fieldKeys, fieldValues);
                    auto globalState = static_cast<nautilus::val<AggregationState*>>(entryRefOnInsert.getValueMemArea());
                    auto entryRefState = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
                    {
                        aggFunction->reset(globalState, pipelineMemoryProvider);
                        aggFunction->combine(globalState, entryRefState, pipelineMemoryProvider);
                        globalState = globalState + aggFunction->getSizeOfStateInBytes();
                        entryRefState = entryRefState + aggFunction->getSizeOfStateInBytes();
                    }
                },
                pipelineMemoryProvider.bufferProvider);
        }
    }

    return snapshot;
}

void serializeAggregationCheckpoint(
    const AggregationOperatorHandler* operatorHandler,
    const std::filesystem::path& checkpointDirectory,
    [[maybe_unused]] AbstractBufferProvider* bufferProvider)
{
    std::lock_guard lock(AggregationCheckpointMutex);
    const auto serializationOptions = operatorHandler->getHashMapOptions().getSerializationOptions();
    INVARIANT(
        !serializationOptions.valuesContainPagedVectors,
        "Aggregation checkpointing expects raw value serialization but received paged vectors");
    const auto checkpointFile = getAggregationCheckpointFile(checkpointDirectory, operatorHandler->getOperatorHandlerId());
    std::vector<AggregationSliceCheckpointSnapshot> checkpointSnapshots;
    checkpointSnapshots.reserve(getActiveAggregationSlices(operatorHandler).size());
    for (const auto& aggregationSlice : getActiveAggregationSlices(operatorHandler))
    {
        auto combinedHashMap = mergeHashMapsForCheckpoint(operatorHandler, *aggregationSlice, bufferProvider);
        if (!combinedHashMap)
        {
            continue;
        }
        checkpointSnapshots.push_back({aggregationSlice, std::move(combinedHashMap)});
    }

    if (checkpointSnapshots.empty())
    {
        removeCheckpointFileIfPresent(checkpointFile);
        return;
    }

    ensureCheckpointDirectory(checkpointFile);
    const auto tempCheckpointFile = getTempCheckpointFile(checkpointFile);
    std::ofstream out(tempCheckpointFile, std::ios::binary | std::ios::trunc);
    if (!out.is_open())
    {
        throw CheckpointError("Cannot open aggregation checkpoint {}", tempCheckpointFile.string());
    }

    writeCheckpointHeader(out);
    writeCheckpointValue(out, static_cast<uint64_t>(checkpointSnapshots.size()));
    uint64_t totalEntries = 0;
    for (const auto& checkpointSnapshot : checkpointSnapshots)
    {
        writeCheckpointValue(out, checkpointSnapshot.slice->getSliceStart().getRawValue());
        writeCheckpointValue(out, checkpointSnapshot.slice->getSliceEnd().getRawValue());
        checkpointSnapshot.hashMap->serialize(out, serializationOptions);
        totalEntries += checkpointSnapshot.hashMap->getNumberOfTuples();
    }
    out.flush();
    if (!out)
    {
        throw CheckpointError("Failed to flush aggregation checkpoint {}", tempCheckpointFile.string());
    }
    out.close();
    if (!out)
    {
        throw CheckpointError("Failed to close aggregation checkpoint {}", tempCheckpointFile.string());
    }
    replaceCheckpointFileAtomically(tempCheckpointFile, checkpointFile);

    NES_INFO(
        "Aggregations serialize checkpoint | slices={} entries={} keySize={} valueSize={}",
        checkpointSnapshots.size(),
        totalEntries,
        serializationOptions.keySize,
        serializationOptions.valueSize);
}

HashMap* deserializeHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    Timestamp timestamp,
    WorkerThreadId workerThreadId,
    [[maybe_unused]] AbstractBufferProvider* bufferProvider)
{
    return getAggHashMapProxy(operatorHandler, timestamp, workerThreadId);
}

void AggregationBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    WindowBuildPhysicalOperator::setup(executionCtx, compilationContext);

    /// Creating the cleanup function for the slice of current stream
    /// As the setup function does not get traced, we do not need to have any nautilus::invoke calls to jump to the C++ runtime
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    auto* const operatorHandler = dynamic_cast<AggregationOperatorHandler*>(
        nautilus::details::RawValueResolver<OperatorHandler*>::getRawValue(executionCtx.getGlobalOperatorHandler(operatorHandlerId)));
    operatorHandler->cleanupStateNautilusFunction
        = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(compilationContext.registerFunction(std::function(
            [copyOfHashMapOptions = hashMapOptions,
             copyOfAggregationFunctions = aggregationPhysicalFunctions](nautilus::val<HashMap*> hashMap)
            {
                const ChainedHashMapRef hashMapRef(
                    hashMap,
                    copyOfHashMapOptions.fieldKeys,
                    copyOfHashMapOptions.fieldValues,
                    copyOfHashMapOptions.entriesPerPage,
                    copyOfHashMapOptions.entrySize);
                for (const auto entry : hashMapRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRefReset(
                        entry, hashMap, copyOfHashMapOptions.fieldKeys, copyOfHashMapOptions.fieldValues);
                    auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
                    for (const auto& aggFunction : nautilus::static_iterable(copyOfAggregationFunctions))
                    {
                        aggFunction->cleanup(state);
                        state = state + aggFunction->getSizeOfStateInBytes();
                    }
                }
            })));

    /// NOLINTEND(performance-unnecessary-value-param)

    if (CheckpointManager::shouldRecoverFromCheckpoint())
    {
        auto* bufferProvider = nautilus::details::RawValueResolver<AbstractBufferProvider*>::getRawValue(
            executionCtx.pipelineMemoryProvider.bufferProvider);
        if (bufferProvider != nullptr)
        {
            auto checkpointStateLock = operatorHandler->acquireCheckpointStateWriteLock();
            restoreAggregationCheckpointState(operatorHandler, bufferProvider, WorkerThreadId(0));
        }
    }
}

void AggregationBuildPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    WindowBuildPhysicalOperator::open(executionCtx, recordBuffer);
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();
    nautilus::invoke(
        restoreAggregationCheckpointStateProxy,
        operatorHandler,
        executionCtx.pipelineMemoryProvider.bufferProvider,
        executionCtx.workerThreadId);
    nautilus::invoke(lockCheckpointStateSharedProxy, operatorHandler);
}

void AggregationBuildPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    const auto operatorHandler = localState->getOperatorHandler();
    SCOPE_EXIT { nautilus::invoke(unlockCheckpointStateSharedProxy, operatorHandler); };
    WindowBuildPhysicalOperator::close(executionCtx, recordBuffer);
}

void AggregationBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Getting the corresponding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);

    const auto hashMapPtr = invoke(
        getAggHashMapProxy,
        operatorHandler,
        timestamp,
        ctx.workerThreadId);
    ChainedHashMapRef hashMap(
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
        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const ChainedHashMapRef::ChainedEntryRef entryRefReset(entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                aggFunction->reset(state, ctx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.pipelineMemoryProvider.bufferProvider);


    /// Updating the aggregation states
    const ChainedHashMapRef::ChainedEntryRef entryRef(hashMapEntry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
    auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
    {
        aggFunction->lift(state, ctx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

void AggregationBuildPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    if (CheckpointManager::isCheckpointingEnabled())
    {
        auto* const operatorHandler = dynamic_cast<AggregationOperatorHandler*>(
            nautilus::details::RawValueResolver<OperatorHandler*>::getRawValue(executionCtx.getGlobalOperatorHandler(operatorHandlerId)));
        auto* bufferProvider = nautilus::details::RawValueResolver<AbstractBufferProvider*>::getRawValue(
            executionCtx.pipelineMemoryProvider.bufferProvider);
        if (operatorHandler != nullptr && bufferProvider != nullptr)
        {
            auto checkpointStateLock = operatorHandler->acquireCheckpointStateWriteLock();
            serializeAggregationCheckpoint(operatorHandler, operatorHandler->getCheckpointDirectory(), bufferProvider);
        }
    }
    WindowBuildPhysicalOperator::terminate(executionCtx);
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
