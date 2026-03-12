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

#include <Join/HashJoin/HJBuildPhysicalOperator.hpp>

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <fstream>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Join/HashJoin/HJOperatorHandler.hpp>
#include <Join/HashJoin/HJSlice.hpp>
#include <Join/StreamJoinBuildPhysicalOperator.hpp>
#include <Join/StreamJoinUtil.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Time/Timestamp.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapSlice.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <fmt/format.h>
#include <function.hpp>
#include <options.hpp>
#include <scope_guard.hpp>
#include <static.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
constexpr std::array<char, 8> HashJoinCheckpointMagic{'N', 'E', 'S', 'H', 'J', '0', '2', '\0'};

struct HashJoinWorkerCheckpointSnapshot
{
    WorkerThreadId workerThreadId;
    HashMap* hashMap;
};

struct HashJoinSliceCheckpointSnapshot
{
    std::shared_ptr<HJSlice> slice;
    std::vector<HashJoinWorkerCheckpointSnapshot> workerStates;
};

template <typename T>
void writeCheckpointValue(std::ostream& out, const T& value)
{
    out.write(reinterpret_cast<const char*>(&value), static_cast<std::streamsize>(sizeof(T)));
    if (!out)
    {
        throw CheckpointError("Failed to write hash join checkpoint");
    }
}

template <typename T>
T readCheckpointValue(std::istream& in)
{
    T value{};
    in.read(reinterpret_cast<char*>(&value), static_cast<std::streamsize>(sizeof(T)));
    if (!in)
    {
        throw CheckpointError("Failed to read hash join checkpoint");
    }
    return value;
}

void ensureCheckpointDirectory(const std::filesystem::path& checkpointFile)
{
    const auto checkpointDir = checkpointFile.parent_path();
    if (!checkpointDir.empty() && !std::filesystem::exists(checkpointDir))
    {
        std::filesystem::create_directories(checkpointDir);
    }
}

std::filesystem::path getHashJoinCheckpointFile(
    const std::filesystem::path& checkpointDirectory, OperatorHandlerId handlerId, JoinBuildSideType buildSide)
{
    auto baseDir = checkpointDirectory;
    baseDir /= fmt::format(
        "hash_join_checkpoint_{}_{}.bin",
        handlerId.getRawValue(),
        buildSide == JoinBuildSideType::Left ? "left" : "right");
    return baseDir;
}

void writeCheckpointHeader(std::ostream& out)
{
    out.write(HashJoinCheckpointMagic.data(), static_cast<std::streamsize>(HashJoinCheckpointMagic.size()));
    if (!out)
    {
        throw CheckpointError("Failed to write hash join checkpoint header");
    }
}

void validateCheckpointHeader(std::istream& in, const std::filesystem::path& checkpointFile)
{
    std::array<char, HashJoinCheckpointMagic.size()> actualMagic{};
    in.read(actualMagic.data(), static_cast<std::streamsize>(actualMagic.size()));
    if (!in)
    {
        throw CheckpointError("Failed to read hash join checkpoint header from {}", checkpointFile.string());
    }
    if (actualMagic != HashJoinCheckpointMagic)
    {
        throw CheckpointError("Unsupported hash join checkpoint format in {}", checkpointFile.string());
    }
}

void removeCheckpointFileIfPresent(const std::filesystem::path& checkpointFile)
{
    std::error_code ec;
    std::filesystem::remove(checkpointFile, ec);
    if (ec)
    {
        NES_WARNING("Failed to remove hash join checkpoint {}: {}", checkpointFile.string(), ec.message());
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
        throw CheckpointError("Cannot replace hash join checkpoint {}; {}", checkpointFile.string(), ec.message());
    }
}

void serializeHashJoinState(
    const HashMap* hashMap,
    const HJOperatorHandler* operatorHandler,
    const JoinBuildSideType buildSide,
    std::ostream& out)
{
    const auto* chainedHashMap = dynamic_cast<const ChainedHashMap*>(hashMap);
    if (!chainedHashMap)
    {
        throw CheckpointError("Hash join checkpoint only supports chained hash maps");
    }
    hashMap->serialize(out, operatorHandler->getHashMapOptions(buildSide).getSerializationOptions());
}

void deserializeHashJoinState(
    HashMap* hashMap,
    const HJOperatorHandler* operatorHandler,
    const JoinBuildSideType buildSide,
    AbstractBufferProvider* bufferProvider,
    std::istream& in)
{
    auto* chainedHashMap = dynamic_cast<ChainedHashMap*>(hashMap);
    if (!chainedHashMap)
    {
        throw CheckpointError("Hash join checkpoint only supports chained hash maps");
    }
    hashMap->deserialize(in, operatorHandler->getHashMapOptions(buildSide).getSerializationOptions(), bufferProvider);
}

CreateNewHashMapSliceArgs createHashJoinHashMapSliceArgs(const HJOperatorHandler* operatorHandler, const JoinBuildSideType buildSide)
{
    const auto& hashMapOptions = operatorHandler->getHashMapOptions(buildSide);
    return {
        operatorHandler->getNautilusCleanupExec(),
        hashMapOptions.keySize,
        hashMapOptions.valueSize,
        hashMapOptions.pageSize,
        hashMapOptions.numberOfBuckets};
}

std::shared_ptr<HJSlice>
getOrCreateHashJoinSlice(const HJOperatorHandler* operatorHandler, const SliceStart sliceStart, const SliceEnd sliceEnd, JoinBuildSideType buildSide)
{
    auto createFunction = operatorHandler->getCreateNewSlicesFunction(createHashJoinHashMapSliceArgs(operatorHandler, buildSide));
    const auto slices = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(
        sliceStart,
        [createFunction = std::move(createFunction), sliceStart, sliceEnd](const SliceStart assignedSliceStart, const SliceEnd assignedSliceEnd)
        {
            INVARIANT(
                assignedSliceStart == sliceStart && assignedSliceEnd == sliceEnd,
                "Recovered hash join slice {}-{} does not match assigned slice {}-{}",
                sliceStart,
                sliceEnd,
                assignedSliceStart,
                assignedSliceEnd);
            return createFunction(assignedSliceStart, assignedSliceEnd);
        });
    INVARIANT(slices.size() == 1, "We expect exactly one slice for recovered hash join state, but got {}", slices.size());
    const auto hjSlice = std::dynamic_pointer_cast<HJSlice>(slices[0]);
    INVARIANT(hjSlice != nullptr, "Recovered slice must be an HJSlice");
    return hjSlice;
}

std::vector<std::shared_ptr<HJSlice>> getActiveHashJoinSlices(const HJOperatorHandler* operatorHandler)
{
    std::vector<std::shared_ptr<HJSlice>> hashJoinSlices;
    for (const auto& slice : operatorHandler->getSliceAndWindowStore().getActiveSlices())
    {
        const auto hjSlice = std::dynamic_pointer_cast<HJSlice>(slice);
        INVARIANT(hjSlice != nullptr, "Hash join checkpointing expects hash join slices only");
        hashJoinSlices.emplace_back(hjSlice);
    }
    return hashJoinSlices;
}

[[noreturn]] void throwHashJoinRecoveryFailure(
    const HJOperatorHandler* operatorHandler,
    const std::filesystem::path& checkpointFile,
    const JoinBuildSideType buildSide,
    const std::string_view reason)
{
    operatorHandler->getSliceAndWindowStore().deleteState();
    throw CheckpointError(
        "Cannot recover hash join checkpoint {} for {} build side: {}. Replay fallback is unavailable because source replay "
        "storage may already have pruned checkpoint-covered buffers.",
        checkpointFile.string(),
        buildSide == JoinBuildSideType::Left ? "left" : "right",
        reason);
}

bool restoreHashJoinCheckpointState(
    const HJOperatorHandler* operatorHandler, const JoinBuildSideType buildSide, AbstractBufferProvider* bufferProvider)
{
    if (!CheckpointManager::shouldRecoverFromCheckpoint())
    {
        return true;
    }
    if (!operatorHandler->markCheckpointRestored(buildSide))
    {
        return true;
    }

    const auto checkpointFile = getHashJoinCheckpointFile(
        operatorHandler->getCheckpointRecoveryDirectory(), operatorHandler->getOperatorHandlerId(), buildSide);
    if (!std::filesystem::exists(checkpointFile))
    {
        return true;
    }

    try
    {
        std::ifstream in(checkpointFile, std::ios::binary);
        if (!in.is_open())
        {
            throw CheckpointError("Cannot open hash join checkpoint {}", checkpointFile.string());
        }

        validateCheckpointHeader(in, checkpointFile);
        const auto sliceCount = readCheckpointValue<uint64_t>(in);
        uint64_t totalMaps = 0;
        uint64_t totalEntries = 0;
        for (uint64_t sliceIdx = 0; sliceIdx < sliceCount; ++sliceIdx)
        {
            const auto sliceStart = SliceStart(readCheckpointValue<uint64_t>(in));
            const auto sliceEnd = SliceEnd(readCheckpointValue<uint64_t>(in));
            const auto workerStateCount = readCheckpointValue<uint64_t>(in);
            auto hjSlice = getOrCreateHashJoinSlice(operatorHandler, sliceStart, sliceEnd, buildSide);
            for (uint64_t workerStateIdx = 0; workerStateIdx < workerStateCount; ++workerStateIdx)
            {
                const auto workerThreadId = WorkerThreadId(readCheckpointValue<uint64_t>(in));
                auto* hashMap = hjSlice->getHashMapPtrOrCreate(workerThreadId, buildSide);
                deserializeHashJoinState(hashMap, operatorHandler, buildSide, bufferProvider, in);
                totalMaps += 1;
                totalEntries += hashMap->getNumberOfTuples();
            }
        }

        NES_INFO(
            "HashJoin deserialized checkpoint | side={} slices={} maps={} entries={}",
            buildSide == JoinBuildSideType::Left ? "left" : "right",
            sliceCount,
            totalMaps,
            totalEntries);
        return true;
    }
    catch (const Exception& exception)
    {
        throwHashJoinRecoveryFailure(operatorHandler, checkpointFile, buildSide, exception.what());
    }
    catch (const std::exception& exception)
    {
        throwHashJoinRecoveryFailure(operatorHandler, checkpointFile, buildSide, exception.what());
    }
}

void restoreHashJoinCheckpointStateProxy(OperatorHandler* ptrOpHandler, AbstractBufferProvider* bufferProvider)
{
    auto* operatorHandler = dynamic_cast<HJOperatorHandler*>(ptrOpHandler);
    if (operatorHandler == nullptr || bufferProvider == nullptr || !CheckpointManager::shouldRecoverFromCheckpoint())
    {
        return;
    }

    const auto cleanupFunctions = operatorHandler->getNautilusCleanupExec();
    const auto allCleanupFunctionsReady = std::all_of(
        cleanupFunctions.begin(), cleanupFunctions.end(), [](const auto& cleanupFunction) { return cleanupFunction != nullptr; });
    const auto allCheckpointStateRestored
        = operatorHandler->wasCheckpointRestored(JoinBuildSideType::Left)
        && operatorHandler->wasCheckpointRestored(JoinBuildSideType::Right);
    if (!allCleanupFunctionsReady || allCheckpointStateRestored)
    {
        return;
    }

    auto checkpointStateLock = operatorHandler->acquireCheckpointStateWriteLock();
    restoreHashJoinCheckpointState(operatorHandler, JoinBuildSideType::Left, bufferProvider);
    restoreHashJoinCheckpointState(operatorHandler, JoinBuildSideType::Right, bufferProvider);
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

HashMap* getHashJoinHashMapProxy(
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType buildSide)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");

    const auto& hashMapOptions = operatorHandler->getHashMapOptions(buildSide);
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        operatorHandler->getNautilusCleanupExec(),
        hashMapOptions.keySize,
        hashMapOptions.valueSize,
        hashMapOptions.pageSize,
        hashMapOptions.numberOfBuckets};
    const auto hashMap = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(
        timestamp, operatorHandler->getCreateNewSlicesFunction(hashMapSliceArgs));
    INVARIANT(
        hashMap.size() == 1,
        "We expect exactly one slice for the given timestamp during the HashJoinBuild, as we currently solely support "
        "slicing, but got {}",
        hashMap.size());

    /// Converting the slice to an HJSlice and returning the pointer to the hashmap
    const auto hjSlice = std::dynamic_pointer_cast<HJSlice>(hashMap[0]);
    INVARIANT(hjSlice != nullptr, "The slice should be an HJSlice in an HJBuildPhysicalOperator");
    return hjSlice->getHashMapPtrOrCreate(workerThreadId, buildSide);
}

void serializeHashMapProxy(
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType buildSide,
    [[maybe_unused]] AbstractBufferProvider* bufferProvider)
{
    if (!CheckpointManager::isCheckpointingEnabled())
    {
        return;
    }
    (void)timestamp;
    (void)workerThreadId;
    serializeHashJoinCheckpoint(operatorHandler, operatorHandler->getCheckpointDirectory(), buildSide, bufferProvider);
}

void serializeHashJoinCheckpoint(
    const HJOperatorHandler* operatorHandler,
    const std::filesystem::path& checkpointDirectory,
    const JoinBuildSideType buildSide,
    [[maybe_unused]] AbstractBufferProvider* bufferProvider)
{
    const auto checkpointFile = getHashJoinCheckpointFile(checkpointDirectory, operatorHandler->getOperatorHandlerId(), buildSide);
    std::vector<HashJoinSliceCheckpointSnapshot> checkpointSnapshots;
    for (const auto& hjSlice : getActiveHashJoinSlices(operatorHandler))
    {
        HashJoinSliceCheckpointSnapshot snapshot{hjSlice, {}};
        for (uint64_t idx = 0; idx < hjSlice->getNumberOfHashMapsForSide(); ++idx)
        {
            auto* const hashMap = hjSlice->getHashMapPtr(WorkerThreadId(idx), buildSide);
            if (hashMap == nullptr || hashMap->getNumberOfTuples() == 0)
            {
                continue;
            }
            snapshot.workerStates.push_back({WorkerThreadId(idx), hashMap});
        }
        if (!snapshot.workerStates.empty())
        {
            checkpointSnapshots.emplace_back(std::move(snapshot));
        }
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
        throw CheckpointError("Cannot open hash join checkpoint {}", tempCheckpointFile.string());
    }

    writeCheckpointHeader(out);
    writeCheckpointValue(out, static_cast<uint64_t>(checkpointSnapshots.size()));
    uint64_t totalMaps = 0;
    uint64_t totalEntries = 0;
    for (const auto& snapshot : checkpointSnapshots)
    {
        writeCheckpointValue(out, snapshot.slice->getSliceStart().getRawValue());
        writeCheckpointValue(out, snapshot.slice->getSliceEnd().getRawValue());
        writeCheckpointValue(out, static_cast<uint64_t>(snapshot.workerStates.size()));
        for (const auto& workerState : snapshot.workerStates)
        {
            writeCheckpointValue(out, workerState.workerThreadId.getRawValue());
            serializeHashJoinState(workerState.hashMap, operatorHandler, buildSide, out);
            totalMaps += 1;
            totalEntries += workerState.hashMap->getNumberOfTuples();
        }
    }
    out.flush();
    if (!out)
    {
        throw CheckpointError("Failed to flush hash join checkpoint {}", tempCheckpointFile.string());
    }
    out.close();
    if (!out)
    {
        throw CheckpointError("Failed to close hash join checkpoint {}", tempCheckpointFile.string());
    }
    replaceCheckpointFileAtomically(tempCheckpointFile, checkpointFile);

    NES_INFO(
        "HashJoin serialize checkpoint | side={} slices={} maps={} entries={}",
        buildSide == JoinBuildSideType::Left ? "left" : "right",
        checkpointSnapshots.size(),
        totalMaps,
        totalEntries);
}

HashMap* deserializeHashMapProxy(
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType buildSide,
    [[maybe_unused]] AbstractBufferProvider* bufferProvider)
{
    return getHashJoinHashMapProxy(operatorHandler, timestamp, workerThreadId, buildSide);
}


void HJBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    StreamJoinBuildPhysicalOperator::setup(executionCtx, compilationContext);

    /// Creating the cleanup function for the slice of current stream
    /// As the setup function does not get traced, we do not need to have any nautilus::invoke calls to jump to the C++ runtime
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    auto* const operatorHandler = dynamic_cast<HJOperatorHandler*>(
        nautilus::details::RawValueResolver<OperatorHandler*>::getRawValue(executionCtx.getGlobalOperatorHandler(operatorHandlerId)));
    if (operatorHandler->wasSetupCalled(joinBuildSide))
    {
        return;
    }

    const auto cleanupStateNautilusFunction
        = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(compilationContext.registerFunction(std::function(
            [copyOfHashMapOptions = hashMapOptions](nautilus::val<HashMap*> hashMap)
            {
                const ChainedHashMapRef hashMapRef{
                    hashMap,
                    copyOfHashMapOptions.fieldKeys,
                    copyOfHashMapOptions.fieldValues,
                    copyOfHashMapOptions.entriesPerPage,
                    copyOfHashMapOptions.entrySize};
                for (const auto entry : hashMapRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef entryRefReset{
                        entry, hashMap, copyOfHashMapOptions.fieldKeys, copyOfHashMapOptions.fieldValues};
                    const auto state = entryRefReset.getValueMemArea();
                    nautilus::invoke(
                        +[](int8_t* pagedVectorMemArea) -> void
                        {
                            /// Calls the destructor of the PagedVector
                            /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                            auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea);
                            pagedVector->~PagedVector();
                        },
                        state);
                }
            })));
    /// NOLINTEND(performance-unnecessary-value-param)
    operatorHandler->setNautilusCleanupExec(cleanupStateNautilusFunction, joinBuildSide);

    if (CheckpointManager::shouldRecoverFromCheckpoint())
    {
        auto* bufferProvider = nautilus::details::RawValueResolver<AbstractBufferProvider*>::getRawValue(
            executionCtx.pipelineMemoryProvider.bufferProvider);
        const auto cleanupFunctions = operatorHandler->getNautilusCleanupExec();
        const auto allCleanupFunctionsReady = std::all_of(
            cleanupFunctions.begin(), cleanupFunctions.end(), [](const auto& cleanupFunction) { return cleanupFunction != nullptr; });
        if (bufferProvider != nullptr && allCleanupFunctionsReady)
        {
            auto checkpointStateLock = operatorHandler->acquireCheckpointStateWriteLock();
            restoreHashJoinCheckpointState(operatorHandler, JoinBuildSideType::Left, bufferProvider);
            restoreHashJoinCheckpointState(operatorHandler, JoinBuildSideType::Right, bufferProvider);
        }
    }
}

void HJBuildPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    WindowBuildPhysicalOperator::open(executionCtx, recordBuffer);
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();
    nautilus::invoke(restoreHashJoinCheckpointStateProxy, operatorHandler, executionCtx.pipelineMemoryProvider.bufferProvider);
    nautilus::invoke(lockCheckpointStateSharedProxy, operatorHandler);
}

void HJBuildPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(executionCtx.getLocalState(id));
    const auto operatorHandler = localState->getOperatorHandler();
    SCOPE_EXIT { nautilus::invoke(unlockCheckpointStateSharedProxy, operatorHandler); };
    WindowBuildPhysicalOperator::close(executionCtx, recordBuffer);
}

void HJBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Get the current slice / hash map that we have to insert the tuple into
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr = invoke(
        getHashJoinHashMapProxy,
        operatorHandler,
        timestamp,
        ctx.workerThreadId,
        nautilus::val<JoinBuildSideType>(joinBuildSide));
    ChainedHashMapRef hashMap{
        hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize};

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
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the underyling paged vector
            const ChainedHashMapRef::ChainedEntryRef entryRefReset{entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
            const auto state = entryRefReset.getValueMemArea();
            nautilus::invoke(
                +[](int8_t* pagedVectorMemArea) -> void
                {
                    /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
                    /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                    auto* pagedVector = reinterpret_cast<PagedVector*>(pagedVectorMemArea);
                    new (pagedVector) PagedVector();
                },
                state);
        },
        ctx.pipelineMemoryProvider.bufferProvider);

    /// Inserting the tuple into the corresponding hash entry
    const ChainedHashMapRef::ChainedEntryRef entryRef{hashMapEntry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
    auto entryMemArea = entryRef.getValueMemArea();
    const PagedVectorRef pagedVectorRef(entryMemArea, bufferRef);
    pagedVectorRef.writeRecord(record, ctx.pipelineMemoryProvider.bufferProvider);
}

void HJBuildPhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    if (CheckpointManager::isCheckpointingEnabled())
    {
        auto* const operatorHandler = dynamic_cast<HJOperatorHandler*>(
            nautilus::details::RawValueResolver<OperatorHandler*>::getRawValue(executionCtx.getGlobalOperatorHandler(operatorHandlerId)));
        auto* bufferProvider = nautilus::details::RawValueResolver<AbstractBufferProvider*>::getRawValue(
            executionCtx.pipelineMemoryProvider.bufferProvider);
        if (operatorHandler != nullptr && bufferProvider != nullptr)
        {
            auto checkpointStateLock = operatorHandler->acquireCheckpointStateWriteLock();
            serializeHashJoinCheckpoint(operatorHandler, operatorHandler->getCheckpointDirectory(), joinBuildSide, bufferProvider);
        }
    }
    WindowBuildPhysicalOperator::terminate(executionCtx);
}

HJBuildPhysicalOperator::HJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<TupleBufferRef>& bufferRef,
    HashMapOptions hashMapOptions)
    : StreamJoinBuildPhysicalOperator(operatorHandlerId, joinBuildSide, std::move(timeFunction), bufferRef)
    , hashMapOptions(std::move(hashMapOptions))
{
}

}
