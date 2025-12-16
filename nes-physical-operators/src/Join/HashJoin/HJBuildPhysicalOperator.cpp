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

#include <cstdint>
#include <filesystem>
#include <functional>
#include <fstream>
#include <memory>
#include <optional>
#include <regex>
#include <sstream>
#include <cstring>
#include <string_view>
#include <utility>
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
#include <static.hpp>
#include <val_enum.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
void ensureCheckpointDirectory(const std::filesystem::path& checkpointFile)
{
    const auto checkpointDir = checkpointFile.parent_path();
    if (!checkpointDir.empty() && !exists(checkpointDir))
    {
        create_directories(checkpointDir);
    }
}

std::filesystem::path getHashJoinCheckpointFile(OperatorHandlerId handlerId, JoinBuildSideType buildSide, WorkerThreadId workerId)
{
    auto baseDir = CheckpointManager::getCheckpointDirectory();
    baseDir /= fmt::format(
        "hash_join_checkpoint_{}_{}_{}.bin",
        handlerId.getRawValue(),
        buildSide == JoinBuildSideType::Left ? "left" : "right",
        workerId.getRawValue());
    return baseDir;
}

std::optional<std::filesystem::path> findLatestCheckpointForSide(JoinBuildSideType buildSide, std::optional<WorkerThreadId> workerId)
{
    const auto checkpointDir = CheckpointManager::getCheckpointDirectory();
    if (!std::filesystem::exists(checkpointDir) || !std::filesystem::is_directory(checkpointDir))
    {
        return std::nullopt;
    }

    /// If a workerId is provided we first try the per-worker naming scheme, otherwise we fall back to the legacy
    /// per-side file names (without the worker suffix).
    const auto sideStr = buildSide == JoinBuildSideType::Left ? "left" : "right";
    std::vector<std::regex> patterns;
    if (workerId.has_value())
    {
        patterns.emplace_back(
            fmt::format(R"(hash_join_checkpoint_.*_{}_{}\.bin)", sideStr, workerId->getRawValue()));
    }
    patterns.emplace_back(fmt::format(R"(hash_join_checkpoint_.*_{}\.bin)", sideStr));

    std::optional<std::filesystem::path> newestFile;
    std::optional<std::filesystem::file_time_type> newestTime;
    std::error_code ec;
    for (std::filesystem::directory_iterator it(checkpointDir, ec); !ec && it != std::filesystem::directory_iterator(); ++it)
    {
        const auto& entry = *it;
        if (!entry.is_regular_file())
        {
            continue;
        }
        const auto filename = entry.path().filename().string();
        const bool matches = std::any_of(
            patterns.begin(), patterns.end(), [&](const auto& pattern) { return std::regex_match(filename, pattern); });
        if (!matches)
        {
            continue;
        }
        const auto writeTime = entry.last_write_time();
        if (!newestTime || writeTime > *newestTime)
        {
            newestTime = writeTime;
            newestFile = entry.path();
        }
    }
    if (ec)
    {
        NES_WARNING("Failed to inspect checkpoint directory {}: {}", checkpointDir.string(), ec.message());
    }
    return newestFile;
}

void serializeHashJoinState(
    const Interface::HashMap* hashMap,
    const HJBuildPhysicalOperator* buildOperator,
    const std::filesystem::path& path)
{
    const auto* chainedHashMap = dynamic_cast<const Interface::ChainedHashMap*>(hashMap);
    if (!chainedHashMap)
    {
        throw CheckpointError("Hash join checkpoint only supports chained hash maps");
    }

    std::ofstream out(path, std::ios::binary);
    if (!out.is_open())
    {
        throw CheckpointError("Cannot open output file {}", path.string());
    }
    hashMap->serialize(out, buildOperator->getHashMapOptions().getSerializationOptions());
}

void deserializeHashJoinState(
    Interface::HashMap* hashMap,
    const HJBuildPhysicalOperator* buildOperator,
    AbstractBufferProvider* bufferProvider,
    const std::filesystem::path& path)
{
    auto* chainedHashMap = dynamic_cast<Interface::ChainedHashMap*>(hashMap);
    if (!chainedHashMap)
    {
        throw CheckpointError("Hash join checkpoint only supports chained hash maps");
    }

    std::ifstream in(path, std::ios::binary);
    if (!in.is_open())
    {
        throw CheckpointError("Cannot open input file {}", path.string());
    }
    hashMap->deserialize(in, buildOperator->getHashMapOptions().getSerializationOptions(), bufferProvider);
}

// Removed merge-based checkpointing; we now serialize each worker's hash map independently.
}

Interface::HashMap* getHashJoinHashMapProxy(
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType buildSide,
    const HJBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        operatorHandler->getNautilusCleanupExec(),
        buildOperator->hashMapOptions.keySize,
        buildOperator->hashMapOptions.valueSize,
        buildOperator->hashMapOptions.pageSize,
        buildOperator->hashMapOptions.numberOfBuckets};
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
    [[maybe_unused]] AbstractBufferProvider* bufferProvider,
    const HJBuildPhysicalOperator* buildOperator)
{
    if (!CheckpointManager::isCheckpointingEnabled())
    {
        return;
    }
    (void)workerThreadId;
    if (bufferProvider == nullptr)
    {
        NES_WARNING("HashJoin checkpoint serialization skipped: buffer provider is null");
        return;
    }

    const auto& hashMapOptions = buildOperator->getHashMapOptions();
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        operatorHandler->getNautilusCleanupExec(),
        hashMapOptions.keySize,
        hashMapOptions.valueSize,
        hashMapOptions.pageSize,
        hashMapOptions.numberOfBuckets};
    const auto slices = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(
        timestamp, operatorHandler->getCreateNewSlicesFunction(hashMapSliceArgs));
    INVARIANT(
        slices.size() == 1,
        "We expect exactly one slice for the given timestamp during the HashJoinBuild, as we currently solely support slicing, but got {}",
        slices.size());
    const auto hjSlice = std::dynamic_pointer_cast<HJSlice>(slices[0]);
    INVARIANT(hjSlice != nullptr, "The slice should be an HJSlice in an HJBuildPhysicalOperator");

    bool wroteAny = false;
    for (uint64_t idx = 0; idx < hjSlice->getNumberOfHashMapsForSide(); ++idx)
    {
        auto* const hashMap = hjSlice->getHashMapPtr(WorkerThreadId(idx), buildSide);
        if (hashMap == nullptr || hashMap->getNumberOfTuples() == 0)
        {
            continue;
        }
        const auto checkpointFile = getHashJoinCheckpointFile(buildOperator->operatorHandlerId, buildSide, WorkerThreadId(idx));
        ensureCheckpointDirectory(checkpointFile);
        serializeHashJoinState(hashMap, buildOperator, checkpointFile);
        wroteAny = true;
    }
    if (!wroteAny)
    {
        NES_DEBUG(
            "HashJoin serialize checkpoint skipped (empty) side={} ts={}",
            buildSide == JoinBuildSideType::Left ? "left" : "right",
            timestamp.getRawValue());
    }
}

Interface::HashMap* deserializeHashMapProxy(
    const HJOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const JoinBuildSideType buildSide,
    AbstractBufferProvider* bufferProvider,
    const HJBuildPhysicalOperator* buildOperator)
{
    if (!CheckpointManager::shouldRecoverFromCheckpoint())
    {
        return nullptr;
    }
    if (!operatorHandler->markCheckpointRestored(buildSide))
    {
        return nullptr;
    }

    const auto& hashMapOptions = buildOperator->getHashMapOptions();
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        operatorHandler->getNautilusCleanupExec(),
        hashMapOptions.keySize,
        hashMapOptions.valueSize,
        hashMapOptions.pageSize,
        hashMapOptions.numberOfBuckets};
    const auto slices = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(
        timestamp, operatorHandler->getCreateNewSlicesFunction(hashMapSliceArgs));
    INVARIANT(
        slices.size() == 1,
        "We expect exactly one slice for the given timestamp during the HashJoinBuild, as we currently solely support slicing, but got {}",
        slices.size());
    const auto hjSlice = std::dynamic_pointer_cast<HJSlice>(slices[0]);
    INVARIANT(hjSlice != nullptr, "The slice should be an HJSlice in an HJBuildPhysicalOperator");

    bool loaded = false;
    for (uint64_t idx = 0; idx < hjSlice->getNumberOfHashMapsForSide(); ++idx)
    {
        auto* const hashMap = hjSlice->getHashMapPtrOrCreate(WorkerThreadId(idx), buildSide);
        auto checkpointFile = getHashJoinCheckpointFile(buildOperator->operatorHandlerId, buildSide, WorkerThreadId(idx));
        if (!std::filesystem::exists(checkpointFile))
        {
            if (auto latest = findLatestCheckpointForSide(buildSide, WorkerThreadId(idx)))
            {
                checkpointFile = *latest;
            }
            else
            {
                continue;
            }
        }
        ensureCheckpointDirectory(checkpointFile);
        deserializeHashJoinState(hashMap, buildOperator, bufferProvider, checkpointFile);
        loaded = true;
    }

    if (!loaded)
    {
        NES_DEBUG(
            "HashJoin deserialize: no checkpoint files for side {}, returning empty map",
            buildSide == JoinBuildSideType::Left ? "left" : "right");
    }
    return getHashJoinHashMapProxy(operatorHandler, timestamp, workerThreadId, buildSide, buildOperator);
}


void HJBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    StreamJoinBuildPhysicalOperator::setup(executionCtx, compilationContext);

    /// Creating the cleanup function for the slice of current stream
    /// As the setup function does not get traced, we do not need to have any nautilus::invoke calls to jump to the C++ runtime
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    auto* operatorHandler = dynamic_cast<HJOperatorHandler*>(executionCtx.getGlobalOperatorHandler(operatorHandlerId).value);
    if (operatorHandler->wasSetupCalled(joinBuildSide))
    {
        return;
    }

    const auto cleanupStateNautilusFunction
        = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(compilationContext.registerFunction(std::function(
            [copyOfHashMapOptions = hashMapOptions](nautilus::val<Nautilus::Interface::HashMap*> hashMap)
            {
                const Interface::ChainedHashMapRef hashMapRef{
                    hashMap,
                    copyOfHashMapOptions.fieldKeys,
                    copyOfHashMapOptions.fieldValues,
                    copyOfHashMapOptions.entriesPerPage,
                    copyOfHashMapOptions.entrySize};
                for (const auto entry : hashMapRef)
                {
                    const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset{
                        entry, hashMap, copyOfHashMapOptions.fieldKeys, copyOfHashMapOptions.fieldValues};
                    const auto state = entryRefReset.getValueMemArea();
                    nautilus::invoke(
                        +[](int8_t* pagedVectorMemArea) -> void
                        {
                            /// Calls the destructor of the PagedVector
                            /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
                            pagedVector->~PagedVector();
                        },
                        state);
                }
            })));
    /// NOLINTEND(performance-unnecessary-value-param)
    operatorHandler->setNautilusCleanupExec(cleanupStateNautilusFunction, joinBuildSide);

    if (CheckpointManager::isCheckpointingEnabled() && !operatorHandler->hasCheckpointCallback(joinBuildSide))
    {
        const auto callbackId = fmt::format(
            "hash_join_state_{}_{}_{}",
            operatorHandlerId.getRawValue(),
            joinBuildSide == JoinBuildSideType::Left ? "left" : "right",
            reinterpret_cast<uintptr_t>(operatorHandler));
        operatorHandler->setCheckpointCallbackId(joinBuildSide, callbackId);
        CheckpointManager::registerCallback(callbackId, [handler = operatorHandler, buildSide = joinBuildSide]() {
            handler->requestCheckpoint(buildSide);
        });
    }

    // If we recover and the build side has no new tuples, eagerly restore the checkpoint once during setup.
    if (CheckpointManager::shouldRecoverFromCheckpoint())
    {
        auto* bufferProvider = executionCtx.pipelineMemoryProvider.bufferProvider.value;
        if (bufferProvider && operatorHandler->markCheckpointRestored(joinBuildSide))
        {
            const auto& opts = hashMapOptions;
            const CreateNewHashMapSliceArgs hashMapSliceArgs{
                operatorHandler->getNautilusCleanupExec(),
                opts.keySize,
                opts.valueSize,
                opts.pageSize,
                opts.numberOfBuckets};
            const auto slices = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(
                Timestamp(0), operatorHandler->getCreateNewSlicesFunction(hashMapSliceArgs));
            if (!slices.empty())
            {
                const auto hjSlice = std::dynamic_pointer_cast<HJSlice>(slices[0]);
                if (hjSlice)
                {
                    for (uint64_t idx = 0; idx < hjSlice->getNumberOfHashMapsForSide(); ++idx)
                    {
                        auto* hashMap = hjSlice->getHashMapPtrOrCreate(WorkerThreadId(idx), joinBuildSide);
                        auto checkpointFile = getHashJoinCheckpointFile(operatorHandlerId, joinBuildSide, WorkerThreadId(idx));
                        if (!std::filesystem::exists(checkpointFile))
                        {
                            if (auto latest = findLatestCheckpointForSide(joinBuildSide, WorkerThreadId(idx)))
                            {
                                checkpointFile = *latest;
                            }
                            else
                            {
                                continue;
                            }
                        }
                        ensureCheckpointDirectory(checkpointFile);
                        deserializeHashJoinState(hashMap, this, bufferProvider, checkpointFile);
                        NES_INFO(
                            "HashJoin restored checkpoint for {} side worker {} with {} tuples from {}",
                            joinBuildSide == JoinBuildSideType::Left ? "left" : "right",
                            idx,
                            hashMap->getNumberOfTuples(),
                            checkpointFile.string());
                    }
                }
            }
        }
    }
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
        nautilus::val<JoinBuildSideType>(joinBuildSide),
        nautilus::val<const HJBuildPhysicalOperator*>(this));
    Interface::ChainedHashMapRef hashMap{
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
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the underyling paged vector
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset{
                entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
            const auto state = entryRefReset.getValueMemArea();
            nautilus::invoke(
                +[](int8_t* pagedVectorMemArea) -> void
                {
                    /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
                    /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                    auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
                    new (pagedVector) Nautilus::Interface::PagedVector();
                },
                state);
        },
        ctx.pipelineMemoryProvider.bufferProvider);

    /// Inserting the tuple into the corresponding hash entry
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef{
        hashMapEntry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues};
    auto entryMemArea = entryRef.getValueMemArea();
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(entryMemArea, bufferRef);
    pagedVectorRef.writeRecord(record, ctx.pipelineMemoryProvider.bufferProvider);

    nautilus::invoke(
        deserializeHashMapProxy,
        operatorHandler,
        timestamp,
        ctx.workerThreadId,
        nautilus::val<JoinBuildSideType>(joinBuildSide),
        ctx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<const HJBuildPhysicalOperator*>(this));

    const auto checkpointRequested = invoke(
        +[](OperatorHandler* handler, JoinBuildSideType side) -> bool
        {
            if (auto* hjHandler = dynamic_cast<HJOperatorHandler*>(handler))
            {
                return hjHandler->consumeCheckpointRequest(side);
            }
            return false;
        },
        operatorHandler,
        nautilus::val<JoinBuildSideType>(joinBuildSide));
    if (checkpointRequested)
    {
        nautilus::invoke(
            serializeHashMapProxy,
            operatorHandler,
            timestamp,
            ctx.workerThreadId,
            nautilus::val<JoinBuildSideType>(joinBuildSide),
            ctx.pipelineMemoryProvider.bufferProvider,
        nautilus::val<const HJBuildPhysicalOperator*>(this));
    }
}

HJBuildPhysicalOperator::HJBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    const JoinBuildSideType joinBuildSide,
    std::unique_ptr<TimeFunction> timeFunction,
    const std::shared_ptr<Interface::BufferRef::TupleBufferRef>& bufferRef,
    HashMapOptions hashMapOptions)
    : StreamJoinBuildPhysicalOperator(operatorHandlerId, joinBuildSide, std::move(timeFunction), bufferRef)
    , hashMapOptions(std::move(hashMapOptions))
{
}

}
