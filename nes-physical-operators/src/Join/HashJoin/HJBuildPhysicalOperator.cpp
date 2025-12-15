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
constexpr std::string_view HashJoinCheckpointFileName = "hash_join_checkpoint.bin";

void ensureCheckpointDirectory(const std::filesystem::path& checkpointFile)
{
    const auto checkpointDir = checkpointFile.parent_path();
    if (!checkpointDir.empty() && !exists(checkpointDir))
    {
        create_directories(checkpointDir);
    }
}

std::filesystem::path getHashJoinCheckpointFile(OperatorHandlerId handlerId, JoinBuildSideType buildSide)
{
    auto baseDir = CheckpointManager::getCheckpointDirectory();
    baseDir /= fmt::format(
        "hash_join_checkpoint_{}_{}.bin",
        handlerId.getRawValue(),
        buildSide == JoinBuildSideType::Left ? "left" : "right");
    return baseDir;
}

std::optional<std::filesystem::path> findLatestCheckpointForSide(JoinBuildSideType buildSide)
{
    const auto checkpointDir = CheckpointManager::getCheckpointDirectory();
    if (!std::filesystem::exists(checkpointDir))
    {
        NES_DEBUG("HJ checkpoint fallback: directory {} does not exist", checkpointDir.string());
        return std::nullopt;
    }
    const auto patternStr = fmt::format(
        R"(hash_join_checkpoint_.*_{})",
        buildSide == JoinBuildSideType::Left ? "left\\.bin" : "right\\.bin");
    const std::regex pattern(patternStr);

    std::optional<std::filesystem::path> newestFile;
    std::optional<std::filesystem::file_time_type> newestTime;
    for (const auto& entry : std::filesystem::directory_iterator(checkpointDir))
    {
        if (!entry.is_regular_file())
        {
            continue;
        }
        const auto filename = entry.path().filename().string();
        if (!std::regex_match(filename, pattern))
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
    NES_DEBUG(
        "HJ checkpoint fallback: picked={} path={} pattern={} dir={}",
        static_cast<bool>(newestFile),
        newestFile ? newestFile->string() : "<none>",
        patternStr,
        checkpointDir.string());
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

std::unique_ptr<Interface::HashMap> mergeHashMapsForCheckpoint(
    const HJOperatorHandler* operatorHandler,
    Timestamp timestamp,
    AbstractBufferProvider* bufferProvider,
    const HJBuildPhysicalOperator* buildOperator,
    JoinBuildSideType buildSide)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");
    if (bufferProvider == nullptr)
    {
        NES_WARNING("HashJoin checkpoint merge skipped: buffer provider is null");
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

    Nautilus::Interface::HashMap* referenceMap = nullptr;
    for (uint64_t idx = 0; idx < hjSlice->getNumberOfHashMapsForSide(); ++idx)
    {
        auto* mapPtr = hjSlice->getHashMapPtr(WorkerThreadId(idx), buildSide);
        if (mapPtr != nullptr && mapPtr->getNumberOfTuples() > 0)
        {
            referenceMap = mapPtr;
            break;
        }
    }
    if (referenceMap == nullptr)
    {
        return nullptr;
    }

    auto* chainedReference = dynamic_cast<Nautilus::Interface::ChainedHashMap*>(referenceMap);
    INVARIANT(chainedReference != nullptr, "Hash join checkpoints expect a chained hash map implementation");
    auto snapshot = Nautilus::Interface::ChainedHashMap::createNewMapWithSameConfiguration(*chainedReference);

    auto mergeSingleMap = [&](Nautilus::Interface::HashMap* sourceMap)
    {
        if (sourceMap == nullptr || sourceMap->getNumberOfTuples() == 0)
        {
            return;
        }
        auto* chainedSource = dynamic_cast<Nautilus::Interface::ChainedHashMap*>(sourceMap);
        INVARIANT(chainedSource != nullptr, "Hash join checkpoints expect chained hash maps for merging");
        for (uint64_t chainIdx = 0; chainIdx < chainedSource->getNumberOfChains(); ++chainIdx)
        {
            auto* entry = chainedSource->getStartOfChain(chainIdx);
            while (entry != nullptr)
            {
                auto* const newEntry = static_cast<Nautilus::Interface::ChainedHashMapEntry*>(
                    snapshot->insertEntry(entry->hash, bufferProvider));
                auto* const keyPtrSrc = reinterpret_cast<const char*>(entry) + sizeof(Nautilus::Interface::ChainedHashMapEntry);
                auto* const keyPtrDst = reinterpret_cast<char*>(newEntry) + sizeof(Nautilus::Interface::ChainedHashMapEntry);
                std::memcpy(keyPtrDst, keyPtrSrc, hashMapOptions.keySize);

                auto* const valuePtrSrc = keyPtrSrc + hashMapOptions.keySize;
                auto* const valuePtrDst = keyPtrDst + hashMapOptions.keySize;
                if (hashMapOptions.valuesContainPagedVectors)
                {
                    auto* srcPagedVector = reinterpret_cast<const Nautilus::Interface::PagedVector*>(valuePtrSrc);
                    auto* dstPagedVector = new (valuePtrDst) Nautilus::Interface::PagedVector();
                    dstPagedVector->copyFrom(*srcPagedVector);
                }
                else
                {
                    std::memcpy(valuePtrDst, valuePtrSrc, hashMapOptions.valueSize);
                }
                entry = entry->next;
            }
        }
    };

    for (uint64_t idx = 0; idx < hjSlice->getNumberOfHashMapsForSide(); ++idx)
    {
        mergeSingleMap(hjSlice->getHashMapPtr(WorkerThreadId(idx), buildSide));
    }

    return snapshot;
}
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

    auto combinedHashMap = mergeHashMapsForCheckpoint(operatorHandler, timestamp, bufferProvider, buildOperator, buildSide);
    if (!combinedHashMap)
    {
        return;
    }
    const auto checkpointFile = getHashJoinCheckpointFile(buildOperator->operatorHandlerId, buildSide);
    ensureCheckpointDirectory(checkpointFile);
    serializeHashJoinState(combinedHashMap.get(), buildOperator, checkpointFile);
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
    auto* const hashMap = getHashJoinHashMapProxy(operatorHandler, timestamp, workerThreadId, buildSide, buildOperator);
    auto checkpointFile = getHashJoinCheckpointFile(buildOperator->operatorHandlerId, buildSide);
    if (!std::filesystem::exists(checkpointFile))
    {
        if (auto latest = findLatestCheckpointForSide(buildSide))
        {
            checkpointFile = *latest;
        }
        else
        {
            return hashMap;
        }
    }
    NES_DEBUG(
        "HJ deserialize checkpoint side={} using file {}",
        buildSide == JoinBuildSideType::Left ? "left" : "right",
        checkpointFile.string());
    ensureCheckpointDirectory(checkpointFile);
    deserializeHashJoinState(hashMap, buildOperator, bufferProvider, checkpointFile);
    return hashMap;
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

    /// If we recover and the build side has no new tuples, eagerly restore the checkpoint once during setup.
    if (CheckpointManager::shouldRecoverFromCheckpoint())
    {
        auto* bufferProvider = executionCtx.pipelineMemoryProvider.bufferProvider.value;
        auto checkpointFile = getHashJoinCheckpointFile(operatorHandlerId, joinBuildSide);
        NES_DEBUG(
            "HJ setup recover={} side={} bufferProvider={} checkpointFile={} exists={}",
            CheckpointManager::shouldRecoverFromCheckpoint(),
            joinBuildSide == JoinBuildSideType::Left ? "left" : "right",
            fmt::ptr(bufferProvider),
            checkpointFile.string(),
            exists(checkpointFile));
        if (!exists(checkpointFile))
        {
            if (auto latest = findLatestCheckpointForSide(joinBuildSide))
            {
                checkpointFile = *latest;
            }
        }
        NES_DEBUG(
            "HJ setup using checkpoint file {} for side {} (exists={})",
            checkpointFile.string(),
            joinBuildSide == JoinBuildSideType::Left ? "left" : "right",
            exists(checkpointFile));
        if (bufferProvider and exists(checkpointFile) && operatorHandler->markCheckpointRestored(joinBuildSide))
        {
            ensureCheckpointDirectory(checkpointFile);
            auto* hashMap
                = getHashJoinHashMapProxy(operatorHandler, Timestamp(0), WorkerThreadId(0), joinBuildSide, this);
            deserializeHashJoinState(hashMap, this, bufferProvider, checkpointFile);
            NES_INFO(
                "HashJoin restored checkpoint for {} side with {} tuples from {}",
                joinBuildSide == JoinBuildSideType::Left ? "left" : "right",
                hashMap->getNumberOfTuples(),
                checkpointFile.string());
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
