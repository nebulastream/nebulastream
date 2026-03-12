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

#pragma once
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <string_view>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
/// Forward declaration of PipelineExecutionContext, which directly includes OperatorHandler
class PipelineExecutionContext;

using OperatorHandlerId = NESStrongType<uint64_t, struct OperatorHandlerId_, 0, 1>;
static constexpr OperatorHandlerId INVALID_OPERATOR_HANDLER_ID = INVALID<OperatorHandlerId>;
static constexpr OperatorHandlerId INITIAL_OPERATOR_HANDLER_ID = INITIAL<OperatorHandlerId>;

namespace detail
{
constexpr uint64_t OperatorHandlerIdNamespaceMask = 0xFFFFFFFF00000000ULL;
constexpr uint64_t OperatorHandlerIdOrdinalMask = 0x00000000FFFFFFFFULL;

struct OperatorHandlerIdAllocatorState
{
    uint64_t namespacePrefix = 0;
    uint32_t nextOrdinal = INITIAL_OPERATOR_HANDLER_ID.getRawValue();
    bool initialized = false;
};

inline OperatorHandlerIdAllocatorState& getOperatorHandlerIdAllocatorState()
{
    static thread_local OperatorHandlerIdAllocatorState state;
    return state;
}

inline uint64_t computeStableOperatorHandlerNamespace(std::string_view stableSeed)
{
    uint64_t hash = 14695981039346656037ULL;
    for (const auto character : stableSeed)
    {
        hash ^= static_cast<unsigned char>(character);
        hash *= 1099511628211ULL;
    }
    return hash & OperatorHandlerIdNamespaceMask;
}
}

inline uint64_t getOperatorHandlerIdNamespacePrefix(const OperatorHandlerId handlerId)
{
    return handlerId.getRawValue() & detail::OperatorHandlerIdNamespaceMask;
}

inline uint32_t getOperatorHandlerIdOrdinal(const OperatorHandlerId handlerId)
{
    return static_cast<uint32_t>(handlerId.getRawValue() & detail::OperatorHandlerIdOrdinalMask);
}

inline void initializeOperatorHandlerIdAllocator(std::string_view stableSeed)
{
    auto& state = detail::getOperatorHandlerIdAllocatorState();
    state.namespacePrefix = detail::computeStableOperatorHandlerNamespace(stableSeed);
    state.nextOrdinal = INITIAL_OPERATOR_HANDLER_ID.getRawValue();
    state.initialized = true;
}

inline void initializeOperatorHandlerIdAllocator(const uint64_t namespacePrefix, const uint32_t nextOrdinal)
{
    auto& state = detail::getOperatorHandlerIdAllocatorState();
    state.namespacePrefix = namespacePrefix & detail::OperatorHandlerIdNamespaceMask;
    state.nextOrdinal = std::max(nextOrdinal, static_cast<uint32_t>(INITIAL_OPERATOR_HANDLER_ID.getRawValue()));
    state.initialized = true;
}

inline void advanceOperatorHandlerIdAllocatorPast(const OperatorHandlerId handlerId)
{
    auto& state = detail::getOperatorHandlerIdAllocatorState();
    PRECONDITION(state.initialized, "Operator handler id allocator must be initialized before advancing it");
    PRECONDITION(
        getOperatorHandlerIdNamespacePrefix(handlerId) == state.namespacePrefix,
        "Mismatching operator handler id namespace while advancing allocator");
    state.nextOrdinal = std::max(state.nextOrdinal, getOperatorHandlerIdOrdinal(handlerId) + 1);
}

inline OperatorHandlerId getNextOperatorHandlerId()
{
    auto& state = detail::getOperatorHandlerIdAllocatorState();
    if (!state.initialized)
    {
        initializeOperatorHandlerIdAllocator(std::string_view{});
    }
    const auto rawValue = state.namespacePrefix | state.nextOrdinal++;
    return OperatorHandlerId(rawValue);
}

/// @brief Interface to handle specific operator state.
class OperatorHandler
{
public:
    OperatorHandler() = default;

    virtual ~OperatorHandler() = default;

    virtual void start(PipelineExecutionContext& pipelineExecutionContext, uint32_t localStateVariableId) = 0;

    virtual void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) = 0;

    void setCheckpointDirectories(std::filesystem::path checkpointDirectory, std::filesystem::path checkpointRecoveryDirectory = {})
    {
        this->checkpointDirectory = std::move(checkpointDirectory);
        this->checkpointRecoveryDirectory
            = checkpointRecoveryDirectory.empty() ? this->checkpointDirectory : std::move(checkpointRecoveryDirectory);
    }

    [[nodiscard]] std::filesystem::path getCheckpointDirectory() const { return checkpointDirectory; }

    [[nodiscard]] std::filesystem::path getCheckpointRecoveryDirectory() const
    {
        return checkpointRecoveryDirectory.empty() ? checkpointDirectory : checkpointRecoveryDirectory;
    }

    using CheckpointStateReadLock = std::shared_lock<std::shared_mutex>;
    using CheckpointStateWriteLock = std::unique_lock<std::shared_mutex>;

    [[nodiscard]] CheckpointStateReadLock acquireCheckpointStateReadLock() const
    {
        return CheckpointStateReadLock(checkpointStateMutex);
    }

    [[nodiscard]] CheckpointStateWriteLock acquireCheckpointStateWriteLock() const
    {
        return CheckpointStateWriteLock(checkpointStateMutex);
    }

    void lockCheckpointStateShared() const
    {
        checkpointStateMutex.lock_shared();
    }

    void unlockCheckpointStateShared() const
    {
        checkpointStateMutex.unlock_shared();
    }

    virtual void serializeState(const std::filesystem::path&) {}

private:
    mutable std::shared_mutex checkpointStateMutex;
    std::filesystem::path checkpointDirectory;
    std::filesystem::path checkpointRecoveryDirectory;
};

}
