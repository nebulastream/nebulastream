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
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <Identifiers/NESStrongType.hpp>
#include <Runtime/QueryTerminationType.hpp>

namespace NES
{
/// Forward declaration of PipelineExecutionContext, which directly includes OperatorHandler
class PipelineExecutionContext;

using OperatorHandlerId = NESStrongType<uint64_t, struct OperatorHandlerId_, 0, 1>;
static constexpr OperatorHandlerId INVALID_OPERATOR_HANDLER_ID = INVALID<OperatorHandlerId>;
static constexpr OperatorHandlerId INITIAL_OPERATOR_HANDLER_ID = INITIAL<OperatorHandlerId>;

namespace detail
{
inline thread_local std::optional<OperatorHandlerId::Underlying> nextDeterministicOperatorHandlerId;
}

/// While an instance is alive, the current thread hands out operator handler ids starting at INITIAL_OPERATOR_HANDLER_ID. Lowering a
/// query within this scope gives its handlers the same ids on every compilation, which is how a recovered query finds the state that
/// its handlers checkpointed.
class DeterministicOperatorHandlerIdScope
{
public:
    DeterministicOperatorHandlerIdScope() : previous(detail::nextDeterministicOperatorHandlerId)
    {
        detail::nextDeterministicOperatorHandlerId = INITIAL_OPERATOR_HANDLER_ID.getRawValue();
    }

    ~DeterministicOperatorHandlerIdScope() { detail::nextDeterministicOperatorHandlerId = previous; }

    DeterministicOperatorHandlerIdScope(const DeterministicOperatorHandlerIdScope&) = delete;
    DeterministicOperatorHandlerIdScope& operator=(const DeterministicOperatorHandlerIdScope&) = delete;
    DeterministicOperatorHandlerIdScope(DeterministicOperatorHandlerIdScope&&) = delete;
    DeterministicOperatorHandlerIdScope& operator=(DeterministicOperatorHandlerIdScope&&) = delete;

private:
    std::optional<OperatorHandlerId::Underlying> previous;
};

inline OperatorHandlerId getNextOperatorHandlerId()
{
    if (auto& nextId = detail::nextDeterministicOperatorHandlerId)
    {
        return OperatorHandlerId((*nextId)++);
    }
    static std::atomic_uint64_t id = INITIAL_OPERATOR_HANDLER_ID.getRawValue();
    return OperatorHandlerId(id++);
}

/// Interface to handle specific operator state.
class OperatorHandler
{
public:
    OperatorHandler() = default;

    virtual ~OperatorHandler() = default;

    virtual void start(PipelineExecutionContext& pipelineExecutionContext) = 0;

    virtual void stop(QueryTerminationType terminationType, PipelineExecutionContext& pipelineExecutionContext) = 0;

    /// Handlers whose state survives a worker restart override supportsCheckpointing() and serializeState(). The worker marks them as
    /// checkpointed before the query starts; a handler that received a recovery directory restores its state from there.
    [[nodiscard]] virtual bool supportsCheckpointing() const { return false; }

    virtual void serializeState(const std::filesystem::path& /*stateDirectory*/) { }

    void setCheckpointed(std::optional<std::filesystem::path> recoveryStateDirectory)
    {
        checkpointed = true;
        this->recoveryStateDirectory = std::move(recoveryStateDirectory);
    }

    [[nodiscard]] bool isCheckpointed() const { return checkpointed; }

    [[nodiscard]] const std::optional<std::filesystem::path>& getRecoveryStateDirectory() const { return recoveryStateDirectory; }

    /// Pipelines that use a checkpointed handler hold the shared lock while they run, a checkpoint holds the exclusive one while it
    /// serializes the state.
    [[nodiscard]] std::shared_lock<std::shared_mutex> acquireCheckpointStateReadLock() const
    {
        return std::shared_lock{checkpointStateMutex};
    }

    [[nodiscard]] std::unique_lock<std::shared_mutex> tryAcquireCheckpointStateWriteLock() const
    {
        return std::unique_lock{checkpointStateMutex, std::try_to_lock};
    }

private:
    mutable std::shared_mutex checkpointStateMutex;
    bool checkpointed = false;
    std::optional<std::filesystem::path> recoveryStateDirectory;
};

}
