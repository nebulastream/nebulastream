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

#include <QueryCheckpointing.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <SerializableQueryPlan.pb.h>

namespace NES
{
namespace
{
constexpr std::string_view PlanFileName = "plan.pb";
constexpr std::string_view ManifestFileName = "manifest";
constexpr std::string_view Manifest = "format_version=1\n";
constexpr std::string_view FailedMarkerFileName = "failed";
constexpr std::string_view StateDirectoryName = "state";
constexpr std::string_view CleanUpCallback = "worker/clean-up";
/// A pipeline holds the lock only while it processes a single buffer, so not getting the lock for this long means that the query
/// is overloaded. The round then fails instead of stalling the query any further.
constexpr std::chrono::milliseconds LockTimeout{1000};

std::filesystem::path getStateDirectory(const std::filesystem::path& roundDirectory, const OperatorHandlerId handlerId)
{
    return roundDirectory / StateDirectoryName / std::to_string(handlerId.getRawValue());
}

std::shared_ptr<void> lockForCheckpoint(const std::map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers)
{
    auto locks = std::make_shared<std::vector<std::unique_lock<std::shared_mutex>>>();
    for (const auto& [handlerId, handler] : handlers)
    {
        const auto deadline = std::chrono::steady_clock::now() + LockTimeout;
        auto lock = handler->tryAcquireCheckpointStateWriteLock();
        while (not lock.owns_lock())
        {
            if (std::chrono::steady_clock::now() > deadline)
            {
                throw CheckpointError("Operator handler {} stayed busy for {} ms", handlerId.getRawValue(), LockTimeout.count());
            }
            std::this_thread::sleep_for(std::chrono::microseconds(100));
            lock = handler->tryAcquireCheckpointStateWriteLock();
        }
        locks->push_back(std::move(lock));
    }
    return locks;
}
}

QueryCheckpointing::QueryCheckpointing(std::shared_ptr<CheckpointManager> checkpointManager, std::shared_ptr<const QueryLog> queryLog)
    : checkpointManager(std::move(checkpointManager)), queryLog(std::move(queryLog))
{
    this->checkpointManager->registerCallback(
        std::string(CleanUpCallback), [this](uint64_t) { removeTerminatedQueries(); }, CheckpointManager::CallbackPhase::Commit);
}

QueryCheckpointing::~QueryCheckpointing()
{
    checkpointManager->unregisterCallback(std::string(CleanUpCallback));
    const std::scoped_lock lock(mutex);
    for (const auto& queryId : checkpointedQueries)
    {
        const auto callbackPrefix = CheckpointManager::getQueryCallbackPrefix(queryId);
        checkpointManager->unregisterPrepareGuard(callbackPrefix);
        checkpointManager->unregisterCallbacksWithPrefix(callbackPrefix);
    }
}

std::vector<QueryCheckpointing::RecoverableQuery> QueryCheckpointing::findRecoverableQueries() const
{
    std::vector<RecoverableQuery> recoverableQueries;
    std::error_code errorCode;
    for (const auto& entry : std::filesystem::directory_iterator(checkpointManager->getRecoveryDirectory(), errorCode))
    {
        if (not entry.is_directory() or not entry.path().filename().string().starts_with(CheckpointManager::QueryDirectoryPrefix))
        {
            continue;
        }
        if (std::filesystem::exists(entry.path() / FailedMarkerFileName))
        {
            NES_INFO("Skipping checkpointed query {}, as it failed", entry.path().string());
            continue;
        }
        if (CheckpointManager::loadFile(entry.path() / ManifestFileName) != Manifest)
        {
            NES_WARNING("Skipping checkpointed query {}, as its manifest is missing or of an unknown format", entry.path().string());
            continue;
        }
        SerializableQueryPlan serializedPlan;
        if (const auto planBytes = CheckpointManager::loadFile(entry.path() / PlanFileName);
            not planBytes or not serializedPlan.ParseFromString(*planBytes))
        {
            NES_WARNING("Skipping checkpointed query {}, as its plan cannot be read", entry.path().string());
            continue;
        }
        recoverableQueries.push_back({QueryPlanSerializationUtil::deserializeQueryPlan(serializedPlan), entry.path()});
    }
    std::ranges::sort(recoverableQueries, {}, &RecoverableQuery::queryDirectory);
    return recoverableQueries;
}

void QueryCheckpointing::registerQuery(const LogicalPlan& plan, const CompiledQueryPlan& compiledQueryPlan)
{
    const auto& queryId = plan.getQueryId();
    const auto recoveryRound = checkpointManager->getRecoveryRoundDirectory(queryId);
    for (const auto& [handlerId, handler] : compiledQueryPlan.checkpointableHandlers)
    {
        handler->setCheckpointed(recoveryRound.transform([handlerId](const auto& round) { return getStateDirectory(round, handlerId); }));
    }
    if (not checkpointManager->isCheckpointingEnabled())
    {
        return;
    }

    const auto queryDirectory = checkpointManager->getQueryDirectory(queryId);
    checkpointManager->persistFile(queryDirectory / PlanFileName, QueryPlanSerializationUtil::serializeQueryPlan(plan).SerializeAsString());
    checkpointManager->persistFile(queryDirectory / ManifestFileName, Manifest);

    const auto callbackPrefix = CheckpointManager::getQueryCallbackPrefix(queryId);
    auto preparedRound = std::make_shared<std::atomic<uint64_t>>(0);
    checkpointManager->registerPrepareGuard(
        callbackPrefix,
        [checkpointManager = checkpointManager.get(),
         queryLog = queryLog.get(),
         queryId,
         handlers = compiledQueryPlan.checkpointableHandlers,
         checkpointsAllState = compiledQueryPlan.checkpointsAllState,
         preparedRound](const uint64_t round) -> std::optional<std::shared_ptr<void>>
        {
            /// Without a round, a recovered query replays its input from the beginning and rewrites its output.
            if (not checkpointsAllState)
            {
                return std::nullopt;
            }
            const auto status = queryLog->getQueryStatus(queryId);
            const auto terminated = status and (status->state == QueryStatus::Stopped or status->state == QueryStatus::Failed);
            /// The sources of a query register their checkpoint callbacks only before the query is running, so a round without them
            /// would miss the progress of the sources. The query keeps its previous round instead.
            if (not terminated and (not status or status->state != QueryStatus::Running))
            {
                return std::nullopt;
            }
            /// A terminated query no longer changes its state, so the round captures it without locking.
            auto locks = terminated ? std::shared_ptr<void>() : lockForCheckpoint(handlers);
            /// A round that did not complete in a previous run of the worker may have left files behind.
            const auto roundDirectory = checkpointManager->getRoundDirectory(queryId, round);
            std::filesystem::remove_all(roundDirectory);
            std::filesystem::create_directories(roundDirectory);
            preparedRound->store(round);
            return locks;
        });
    checkpointManager->registerCallback(
        callbackPrefix + "state",
        [checkpointManager = checkpointManager.get(), queryId, handlers = compiledQueryPlan.checkpointableHandlers](const uint64_t round)
        {
            for (const auto& [handlerId, handler] : handlers)
            {
                handler->serializeState(getStateDirectory(checkpointManager->getRoundDirectory(queryId, round), handlerId));
            }
        },
        CheckpointManager::CallbackPhase::Prepare);
    checkpointManager->registerCallback(
        callbackPrefix + "rounds",
        [checkpointManager = checkpointManager.get(),
         queryId,
         preparedRound,
         roundsDirectory = checkpointManager->getRoundDirectory(queryId, 0).parent_path()](const uint64_t completedRound)
        {
            if (preparedRound->load() != completedRound)
            {
                return;
            }
            checkpointManager->recordCompletedQueryRound(queryId, completedRound);
            std::error_code errorCode;
            for (const auto& round : std::filesystem::directory_iterator(roundsDirectory, errorCode))
            {
                if (round.path().filename() != std::to_string(completedRound))
                {
                    std::filesystem::remove_all(round.path(), errorCode);
                }
            }
        },
        CheckpointManager::CallbackPhase::Commit);

    const std::scoped_lock lock(mutex);
    checkpointedQueries.push_back(queryId);
}

void QueryCheckpointing::removeTerminatedQueries()
{
    const std::scoped_lock lock(mutex);
    std::erase_if(
        checkpointedQueries,
        [this](const QueryId& queryId)
        {
            /// A query that is not logged yet is still starting.
            const auto status = queryLog->getQueryStatus(queryId);
            if (not status or (status->state != QueryStatus::Stopped and status->state != QueryStatus::Failed))
            {
                return false;
            }
            removeQuery(queryId, status->state == QueryStatus::Stopped);
            /// The checkpoint of a failed query stays for inspection, but recovering it would most likely fail again.
            if (status->state == QueryStatus::Failed)
            {
                checkpointManager->persistFile(checkpointManager->getQueryDirectory(queryId) / FailedMarkerFileName, "");
            }
            return true;
        });
}

void QueryCheckpointing::unregisterQuery(const QueryId& queryId)
{
    const std::scoped_lock lock(mutex);
    std::erase(checkpointedQueries, queryId);
    removeQuery(queryId, true);
}

void QueryCheckpointing::removeQuery(const QueryId& queryId, const bool removeCheckpoint)
{
    const auto callbackPrefix = CheckpointManager::getQueryCallbackPrefix(queryId);
    checkpointManager->unregisterPrepareGuard(callbackPrefix);
    checkpointManager->unregisterCallbacksWithPrefix(callbackPrefix);
    if (removeCheckpoint)
    {
        std::error_code errorCode;
        std::filesystem::remove_all(checkpointManager->getQueryDirectory(queryId), errorCode);
    }
}

}
