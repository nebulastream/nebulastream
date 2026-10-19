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

#include <Runtime/CheckpointManager.hpp>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
#include <tuple>
#include <utility>
#include <vector>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <Thread.hpp>
#include <scope_guard.hpp>

namespace NES
{

struct CheckpointManager::CallbackEntry
{
    explicit CallbackEntry(Callback callback) : callback(std::move(callback)) { }

    Callback callback;
    std::mutex mutex;
    std::condition_variable idle;
    size_t activeInvocations = 0;
    bool cancelled = false;
};

namespace
{
constexpr std::string_view RecoverySnapshotDirectoryName = ".recovery_snapshot";
constexpr std::string_view CompletedRoundFileName = "completed_round";
constexpr std::string_view RoundsDirectoryName = "rounds";
}

void CheckpointManager::cancelAndWaitUntilIdle(const std::shared_ptr<CallbackEntry>& entry)
{
    std::unique_lock lock(entry->mutex);
    entry->cancelled = true;
    entry->idle.wait(lock, [&entry] { return entry->activeInvocations == 0; });
}

void CheckpointManager::invoke(const std::shared_ptr<CallbackEntry>& entry, const uint64_t round)
{
    {
        const std::scoped_lock lock(entry->mutex);
        if (entry->cancelled)
        {
            return;
        }
        ++entry->activeInvocations;
    }
    SCOPE_EXIT
    {
        const std::scoped_lock lock(entry->mutex);
        if (--entry->activeInvocations == 0)
        {
            entry->idle.notify_all();
        }
    };
    entry->callback(round);
}

std::vector<std::shared_ptr<CheckpointManager::CallbackEntry>> CheckpointManager::takeEntriesLocked(const std::string& identifier)
{
    std::vector<std::shared_ptr<CallbackEntry>> entries;
    for (auto* registry : {&prepareCallbacks, &commitCallbacks})
    {
        if (const auto entry = registry->find(identifier); entry != registry->end())
        {
            entries.push_back(entry->second);
            registry->erase(entry);
        }
    }
    return entries;
}

CheckpointManager::CheckpointManager(
    std::filesystem::path checkpointDirectory, const std::chrono::milliseconds checkpointInterval, const bool recoverFromCheckpoint)
    : checkpointDirectory(std::move(checkpointDirectory))
    , recoveryDirectory(this->checkpointDirectory)
    , checkpointInterval(checkpointInterval)
    , recoverFromCheckpoint(recoverFromCheckpoint)
{
    if (recoverFromCheckpoint)
    {
        prepareRecoverySnapshot();
        recoveredRound = loadCompletedRound(recoveryDirectory);
    }
    /// Continuing after the recovered round keeps the rounds of the recovered process apart from the ones of this process.
    nextRound = std::max(loadCompletedRound(this->checkpointDirectory).value_or(0), recoveredRound.value_or(0)) + 1;
    if (isCheckpointingEnabled())
    {
        scheduler = Thread("Checkpointing", [this](const std::stop_token& stopToken) { runScheduler(stopToken); });
    }
}

CheckpointManager::~CheckpointManager()
{
    scheduler = {};
    std::vector<std::shared_ptr<CallbackEntry>> entries;
    {
        const std::scoped_lock lock(mutex);
        for (auto* registry : {&prepareCallbacks, &commitCallbacks})
        {
            for (auto& [identifier, entry] : *registry)
            {
                entries.push_back(entry);
            }
            registry->clear();
        }
    }
    for (const auto& entry : entries)
    {
        cancelAndWaitUntilIdle(entry);
    }
}

void CheckpointManager::prepareRecoverySnapshot()
{
    const auto snapshotDirectory = checkpointDirectory / RecoverySnapshotDirectoryName;
    const auto temporarySnapshotDirectory = checkpointDirectory / (std::string(RecoverySnapshotDirectoryName) + ".tmp");
    std::error_code errorCode;
    std::filesystem::remove_all(snapshotDirectory, errorCode);
    std::filesystem::remove_all(temporarySnapshotDirectory, errorCode);
    if (!std::filesystem::is_directory(checkpointDirectory, errorCode))
    {
        return;
    }

    std::filesystem::create_directories(temporarySnapshotDirectory, errorCode);
    for (std::filesystem::directory_iterator entry(checkpointDirectory, errorCode);
         !errorCode && entry != std::filesystem::directory_iterator();
         entry.increment(errorCode))
    {
        if (entry->path() == snapshotDirectory || entry->path() == temporarySnapshotDirectory)
        {
            continue;
        }
        std::filesystem::copy(
            entry->path(),
            temporarySnapshotDirectory / entry->path().filename(),
            std::filesystem::copy_options::recursive | std::filesystem::copy_options::overwrite_existing,
            errorCode);
        if (errorCode)
        {
            break;
        }
    }
    if (!errorCode)
    {
        std::filesystem::rename(temporarySnapshotDirectory, snapshotDirectory, errorCode);
    }
    if (errorCode)
    {
        NES_WARNING(
            "Cannot prepare the recovery snapshot of {}, recovering from the live checkpoint directory instead: {}",
            checkpointDirectory.string(),
            errorCode.message());
        std::filesystem::remove_all(temporarySnapshotDirectory, errorCode);
        return;
    }
    recoveryDirectory = snapshotDirectory;
}

const std::filesystem::path& CheckpointManager::getCheckpointDirectory() const
{
    return checkpointDirectory;
}

const std::filesystem::path& CheckpointManager::getRecoveryDirectory() const
{
    return recoveryDirectory;
}

bool CheckpointManager::isCheckpointingEnabled() const
{
    return checkpointInterval.count() > 0;
}

bool CheckpointManager::shouldRecoverFromCheckpoint() const
{
    return recoverFromCheckpoint;
}

std::optional<uint64_t> CheckpointManager::loadCompletedRound(const std::filesystem::path& directory)
{
    const auto completedRound = loadFile(directory / CompletedRoundFileName);
    if (not completedRound)
    {
        return std::nullopt;
    }
    uint64_t round = 0;
    if (const auto [end, error] = std::from_chars(completedRound->data(), completedRound->data() + completedRound->size(), round);
        error != std::errc{} or end != completedRound->data() + completedRound->size())
    {
        throw CheckpointError("Corrupt completed checkpoint round in {}", directory.string());
    }
    return round;
}

std::filesystem::path CheckpointManager::getQueryDirectory(const QueryId& queryId) const
{
    return checkpointDirectory / fmt::format("{}{}", QueryDirectoryPrefix, queryId.getLocalQueryId().getRawValue());
}

std::filesystem::path CheckpointManager::getRoundDirectory(const QueryId& queryId, const uint64_t round) const
{
    return getQueryDirectory(queryId) / RoundsDirectoryName / std::to_string(round);
}

std::filesystem::path CheckpointManager::getRecoveryQueryDirectory(const QueryId& queryId) const
{
    return recoveryDirectory / fmt::format("{}{}", QueryDirectoryPrefix, queryId.getLocalQueryId().getRawValue());
}

std::optional<std::filesystem::path> CheckpointManager::getRecoveryRoundDirectory(const QueryId& queryId) const
{
    if (not recoverFromCheckpoint)
    {
        return std::nullopt;
    }
    /// The worker may have stopped between completing the first round of a query and the query recording it.
    const auto round = loadCompletedRound(getRecoveryQueryDirectory(queryId)).or_else([this] { return recoveredRound; });
    if (not round)
    {
        return std::nullopt;
    }
    auto roundDirectory = getRecoveryQueryDirectory(queryId) / RoundsDirectoryName / std::to_string(*round);
    if (not std::filesystem::is_directory(roundDirectory))
    {
        return std::nullopt;
    }
    return roundDirectory;
}

void CheckpointManager::recordCompletedQueryRound(const QueryId& queryId, const uint64_t round) const
{
    persistFile(getQueryDirectory(queryId) / CompletedRoundFileName, std::to_string(round));
}

void CheckpointManager::persistFile(const std::filesystem::path& path, const std::string_view contents) const
{
    const auto filePath = path.is_absolute() ? path : checkpointDirectory / path;
    const auto temporaryFilePath = std::filesystem::path(filePath.string() + ".tmp");
    std::filesystem::create_directories(filePath.parent_path());
    {
        std::ofstream out(temporaryFilePath, std::ios::binary | std::ios::trunc);
        out.write(contents.data(), static_cast<std::streamsize>(contents.size()));
        out.close();
        if (!out)
        {
            throw CheckpointError("Cannot write checkpoint file {}", temporaryFilePath.string());
        }
    }
    std::error_code errorCode;
    std::filesystem::rename(temporaryFilePath, filePath, errorCode);
    if (errorCode)
    {
        std::filesystem::remove(temporaryFilePath, errorCode);
        throw CheckpointError("Cannot replace checkpoint file {}: {}", filePath.string(), errorCode.message());
    }
}

std::optional<std::string> CheckpointManager::loadFile(const std::filesystem::path& path)
{
    std::ifstream in(path, std::ios::binary);
    if (!in.is_open())
    {
        return std::nullopt;
    }
    return std::string(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void CheckpointManager::registerCallback(const std::string& identifier, Callback callback, const CallbackPhase phase)
{
    std::vector<std::shared_ptr<CallbackEntry>> replacedEntries;
    {
        const std::scoped_lock lock(mutex);
        replacedEntries = takeEntriesLocked(identifier);
        auto& registry = phase == CallbackPhase::Prepare ? prepareCallbacks : commitCallbacks;
        registry.emplace(identifier, std::make_shared<CallbackEntry>(std::move(callback)));
    }
    for (const auto& entry : replacedEntries)
    {
        cancelAndWaitUntilIdle(entry);
    }
}

void CheckpointManager::unregisterCallback(const std::string& identifier)
{
    std::vector<std::shared_ptr<CallbackEntry>> removedEntries;
    {
        const std::scoped_lock lock(mutex);
        removedEntries = takeEntriesLocked(identifier);
    }
    for (const auto& entry : removedEntries)
    {
        cancelAndWaitUntilIdle(entry);
    }
}

void CheckpointManager::unregisterCallbacksWithPrefix(const std::string& prefix)
{
    std::vector<std::shared_ptr<CallbackEntry>> removedEntries;
    {
        const std::scoped_lock lock(mutex);
        for (auto* registry : {&prepareCallbacks, &commitCallbacks})
        {
            for (auto entry = registry->lower_bound(prefix); entry != registry->end() and entry->first.starts_with(prefix);)
            {
                removedEntries.push_back(entry->second);
                entry = registry->erase(entry);
            }
        }
    }
    for (const auto& entry : removedEntries)
    {
        cancelAndWaitUntilIdle(entry);
    }
}

std::vector<std::shared_ptr<CheckpointManager::CallbackEntry>> CheckpointManager::getPrepareCallbacksWithPrefix(const std::string& prefix)
{
    const std::scoped_lock lock(mutex);
    std::vector<std::shared_ptr<CallbackEntry>> entries;
    for (auto entry = prepareCallbacks.lower_bound(prefix); entry != prepareCallbacks.end() and entry->first.starts_with(prefix); ++entry)
    {
        entries.push_back(entry->second);
    }
    return entries;
}

void CheckpointManager::registerPrepareGuard(const std::string& prefix, PrepareGuard acquireGuard)
{
    const std::scoped_lock lock(mutex);
    prepareGuards.insert_or_assign(prefix, std::move(acquireGuard));
}

void CheckpointManager::unregisterPrepareGuard(const std::string& prefix)
{
    const std::scoped_lock lock(mutex);
    prepareGuards.erase(prefix);
}

std::string CheckpointManager::getQueryCallbackPrefix(const QueryId& queryId)
{
    return fmt::format("query/{}/", queryId.getLocalQueryId().getRawValue());
}

void CheckpointManager::runCheckpointRound()
{
    const std::scoped_lock roundLock(roundMutex);
    std::vector<std::pair<std::string, std::shared_ptr<CallbackEntry>>> prepare;
    std::vector<std::shared_ptr<CallbackEntry>> commit;
    std::map<std::string, PrepareGuard> guards;
    uint64_t round = 0;
    {
        const std::scoped_lock lock(mutex);
        round = nextRound++;
        prepare.assign(prepareCallbacks.begin(), prepareCallbacks.end());
        for (const auto& [identifier, entry] : commitCallbacks)
        {
            commit.push_back(entry);
        }
        guards = prepareGuards;
    }

    /// Callbacks with a common prefix are adjacent in identifier order, so each guard is acquired at most once per round.
    for (auto callback = prepare.begin(); callback != prepare.end();)
    {
        const auto matchingGuard
            = std::ranges::find_if(guards, [&](const auto& prefixAndGuard) { return callback->first.starts_with(prefixAndGuard.first); });
        if (matchingGuard == guards.end())
        {
            invoke(callback->second, round);
            ++callback;
            continue;
        }
        const auto& [prefix, acquireGuard] = *matchingGuard;
        if (const auto guard = acquireGuard(round))
        {
            /// The guarded callbacks are looked up again, as callbacks registered until the guard was acquired, e.g., by the sources of a
            /// query that just started, belong to the round as well.
            for (const auto& entry : getPrepareCallbacksWithPrefix(prefix))
            {
                invoke(entry, round);
            }
        }
        callback = std::find_if(
            callback, prepare.end(), [&](const auto& identifierAndEntry) { return not identifierAndEntry.first.starts_with(prefix); });
    }

    persistFile(CompletedRoundFileName, std::to_string(round));
    for (const auto& entry : commit)
    {
        invoke(entry, round);
    }
}

void CheckpointManager::runScheduler(const std::stop_token& stopToken)
{
    while (!stopToken.stop_requested())
    {
        {
            std::unique_lock lock(mutex);
            std::ignore = schedulerWakeup.wait_for(lock, stopToken, checkpointInterval, [] { return false; });
        }
        if (stopToken.stop_requested())
        {
            return;
        }
        try
        {
            runCheckpointRound();
        }
        catch (const std::exception& exception)
        {
            NES_WARNING("Checkpoint round failed, skipping its commit phase: {}", exception.what());
        }
    }
}

}
