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

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <vector>
#include <QueryId.hpp>
#include <Thread.hpp>

namespace NES
{

/// Drives the periodic checkpoints of one worker. In each numbered round, the prepare callbacks write the checkpoint of every query into
/// the query's directory of that round. Only once all of them succeeded, the round is recorded as completed, which atomically replaces
/// the previous checkpoint of all queries. Afterwards, the commit callbacks may discard what the new checkpoint covers, e.g., source
/// replay files. Recovery only ever reads the last completed round.
class CheckpointManager final
{
public:
    enum class CallbackPhase : uint8_t
    {
        Prepare,
        Commit
    };

    using Callback = std::function<void(uint64_t round)>;

    /// With recoverFromCheckpoint set, the current content of the checkpoint directory is copied into a recovery snapshot first, so that
    /// recovery reads a stable state while the recovered queries already write new checkpoints.
    CheckpointManager(std::filesystem::path checkpointDirectory, std::chrono::milliseconds checkpointInterval, bool recoverFromCheckpoint);
    ~CheckpointManager();

    CheckpointManager(const CheckpointManager&) = delete;
    CheckpointManager& operator=(const CheckpointManager&) = delete;
    CheckpointManager(CheckpointManager&&) = delete;
    CheckpointManager& operator=(CheckpointManager&&) = delete;

    [[nodiscard]] const std::filesystem::path& getCheckpointDirectory() const;
    /// The recovery snapshot, or the checkpoint directory itself if the snapshot could not be prepared.
    [[nodiscard]] const std::filesystem::path& getRecoveryDirectory() const;
    [[nodiscard]] bool isCheckpointingEnabled() const;
    [[nodiscard]] bool shouldRecoverFromCheckpoint() const;

    /// Holds everything the worker keeps for a query, including the checkpoints of all rounds.
    [[nodiscard]] std::filesystem::path getQueryDirectory(const QueryId& queryId) const;
    [[nodiscard]] std::filesystem::path getRoundDirectory(const QueryId& queryId, uint64_t round) const;
    /// The query's directory in the recovery snapshot.
    [[nodiscard]] std::filesystem::path getRecoveryQueryDirectory(const QueryId& queryId) const;
    /// The checkpoint of the query in the last completed round that the query took part in before the worker stopped.
    [[nodiscard]] std::optional<std::filesystem::path> getRecoveryRoundDirectory(const QueryId& queryId) const;
    /// A query may skip rounds, so the commit phase of a round that it took part in records the round for the query.
    void recordCompletedQueryRound(const QueryId& queryId, uint64_t round) const;

    /// Replaces the file atomically. Relative paths are resolved against the checkpoint directory.
    void persistFile(const std::filesystem::path& path, std::string_view contents) const;
    [[nodiscard]] static std::optional<std::string> loadFile(const std::filesystem::path& path);

    /// Callbacks run in the order of their identifiers within a phase. Replacing or unregistering a callback waits for a running
    /// invocation of it to finish, so a callback never outlives what it captured once it is unregistered.
    void registerCallback(const std::string& identifier, Callback callback, CallbackPhase phase);
    void unregisterCallback(const std::string& identifier);
    void unregisterCallbacksWithPrefix(const std::string& prefix);

    /// Returns nullopt to leave out the callbacks of the prefix from the round.
    using PrepareGuard = std::function<std::optional<std::shared_ptr<void>>(uint64_t round)>;

    /// The prepare callbacks whose identifiers start with the prefix run while the object returned by acquireGuard is alive. This keeps,
    /// e.g., a query from processing data while its operator state and the progress of its sources are captured. The round includes
    /// all callbacks with the prefix that are registered when the guard is acquired. A guard that cannot be acquired fails the round.
    void registerPrepareGuard(const std::string& prefix, PrepareGuard acquireGuard);
    void unregisterPrepareGuard(const std::string& prefix);

    /// The callbacks and the prepare guard of a query share this prefix.
    [[nodiscard]] static std::string getQueryCallbackPrefix(const QueryId& queryId);

    /// Runs one checkpoint round on the calling thread and rethrows the first callback failure. Waits for a round that runs already, e.g.,
    /// a periodic one.
    void runCheckpointRound();

    static constexpr std::string_view QueryDirectoryPrefix = "query_";

private:
    struct CallbackEntry;
    using CallbackRegistry = std::map<std::string, std::shared_ptr<CallbackEntry>>;

    static void invoke(const std::shared_ptr<CallbackEntry>& entry, uint64_t round);
    static void cancelAndWaitUntilIdle(const std::shared_ptr<CallbackEntry>& entry);
    [[nodiscard]] std::vector<std::shared_ptr<CallbackEntry>> takeEntriesLocked(const std::string& identifier);
    [[nodiscard]] std::vector<std::shared_ptr<CallbackEntry>> getPrepareCallbacksWithPrefix(const std::string& prefix);

    void prepareRecoverySnapshot();
    [[nodiscard]] static std::optional<uint64_t> loadCompletedRound(const std::filesystem::path& directory);
    void runScheduler(const std::stop_token& stopToken);

    std::filesystem::path checkpointDirectory;
    std::filesystem::path recoveryDirectory;
    std::chrono::milliseconds checkpointInterval;
    bool recoverFromCheckpoint;
    std::optional<uint64_t> recoveredRound;
    uint64_t nextRound = 1;

    std::mutex roundMutex;
    std::mutex mutex;
    CallbackRegistry prepareCallbacks;
    CallbackRegistry commitCallbacks;
    std::map<std::string, PrepareGuard> prepareGuards;
    std::condition_variable_any schedulerWakeup;
    /// Declared last, so that the scheduler stops before the callbacks it runs are destroyed.
    Thread scheduler;
};

}
