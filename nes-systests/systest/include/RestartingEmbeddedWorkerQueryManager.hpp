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

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <sys/types.h>
#include <unordered_map>
#include <vector>

#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQueryManager.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Util/URI.hpp>

namespace NES::Systest
{
class RestartingEmbeddedWorkerQueryManager final : public QueryManager
{
public:
    explicit RestartingEmbeddedWorkerQueryManager(SingleNodeWorkerConfiguration configuration);
    ~RestartingEmbeddedWorkerQueryManager() override;

    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(const LogicalPlan& plan) override;
    std::expected<void, Exception> start(QueryId queryId) noexcept override;
    std::expected<void, Exception> stop(QueryId queryId) noexcept override;
    std::expected<void, Exception> unregister(QueryId queryId) noexcept override;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(QueryId queryId) const noexcept override;

private:
    struct ManagedQuery
    {
        QueryId syntheticId = INVALID_QUERY_ID;
        QueryId bundleQueryId = INVALID_QUERY_ID;
        QueryId currentRuntimeId = INVALID_QUERY_ID;
        bool started = false;
        std::optional<LocalQueryStatus> terminalStatus;
    };

    void refreshWorkerStateLocked() const;
    [[nodiscard]] std::expected<void, Exception> ensureWorkerAvailableLocked(bool recoverFromCheckpoint) const;
    [[nodiscard]] std::expected<void, Exception> spawnWorkerLocked(bool recoverFromCheckpoint) const;
    void terminateWorkerLocked() const;
    void setWorkerArmedLocked(bool armed) const;
    void maybeArmWorkerLocked() const;
    [[nodiscard]] bool hasActiveQueriesLocked() const;
    [[nodiscard]] bool hasRecoverableBundlesForActiveQueriesLocked() const;
    [[nodiscard]] std::vector<std::pair<std::string, QueryId>> listCheckpointBundlesLocked() const;
    static std::optional<QueryId> parseBundleQueryId(std::string_view bundleName);
    static std::string bundlePrefix(QueryId queryId);
    void removeBundleDirectoriesLocked(QueryId bundleQueryId) const;
    void finalizeTerminalQueryLocked(ManagedQuery& query, LocalQueryStatus status) const;
    [[nodiscard]] std::expected<ManagedQuery*, Exception> findManagedQueryLocked(QueryId syntheticId) const;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> failedStatusLocked(QueryId syntheticId) const;
    [[nodiscard]] static std::filesystem::path resolveWorkerBinaryPath();
    [[nodiscard]] static uint16_t reserveLoopbackPort();
    [[nodiscard]] static std::vector<std::string> buildWorkerArguments(
        const std::filesystem::path& workerBinaryPath,
        const SingleNodeWorkerConfiguration& configuration,
        const URI& workerUri,
        bool recoverFromCheckpoint);
    [[nodiscard]] static std::string boolString(bool value);

    SingleNodeWorkerConfiguration configuration;
    std::filesystem::path checkpointDirectory;
    std::filesystem::path workerBinaryPath;
    mutable std::mutex mutex;
    mutable std::unique_ptr<GRPCQueryManager> worker;
    mutable std::unordered_map<QueryId, ManagedQuery> queriesBySyntheticId;
    mutable std::unordered_map<QueryId, QueryId> syntheticIdByBundleQueryId;
    mutable std::optional<Exception> backgroundFailure;
    QueryId::Underlying nextSyntheticQueryId = INITIAL<QueryId>.getRawValue();
    mutable QueryId::Underlying nextExpectedRuntimeQueryId = INITIAL<QueryId>.getRawValue();
    mutable pid_t workerPid = -1;
    mutable URI workerUri;
    mutable bool workerArmed = false;
    mutable bool armPending = false;
};
}
