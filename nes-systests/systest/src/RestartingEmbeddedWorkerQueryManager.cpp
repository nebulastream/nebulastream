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

#include <RestartingEmbeddedWorkerQueryManager.hpp>

#include <algorithm>
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <netinet/in.h>
#include <string_view>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>
#include <utility>

#include <fmt/format.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <magic_enum/magic_enum.hpp>

#include <Runtime/QueryTerminationType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/SystestTupleCrashControl.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{
namespace
{
constexpr std::string_view BundleNamePrefix = "plan_";
constexpr std::string_view RecoverySnapshotDirectoryName = ".recovery_snapshot";
constexpr auto WorkerStartupTimeout = std::chrono::seconds(10);

LocalQueryStatus makeFailedStatus(const QueryId queryId, const Exception& exception)
{
    LocalQueryStatus status;
    status.queryId = queryId;
    status.state = QueryState::Failed;
    status.metrics.error = exception;
    status.metrics.stop = std::chrono::system_clock::now();
    return status;
}
}

RestartingEmbeddedWorkerQueryManager::RestartingEmbeddedWorkerQueryManager(SingleNodeWorkerConfiguration configuration)
    : configuration(std::move(configuration))
    , checkpointDirectory(this->configuration.workerConfiguration.checkpointConfiguration.checkpointDirectory.getValue())
    , workerBinaryPath(resolveWorkerBinaryPath())
{
}

RestartingEmbeddedWorkerQueryManager::~RestartingEmbeddedWorkerQueryManager()
{
    std::scoped_lock lock(mutex);
    terminateWorkerLocked();
}

std::expected<QueryId, Exception> RestartingEmbeddedWorkerQueryManager::registerQuery(const LogicalPlan& plan)
{
    std::scoped_lock lock(mutex);
    if (backgroundFailure)
    {
        return std::unexpected(*backgroundFailure);
    }

    const auto recoverFromCheckpoint = hasActiveQueriesLocked();
    if (auto available = ensureWorkerAvailableLocked(recoverFromCheckpoint); !available)
    {
        return std::unexpected(available.error());
    }
    setWorkerArmedLocked(false);

    auto runtimeQueryId = worker->registerQuery(plan);
    if (!runtimeQueryId)
    {
        return std::unexpected(runtimeQueryId.error());
    }

    const auto syntheticId = QueryId(nextSyntheticQueryId++);
    nextExpectedRuntimeQueryId = std::max(nextExpectedRuntimeQueryId, runtimeQueryId->getRawValue() + 1);
    queriesBySyntheticId.emplace(
        syntheticId,
        ManagedQuery{
            .syntheticId = syntheticId,
            .bundleQueryId = *runtimeQueryId,
            .currentRuntimeId = *runtimeQueryId,
            .started = false,
            .terminalStatus = std::nullopt});
    syntheticIdByBundleQueryId.emplace(*runtimeQueryId, syntheticId);
    armPending = hasActiveQueriesLocked();
    return syntheticId;
}

std::expected<void, Exception> RestartingEmbeddedWorkerQueryManager::start(const QueryId queryId) noexcept
{
    try
    {
        std::scoped_lock lock(mutex);
        if (backgroundFailure)
        {
            return std::unexpected(*backgroundFailure);
        }

        auto managedQuery = findManagedQueryLocked(queryId);
        if (!managedQuery)
        {
            return std::unexpected(managedQuery.error());
        }

        if (auto available = ensureWorkerAvailableLocked(hasActiveQueriesLocked()); !available)
        {
            return std::unexpected(available.error());
        }
        setWorkerArmedLocked(false);

        if (auto started = worker->start((*managedQuery)->currentRuntimeId); !started)
        {
            return std::unexpected(started.error());
        }
        (*managedQuery)->started = true;
        armPending = hasActiveQueriesLocked();
        return {};
    }
    catch (...)
    {
        return std::unexpected(wrapExternalException());
    }
}

std::expected<void, Exception> RestartingEmbeddedWorkerQueryManager::stop(const QueryId queryId) noexcept
{
    try
    {
        std::scoped_lock lock(mutex);
        if (backgroundFailure)
        {
            return std::unexpected(*backgroundFailure);
        }

        auto managedQuery = findManagedQueryLocked(queryId);
        if (!managedQuery)
        {
            return std::unexpected(managedQuery.error());
        }

        const auto recoverFromCheckpoint = hasActiveQueriesLocked();
        (*managedQuery)->started = false;
        if ((*managedQuery)->terminalStatus)
        {
            return {};
        }

        if (auto available = ensureWorkerAvailableLocked(recoverFromCheckpoint); !available)
        {
            return std::unexpected(available.error());
        }
        setWorkerArmedLocked(false);

        if (auto stopped = worker->stop((*managedQuery)->currentRuntimeId); !stopped)
        {
            return std::unexpected(stopped.error());
        }
        armPending = hasActiveQueriesLocked();
        return {};
    }
    catch (...)
    {
        return std::unexpected(wrapExternalException());
    }
}

std::expected<void, Exception> RestartingEmbeddedWorkerQueryManager::unregister(const QueryId queryId) noexcept
{
    try
    {
        std::scoped_lock lock(mutex);
        auto managedQuery = findManagedQueryLocked(queryId);
        if (!managedQuery)
        {
            return std::unexpected(managedQuery.error());
        }

        auto& query = **managedQuery;
        if (!query.terminalStatus)
        {
            const auto recoverFromCheckpoint = hasActiveQueriesLocked();
            if (auto available = ensureWorkerAvailableLocked(recoverFromCheckpoint); !available)
            {
                return std::unexpected(available.error());
            }
            setWorkerArmedLocked(false);
            if (worker)
            {
                if (auto unregistered = worker->unregister(query.currentRuntimeId); !unregistered)
                {
                    return std::unexpected(unregistered.error());
                }
            }
        }

        removeBundleDirectoriesLocked(query.bundleQueryId);
        syntheticIdByBundleQueryId.erase(query.bundleQueryId);
        queriesBySyntheticId.erase(query.syntheticId);
        armPending = hasActiveQueriesLocked();
        return {};
    }
    catch (...)
    {
        return std::unexpected(wrapExternalException());
    }
}

std::expected<LocalQueryStatus, Exception> RestartingEmbeddedWorkerQueryManager::status(const QueryId queryId) const noexcept
{
    try
    {
        std::scoped_lock lock(mutex);
        auto managedQuery = findManagedQueryLocked(queryId);
        if (!managedQuery)
        {
            return std::unexpected(managedQuery.error());
        }

        auto& query = **managedQuery;
        if (query.terminalStatus)
        {
            return *query.terminalStatus;
        }
        if (backgroundFailure)
        {
            return failedStatusLocked(queryId);
        }

        if (auto available = ensureWorkerAvailableLocked(hasActiveQueriesLocked()); !available)
        {
            return std::unexpected(available.error());
        }
        maybeArmWorkerLocked();

        if (!worker)
        {
            return std::unexpected(TestException("restart-mode worker is unavailable"));
        }

        auto queryStatus = worker->status(query.currentRuntimeId);
        if (!queryStatus)
        {
            refreshWorkerStateLocked();
            return std::unexpected(queryStatus.error());
        }

        queryStatus->queryId = query.syntheticId;
        if (queryStatus->state == QueryState::Stopped || queryStatus->state == QueryState::Failed)
        {
            finalizeTerminalQueryLocked(query, *queryStatus);
            return *query.terminalStatus;
        }
        return *queryStatus;
    }
    catch (...)
    {
        return std::unexpected(wrapExternalException());
    }
}

void RestartingEmbeddedWorkerQueryManager::refreshWorkerStateLocked() const
{
    if (workerPid < 0)
    {
        return;
    }

    int status = 0;
    const auto result = ::waitpid(workerPid, &status, WNOHANG);
    if (result == 0)
    {
        return;
    }

    if (result < 0)
    {
        if (errno == ECHILD)
        {
            workerPid = -1;
            worker.reset();
            workerArmed = false;
            armPending = hasActiveQueriesLocked();
        }
        return;
    }

    if (WIFEXITED(status))
    {
        NES_INFO("Restart-mode worker exited with status {}", WEXITSTATUS(status));
    }
    else if (WIFSIGNALED(status))
    {
        NES_INFO("Restart-mode worker terminated by signal {}", WTERMSIG(status));
    }

    workerPid = -1;
    worker.reset();
    workerArmed = false;
    armPending = hasActiveQueriesLocked();
}

std::expected<void, Exception> RestartingEmbeddedWorkerQueryManager::ensureWorkerAvailableLocked(const bool recoverFromCheckpoint) const
{
    refreshWorkerStateLocked();
    if (worker)
    {
        return {};
    }

    if (!recoverFromCheckpoint)
    {
        return spawnWorkerLocked(false);
    }

    const auto recoverableBundles = listCheckpointBundlesLocked();
    const auto firstRecoveredRuntimeId = INITIAL<QueryId>.getRawValue();
    for (auto& [_, query] : queriesBySyntheticId)
    {
        if (query.started && !query.terminalStatus)
        {
            query.currentRuntimeId = INVALID_QUERY_ID;
        }
    }

    if (auto spawned = spawnWorkerLocked(true); !spawned)
    {
        return spawned;
    }

    auto recoveredRuntimeId = firstRecoveredRuntimeId;
    for (const auto& [bundleName, bundleQueryId] : recoverableBundles)
    {
        const auto assignedRuntimeId = QueryId(recoveredRuntimeId++);
        const auto syntheticIdIt = syntheticIdByBundleQueryId.find(bundleQueryId);
        if (syntheticIdIt == syntheticIdByBundleQueryId.end())
        {
            continue;
        }

        const auto managedQueryIt = queriesBySyntheticId.find(syntheticIdIt->second);
        if (managedQueryIt == queriesBySyntheticId.end() || managedQueryIt->second.terminalStatus)
        {
            continue;
        }

        NES_DEBUG(
            "Mapped recovered checkpoint bundle {} from stable bundle query {} to runtime query {}",
            bundleName,
            bundleQueryId,
            assignedRuntimeId);
        managedQueryIt->second.currentRuntimeId = assignedRuntimeId;
    }
    nextExpectedRuntimeQueryId = recoveredRuntimeId;

    for (auto& [syntheticId, query] : queriesBySyntheticId)
    {
        if (query.terminalStatus || !query.started)
        {
            continue;
        }
        if (query.currentRuntimeId == INVALID_QUERY_ID)
        {
            return std::unexpected(TestException("failed to recover active query {}", syntheticId));
        }
    }

    armPending = hasActiveQueriesLocked();
    return {};
}

std::expected<void, Exception> RestartingEmbeddedWorkerQueryManager::spawnWorkerLocked(const bool recoverFromCheckpoint) const
{
    if (!std::filesystem::exists(workerBinaryPath))
    {
        return std::unexpected(TestException("restart-mode worker binary not found at {}", workerBinaryPath.string()));
    }

    workerUri = URI(fmt::format("localhost:{}", reserveLoopbackPort()));
    auto args = buildWorkerArguments(workerBinaryPath, configuration, workerUri, recoverFromCheckpoint);

    const auto pid = ::fork();
    if (pid < 0)
    {
        return std::unexpected(UnknownException("failed to fork restart-mode worker: {}", std::strerror(errno)));
    }

    if (pid == 0)
    {
        ::setenv(SystestTupleCrashControl::EnabledEnvironmentVariable, "1", 1);
        ::setenv(SystestTupleCrashControl::PauseOnStartupEnvironmentVariable, recoverFromCheckpoint ? "1" : "0", 1);

        std::vector<char*> argv;
        argv.reserve(args.size() + 1);
        for (auto& argument : args)
        {
            argv.push_back(argument.data());
        }
        argv.push_back(nullptr);

        ::execv(workerBinaryPath.c_str(), argv.data());
        std::_Exit(127);
    }

    workerPid = pid;
    workerArmed = false;
    armPending = false;

    const auto channel = grpc::CreateChannel(workerUri.toString(), grpc::InsecureChannelCredentials());
    const auto startupDeadline = std::chrono::steady_clock::now() + WorkerStartupTimeout;
    while (std::chrono::steady_clock::now() < startupDeadline)
    {
        if (channel->WaitForConnected(std::chrono::system_clock::now() + std::chrono::milliseconds(200)))
        {
            worker = std::make_unique<GRPCQueryManager>(channel);
            return {};
        }

        refreshWorkerStateLocked();
        if (workerPid < 0)
        {
            return std::unexpected(TestException("restart-mode worker exited before accepting gRPC connections"));
        }
    }

    terminateWorkerLocked();
    return std::unexpected(TestException("timed out waiting for restart-mode worker to accept gRPC connections"));
}

void RestartingEmbeddedWorkerQueryManager::terminateWorkerLocked() const
{
    worker.reset();
    if (workerPid >= 0)
    {
        ::kill(workerPid, SIGTERM);
        ::waitpid(workerPid, nullptr, 0);
    }
    workerPid = -1;
    workerArmed = false;
    armPending = false;
}

void RestartingEmbeddedWorkerQueryManager::setWorkerArmedLocked(const bool armed) const
{
    if (workerPid < 0 || workerArmed == armed)
    {
        return;
    }

    const auto signal = armed ? SystestTupleCrashControl::ArmSignal : SystestTupleCrashControl::DisarmSignal;
    if (::kill(workerPid, signal) != 0)
    {
        if (errno == ESRCH)
        {
            refreshWorkerStateLocked();
            return;
        }
        backgroundFailure = UnknownException("failed to signal restart-mode worker: {}", std::strerror(errno));
        return;
    }

    workerArmed = armed;
    armPending = !armed && hasActiveQueriesLocked();
}

void RestartingEmbeddedWorkerQueryManager::maybeArmWorkerLocked() const
{
    if (!armPending || !hasActiveQueriesLocked() || !hasRecoverableBundlesForActiveQueriesLocked())
    {
        return;
    }
    setWorkerArmedLocked(true);
}

bool RestartingEmbeddedWorkerQueryManager::hasActiveQueriesLocked() const
{
    return std::ranges::any_of(queriesBySyntheticId, [](const auto& entry)
    {
        const auto& query = entry.second;
        return query.started && !query.terminalStatus.has_value();
    });
}

bool RestartingEmbeddedWorkerQueryManager::hasRecoverableBundlesForActiveQueriesLocked() const
{
    std::vector<QueryId> bundleQueryIds;
    bundleQueryIds.reserve(queriesBySyntheticId.size());
    for (const auto& [_, query] : queriesBySyntheticId)
    {
        if (query.started && !query.terminalStatus)
        {
            bundleQueryIds.push_back(query.bundleQueryId);
        }
    }

    if (bundleQueryIds.empty())
    {
        return false;
    }

    const auto checkpointBundles = listCheckpointBundlesLocked();
    return std::ranges::all_of(bundleQueryIds, [&](const QueryId bundleQueryId)
    {
        return std::ranges::any_of(checkpointBundles, [&](const auto& bundle) { return bundle.second == bundleQueryId; });
    });
}

std::vector<std::pair<std::string, QueryId>> RestartingEmbeddedWorkerQueryManager::listCheckpointBundlesLocked() const
{
    std::vector<std::pair<std::string, QueryId>> bundles;
    std::error_code errorCode;
    for (std::filesystem::directory_iterator iterator(checkpointDirectory, errorCode);
         !errorCode && iterator != std::filesystem::directory_iterator();
         ++iterator)
    {
        if (!iterator->is_directory() || iterator->path().filename() == RecoverySnapshotDirectoryName)
        {
            continue;
        }

        const auto bundleName = iterator->path().filename().string();
        if (const auto bundleQueryId = parseBundleQueryId(bundleName))
        {
            bundles.emplace_back(bundleName, *bundleQueryId);
        }
    }
    if (errorCode)
    {
        throw TestException("failed to inspect checkpoint bundles in {}: {}", checkpointDirectory.string(), errorCode.message());
    }

    std::ranges::sort(bundles, [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    return bundles;
}

std::optional<QueryId> RestartingEmbeddedWorkerQueryManager::parseBundleQueryId(const std::string_view bundleName)
{
    if (!bundleName.starts_with(BundleNamePrefix))
    {
        return std::nullopt;
    }

    const auto suffix = bundleName.substr(BundleNamePrefix.size());
    const auto separator = suffix.find('_');
    if (separator == std::string_view::npos)
    {
        return std::nullopt;
    }

    QueryId::Underlying queryIdValue = 0;
    const auto numberPart = suffix.substr(0, separator);
    const auto [ptr, errorCode] = std::from_chars(numberPart.data(), numberPart.data() + numberPart.size(), queryIdValue);
    if (errorCode != std::errc{} || ptr != numberPart.data() + numberPart.size())
    {
        return std::nullopt;
    }
    return QueryId(queryIdValue);
}

std::string RestartingEmbeddedWorkerQueryManager::bundlePrefix(const QueryId queryId)
{
    return std::string(BundleNamePrefix) + std::to_string(queryId.getRawValue()) + "_";
}

void RestartingEmbeddedWorkerQueryManager::removeBundleDirectoriesLocked(const QueryId bundleQueryId) const
{
    const auto removeMatchingBundles = [&](const std::filesystem::path& root)
    {
        if (!std::filesystem::exists(root))
        {
            return;
        }

        std::error_code errorCode;
        for (std::filesystem::directory_iterator iterator(root, errorCode);
             !errorCode && iterator != std::filesystem::directory_iterator();
             ++iterator)
        {
            if (!iterator->is_directory())
            {
                continue;
            }
            const auto bundleName = iterator->path().filename().string();
            if (bundleName.starts_with(bundlePrefix(bundleQueryId)))
            {
                std::filesystem::remove_all(iterator->path(), errorCode);
            }
            if (errorCode)
            {
                throw TestException("failed to remove checkpoint bundle directory {}: {}", iterator->path().string(), errorCode.message());
            }
        }
        if (errorCode)
        {
            throw TestException("failed to inspect checkpoint bundle root {}: {}", root.string(), errorCode.message());
        }
    };

    removeMatchingBundles(checkpointDirectory);
    removeMatchingBundles(checkpointDirectory / RecoverySnapshotDirectoryName);
}

void RestartingEmbeddedWorkerQueryManager::finalizeTerminalQueryLocked(ManagedQuery& query, LocalQueryStatus status) const
{
    status.queryId = query.syntheticId;
    query.started = false;
    query.terminalStatus = status;
    if (worker && query.currentRuntimeId != INVALID_QUERY_ID)
    {
        const auto unregisterResult = worker->unregister(query.currentRuntimeId);
        if (!unregisterResult)
        {
            NES_WARNING("Failed to unregister completed restart-mode query {}: {}", query.currentRuntimeId, unregisterResult.error().what());
        }
    }
    query.currentRuntimeId = INVALID_QUERY_ID;
    removeBundleDirectoriesLocked(query.bundleQueryId);
    armPending = hasActiveQueriesLocked();
}

std::expected<RestartingEmbeddedWorkerQueryManager::ManagedQuery*, Exception>
RestartingEmbeddedWorkerQueryManager::findManagedQueryLocked(const QueryId syntheticId) const
{
    if (const auto it = queriesBySyntheticId.find(syntheticId); it != queriesBySyntheticId.end())
    {
        return std::addressof(const_cast<ManagedQuery&>(it->second));
    }
    return std::unexpected(QueryNotFound("{}", syntheticId));
}

std::expected<LocalQueryStatus, Exception> RestartingEmbeddedWorkerQueryManager::failedStatusLocked(const QueryId syntheticId) const
{
    INVARIANT(backgroundFailure.has_value(), "failedStatusLocked requires a background failure");
    return makeFailedStatus(syntheticId, *backgroundFailure);
}

std::filesystem::path RestartingEmbeddedWorkerQueryManager::resolveWorkerBinaryPath()
{
    return std::filesystem::path(PATH_TO_BINARY_DIR) / "nes-single-node-worker" / "nes-single-node-worker";
}

uint16_t RestartingEmbeddedWorkerQueryManager::reserveLoopbackPort()
{
    const auto socketFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd < 0)
    {
        throw TestException("failed to create socket for restart-mode worker port allocation: {}", std::strerror(errno));
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (::bind(socketFd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0)
    {
        const auto error = std::strerror(errno);
        ::close(socketFd);
        throw TestException("failed to bind loopback port for restart-mode worker: {}", error);
    }

    socklen_t addressLength = sizeof(address);
    if (::getsockname(socketFd, reinterpret_cast<sockaddr*>(&address), &addressLength) != 0)
    {
        const auto error = std::strerror(errno);
        ::close(socketFd);
        throw TestException("failed to query loopback port for restart-mode worker: {}", error);
    }

    ::close(socketFd);
    return ntohs(address.sin_port);
}

std::vector<std::string> RestartingEmbeddedWorkerQueryManager::buildWorkerArguments(
    const std::filesystem::path& workerBinaryPath,
    const SingleNodeWorkerConfiguration& configuration,
    const URI& workerUri,
    const bool recoverFromCheckpoint)
{
    const auto& workerConfiguration = configuration.workerConfiguration;
    const auto& queryEngine = workerConfiguration.queryEngine;
    const auto& queryExecution = workerConfiguration.defaultQueryExecution;
    const auto& checkpoint = workerConfiguration.checkpointConfiguration;

    return {
        workerBinaryPath.string(),
        fmt::format("--grpc={}", workerUri.toString()),
        fmt::format("--enable_event_trace={}", boolString(configuration.enableGoogleEventTrace.getValue())),
        fmt::format("--worker.query_engine.number_of_worker_threads={}", queryEngine.numberOfWorkerThreads.getValue()),
        fmt::format("--worker.query_engine.admission_queue_size={}", queryEngine.admissionQueueSize.getValue()),
        fmt::format("--worker.default_query_execution.execution_mode={}", magic_enum::enum_name(queryExecution.executionMode.getValue())),
        fmt::format("--worker.default_query_execution.number_of_partitions={}", queryExecution.numberOfPartitions.getValue()),
        fmt::format("--worker.default_query_execution.page_size={}", queryExecution.pageSize.getValue()),
        fmt::format("--worker.default_query_execution.number_of_records_per_key={}", queryExecution.numberOfRecordsPerKey.getValue()),
        fmt::format("--worker.default_query_execution.max_number_of_buckets={}", queryExecution.maxNumberOfBuckets.getValue()),
        fmt::format("--worker.default_query_execution.operator_buffer_size={}", queryExecution.operatorBufferSize.getValue()),
        fmt::format("--worker.default_query_execution.join_strategy={}", magic_enum::enum_name(queryExecution.joinStrategy.getValue())),
        fmt::format(
            "--worker.number_of_buffers_in_global_buffer_manager={}",
            workerConfiguration.numberOfBuffersInGlobalBufferManager.getValue()),
        fmt::format("--worker.default_max_inflight_buffers={}", workerConfiguration.defaultMaxInflightBuffers.getValue()),
        fmt::format(
            "--worker.dump_compilation_result={}",
            magic_enum::enum_name(workerConfiguration.dumpQueryCompilationIR.getValue())),
        fmt::format("--worker.dump_graph={}", boolString(workerConfiguration.dumpGraph.getValue())),
        fmt::format(
            "--worker.enable_compilation_cache={}",
            boolString(workerConfiguration.enableCompilationCache.getValue())),
        fmt::format("--worker.compilation_cache_dir={}", workerConfiguration.compilationCacheDir.getValue()),
        fmt::format("--worker.checkpoint.checkpoint_directory={}", checkpoint.checkpointDirectory.getValue()),
        fmt::format("--worker.checkpoint.checkpoint_interval_ms={}", checkpoint.checkpointIntervalMs.getValue()),
        fmt::format("--worker.checkpoint.recover_from_checkpoint={}", boolString(recoverFromCheckpoint))};
}

std::string RestartingEmbeddedWorkerQueryManager::boolString(const bool value)
{
    return value ? "true" : "false";
}
}
