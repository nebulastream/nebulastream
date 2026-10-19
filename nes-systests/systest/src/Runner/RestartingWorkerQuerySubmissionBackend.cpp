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

#include <Runner/RestartingWorkerQuerySubmissionBackend.hpp>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstring>
#include <expected>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>
#include <fcntl.h>
#include <unistd.h>
#include <Configurations/OptionVisitor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Runtime/CheckpointManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/UUID.hpp>
#include <fmt/format.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Version.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

extern char** environ; /// NOLINT(readability-redundant-declaration)

namespace NES
{
namespace
{
constexpr auto WorkerStartupTimeout = std::chrono::seconds(60);
constexpr auto WorkerCrashTimeout = std::chrono::seconds(2);
constexpr auto CheckpointInterval = std::chrono::milliseconds(10);
/// Defined by the SystestTupleFile source, which crashes the worker.
constexpr std::string_view CrashEveryNTuplesVariable = "NES_SYSTEST_CRASH_EVERY_N_TUPLES";
constexpr int CrashExitCode = 86;

/// Collects the explicitly set options of a configuration as command line arguments for the worker.
class CommandLineArguments final : public OptionVisitor
{
public:
    void visitOption(const OptionInfo& info) override
    {
        if (not info.isExplicitlySet or info.currentValue.empty())
        {
            return;
        }
        std::string argument = "--";
        for (const auto& section : sections)
        {
            argument += section + ".";
        }
        arguments.push_back(fmt::format("{}{}={}", argument, info.name, info.currentValue));
    }

    void pushSection(const std::string_view sectionName) override { sections.emplace_back(sectionName); }

    void popSection() override { sections.pop_back(); }

    void push() override { }

    void pop() override { }

    std::vector<std::string> arguments;

private:
    std::vector<std::string> sections;
};

uint16_t reserveLoopbackPort()
{
    const auto socketDescriptor = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socketDescriptor < 0)
    {
        throw TestException("Cannot create a socket to find a free port: {}", std::strerror(errno));
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t addressLength = sizeof(address);
    /// NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
    if (::bind(socketDescriptor, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0
        or ::getsockname(socketDescriptor, reinterpret_cast<sockaddr*>(&address), &addressLength) != 0)
    /// NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
    {
        const auto error = errno;
        ::close(socketDescriptor);
        throw TestException("Cannot find a free port: {}", std::strerror(error));
    }
    ::close(socketDescriptor);
    return ntohs(address.sin_port);
}

bool isTerminated(const LocalQueryStatusSnapshot& status)
{
    return status.state == QueryStatus::Stopped or status.state == QueryStatus::Failed;
}
}

RestartingWorkerQuerySubmissionBackend::RestartingWorkerQuerySubmissionBackend(
    const WorkerConfig& config,
    const SingleNodeWorkerConfiguration& workerConfiguration,
    const uint64_t crashEveryNTuples,
    std::filesystem::path workingDirectory)
    : workerConfiguration(config.config)
    , crashEveryNTuples(crashEveryNTuples)
    , workingDirectory(std::move(workingDirectory))
    , checkpointDirectory(this->workingDirectory / "checkpoints")
{
    this->workerConfiguration.applyExplicitlySetFrom(workerConfiguration);
    auto& checkpoint = this->workerConfiguration.workerConfiguration.checkpoint;
    checkpoint.checkpointDirectory.setValue(checkpointDirectory.string());
    checkpoint.checkpointIntervalMs.setValue(CheckpointInterval.count());
    /// Every restart recompiles the queries of the worker, which the compilation cache keeps from dominating the runtime.
    if (auto& compilationCache = this->workerConfiguration.workerConfiguration.compilationCache;
        not compilationCache.enabled.isExplicitlySet())
    {
        compilationCache.enabled.setValue(true);
        compilationCache.cacheDir.setValue((this->workingDirectory / "compilation-cache").string());
    }
    std::filesystem::remove_all(checkpointDirectory);
    std::filesystem::create_directories(this->workingDirectory);
    const std::scoped_lock lock(mutex);
    startWorker(false);
}

RestartingWorkerQuerySubmissionBackend::~RestartingWorkerQuerySubmissionBackend()
{
    const std::scoped_lock lock(mutex);
    stopWorker();
}

void RestartingWorkerQuerySubmissionBackend::startWorker(const bool recoverFromCheckpoint) const
{
    /// Recovered queries may read enough tuples to crash the worker again before it accepts requests.
    for (auto recover = recoverFromCheckpoint; not launchWorker(recover); recover = true)
    {
        ++numberOfRestarts;
    }
}

bool RestartingWorkerQuerySubmissionBackend::launchWorker(const bool recoverFromCheckpoint) const
{
    auto configuration = workerConfiguration;
    const auto grpcAddress = fmt::format("localhost:{}", reserveLoopbackPort());
    configuration.grpcAddressUri.setValue(grpcAddress);
    configuration.dataAddress.setValue(fmt::format("localhost:{}", reserveLoopbackPort()));
    configuration.workerConfiguration.checkpoint.recoverFromCheckpoint.setValue(recoverFromCheckpoint);
    CommandLineArguments arguments;
    configuration.accept(arguments);

    const std::string workerBinary = PATH_TO_BINARY_DIR "/nes-single-node-worker/nes-single-node-worker";
    std::vector<std::string> argumentStrings{workerBinary, "--"};
    argumentStrings.insert(argumentStrings.end(), arguments.arguments.begin(), arguments.arguments.end());
    std::vector<char*> argv;
    for (auto& argument : argumentStrings)
    {
        argv.push_back(argument.data());
    }
    argv.push_back(nullptr);

    auto crashVariable = fmt::format("{}={}", CrashEveryNTuplesVariable, crashEveryNTuples);
    std::vector<char*> envp;
    for (auto** variable = environ; *variable != nullptr; ++variable) /// NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    {
        envp.push_back(*variable);
    }
    envp.push_back(crashVariable.data());
    envp.push_back(nullptr);

    const auto workerDirectory = workingDirectory / "restarting-worker";
    std::filesystem::create_directories(workerDirectory);
    const auto workerDirectoryString = workerDirectory.string();
    const auto workerOutputFile = (workerDirectory / "worker.out").string();
    NES_INFO("Starting worker {} at {} (recover: {})", numberOfRestarts, grpcAddress, recoverFromCheckpoint);

    workerProcess = ::fork();
    if (workerProcess < 0)
    {
        throw TestException("Cannot fork the worker: {}", std::strerror(errno));
    }
    if (workerProcess == 0)
    {
        /// Only async-signal-safe calls are allowed between fork and exec in a multi-threaded process.
        const auto output
            = ::open(workerOutputFile.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644); /// NOLINT(cppcoreguidelines-pro-type-vararg)
        if (output >= 0 and ::dup2(output, STDOUT_FILENO) >= 0 and ::dup2(output, STDERR_FILENO) >= 0
            and ::chdir(workerDirectoryString.c_str()) == 0)
        {
            ::execve(argv[0], argv.data(), envp.data());
        }
        ::_exit(127);
    }

    worker = std::make_unique<GRPCQuerySubmissionBackend>(WorkerConfig{
        .host = Host(grpcAddress),
        .dataAddress = std::string(configuration.dataAddress.getValue()),
        .maxOperators = CapacityKind::Unlimited{},
        .downstream = {},
        .config = configuration});
    const auto deadline = std::chrono::steady_clock::now() + WorkerStartupTimeout;
    while (not worker->version())
    {
        if (reapCrashedWorker())
        {
            return false;
        }
        if (std::chrono::steady_clock::now() > deadline)
        {
            throw TestException("The worker did not start within {} s", WorkerStartupTimeout.count());
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return true;
}

void RestartingWorkerQuerySubmissionBackend::stopWorker() const
{
    worker.reset();
    if (workerProcess > 0)
    {
        ::kill(workerProcess, SIGTERM);
        ::waitpid(workerProcess, nullptr, 0);
        workerProcess = -1;
    }
}

bool RestartingWorkerQuerySubmissionBackend::reapCrashedWorker() const
{
    int status = 0;
    if (workerProcess <= 0 or ::waitpid(workerProcess, &status, WNOHANG) != workerProcess)
    {
        return false;
    }
    workerProcess = -1;
    if (not WIFEXITED(status) or WEXITSTATUS(status) != CrashExitCode)
    {
        throw TestException("The worker terminated unexpectedly with status {}", status);
    }
    return true;
}

bool RestartingWorkerQuerySubmissionBackend::waitForWorkerCrash() const
{
    for (const auto deadline = std::chrono::steady_clock::now() + WorkerCrashTimeout; std::chrono::steady_clock::now() < deadline;)
    {
        if (reapCrashedWorker())
        {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}

void RestartingWorkerQuerySubmissionBackend::ensureWorkerIsRunning() const
{
    if (workerProcess > 0 and not reapCrashedWorker())
    {
        return;
    }
    ++numberOfRestarts;
    startWorker(true);
}

void RestartingWorkerQuerySubmissionBackend::removeCheckpoint(const QueryId& queryId) const
{
    const auto queryDirectoryName = fmt::format("{}{}", CheckpointManager::QueryDirectoryPrefix, queryId.getLocalQueryId().getRawValue());
    std::error_code errorCode;
    std::filesystem::remove_all(checkpointDirectory / queryDirectoryName, errorCode);
    std::filesystem::remove_all(checkpointDirectory / ".recovery_snapshot" / queryDirectoryName, errorCode);
}

std::expected<QueryId, Exception> RestartingWorkerQuerySubmissionBackend::start(LogicalPlan plan)
{
    const std::scoped_lock lock(mutex);
    if (plan.getQueryId().getLocalQueryId() == INVALID_LOCAL_QUERY_ID)
    {
        plan.setQueryId(QueryId::create(LocalQueryId(generateUUID()), plan.getQueryId().getDistributedQueryId()));
    }
    const auto queryId = plan.getQueryId();
    while (true)
    {
        ensureWorkerIsRunning();
        restartsWhenStarted.insert_or_assign(queryId, numberOfRestarts);
        auto started = worker->start(plan);
        if (started or not waitForWorkerCrash())
        {
            return started;
        }
        /// The worker may have persisted the query before it crashed, in which case it recovered the query.
        ensureWorkerIsRunning();
        if (worker->status(queryId))
        {
            return queryId;
        }
    }
}

std::expected<void, Exception> RestartingWorkerQuerySubmissionBackend::stop(QueryId queryId)
{
    const std::scoped_lock lock(mutex);
    while (not terminatedQueries.contains(queryId))
    {
        ensureWorkerIsRunning();
        if (auto stopped = worker->stop(queryId); stopped or not waitForWorkerCrash())
        {
            return stopped;
        }
    }
    return {};
}

std::expected<LocalQueryStatusSnapshot, Exception> RestartingWorkerQuerySubmissionBackend::status(QueryId queryId) const
{
    const std::scoped_lock lock(mutex);
    while (not terminatedQueries.contains(queryId))
    {
        ensureWorkerIsRunning();
        auto queryStatus = worker->status(queryId);
        if (not queryStatus)
        {
            if (waitForWorkerCrash())
            {
                continue;
            }
            /// Recovery restarts every query that did not stop, so a restarted worker only forgets the queries that stopped before it
            /// crashed and before this backend saw them stop.
            if (queryStatus.error().code() == ErrorCode::QueryNotFound and restartsWhenStarted.contains(queryId)
                and restartsWhenStarted.at(queryId) < numberOfRestarts)
            {
                const auto now = std::chrono::system_clock::now();
                terminatedQueries.emplace(
                    queryId,
                    LocalQueryStatusSnapshot{
                        .queryId = queryId,
                        .state = QueryStatus::Stopped,
                        .metrics = {.start = now, .running = now, .stop = now, .error = {}}});
                removeCheckpoint(queryId);
                continue;
            }
            return queryStatus;
        }
        if (not isTerminated(*queryStatus))
        {
            return queryStatus;
        }
        removeCheckpoint(queryId);
        terminatedQueries.emplace(queryId, *queryStatus);
    }
    return terminatedQueries.at(queryId);
}

std::expected<WorkerStatus, Exception>
RestartingWorkerQuerySubmissionBackend::workerStatus(const std::chrono::system_clock::time_point after) const
{
    const std::scoped_lock lock(mutex);
    ensureWorkerIsRunning();
    return worker->workerStatus(after);
}

std::expected<VersionInfo, Exception> RestartingWorkerQuerySubmissionBackend::version() const
{
    const std::scoped_lock lock(mutex);
    ensureWorkerIsRunning();
    return worker->version();
}

BackendProvider createRestartingWorkerBackend(
    const SingleNodeWorkerConfiguration& workerConfiguration,
    const uint64_t crashEveryNTuples,
    const std::filesystem::path& workingDirectory)
{
    return [workerConfiguration, crashEveryNTuples, workingDirectory](const WorkerConfig& config)
    { return std::make_unique<RestartingWorkerQuerySubmissionBackend>(config, workerConfiguration, crashEveryNTuples, workingDirectory); };
}

}
