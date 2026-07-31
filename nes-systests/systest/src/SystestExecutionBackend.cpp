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

#include <SystestExecutionBackend.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <variant>
#include <vector>

#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QuerySubmitter.hpp>
#include <SystestResolver.hpp>
#include <WorkerCatalog.hpp>

namespace NES::Systest
{
namespace
{

bool isBackendError(const ErrorCode code)
{
    return code == ErrorCode::QueryStartFailed || code == ErrorCode::QueryStopFailed || code == ErrorCode::QueryStatusFailed
        || code == ErrorCode::QueryNotFound || code == ErrorCode::CannotSerialize || code == ErrorCode::CannotDeserialize
        || code == ErrorCode::UnknownException;
}

}

ExecutionError runtimeExecutionError(const Exception& exception)
{
    return ExecutionError{
        .kind = isBackendError(exception.code()) ? ExecutionErrorKind::Backend : ExecutionErrorKind::Statement,
        .details = {{.code = exception.code(), .message = exception.what()}}};
}

ExecutionError runtimeExecutionError(const DistributedException& exception)
{
    ExecutionError result{.kind = ExecutionErrorKind::Statement, .details = {}};
    for (const auto& [host, exceptions] : exception.details())
    {
        for (const auto& detail : exceptions)
        {
            if (isBackendError(detail.code()))
            {
                result.kind = ExecutionErrorKind::Backend;
            }
            result.details.push_back(ExecutionErrorDetail{.code = detail.code(), .message = fmt::format("{}: {}", host, detail.what())});
        }
    }
    if (result.details.empty())
    {
        result.kind = ExecutionErrorKind::Backend;
        result.details.push_back(ExecutionErrorDetail{.code = ErrorCode::QueryStatusFailed, .message = exception.what()});
    }
    return result;
}

namespace
{

BackendFault backendFault(const Exception& exception)
{
    return BackendFault{.kind = BackendFaultKind::Failure, .code = exception.code(), .message = exception.what()};
}

BackendFault backendFault(const std::exception& exception)
{
    return BackendFault{.kind = BackendFaultKind::Failure, .code = ErrorCode::UnknownException, .message = exception.what()};
}

const PreparedStatement* preparedStatement(const PreparedAction& action, const ExecutionRole role)
{
    if (const auto* query = std::get_if<PreparedQuery>(&action))
    {
        return role == ExecutionRole::Primary ? &query->statement : nullptr;
    }
    if (const auto* differential = std::get_if<PreparedDifferential>(&action))
    {
        return role == ExecutionRole::Primary ? &differential->primary : &differential->differential;
    }
    return nullptr;
}

ExecutionMetrics executionMetrics(
    const DistributedQueryStatusSnapshot& status,
    const PreparedStatement& statement,
    const std::chrono::system_clock::time_point fallbackStart,
    const bool collectInputMetrics)
{
    ExecutionMetrics result;
    const auto metrics = status.coalesceQueryMetrics();
    result.started = metrics.running ? metrics.running : metrics.start;
    if (!result.started)
    {
        result.started = fallbackStart;
    }
    result.finished = metrics.stop;
    if (!result.finished)
    {
        result.finished = std::chrono::system_clock::now();
    }

    if (!collectInputMetrics)
    {
        return result;
    }
    for (const auto& source : statement.sourceMetrics)
    {
        std::error_code error;
        if (!std::filesystem::is_regular_file(source.file, error) || error)
        {
            continue;
        }
        result.bytesProcessed += std::filesystem::file_size(source.file, error) * source.occurrences;
        if (error)
        {
            result.bytesProcessed = 0;
            continue;
        }
        std::ifstream input(source.file);
        result.tuplesProcessed
            += static_cast<uint64_t>(std::count(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>(), '\n'))
            * source.occurrences;
    }
    return result;
}

ArtifactSet artifactsFor(const OutputTarget& target)
{
    if (target.kind == OutputTargetKind::Table && !target.file.empty())
    {
        return ArtifactSet{.files = {target.file}};
    }
    return {};
}

StatementOutput outputFor(const OutputTarget& target)
{
    switch (target.kind)
    {
        case OutputTargetKind::Table:
            return TableArtifact{.file = target.file};
        case OutputTargetKind::Text:
            return TextArtifact{};
        case OutputTargetKind::Discard:
            return DiscardedOutput{};
    }
    std::unreachable();
}

class QueryManagerExecutionSession final : public ExecutionSession
{
public:
    QueryManagerExecutionSession(
        std::unique_ptr<QueryManager> queryManager,
        std::shared_ptr<const PreparedExecutionCatalog> preparedExecutions,
        const EnvironmentId environment)
        : submitter(std::move(queryManager)), preparedExecutions(std::move(preparedExecutions)), environment(environment)
    {
    }

    std::expected<std::variant<ExecutionHandle, StatementFailure>, BackendFault>
    start(const ExecutionRequest& request, const std::stop_token stopToken) override
    {
        if (stopToken.stop_requested())
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Cancelled,
                .code = ErrorCode::QueryStatusFailed,
                .message = "Execution was cancelled before the statement started"});
        }
        if (std::chrono::steady_clock::now() >= request.deadline)
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::DeadlineReached,
                .code = ErrorCode::QueryStatusFailed,
                .message = "Execution deadline was reached before the statement started"});
        }

        const auto* execution = preparedExecutions->find(request.testCase);
        if (execution == nullptr)
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Failure,
                .code = ErrorCode::TestException,
                .message = fmt::format(
                    "No prepared plan for {}:{} variant {}",
                    request.testCase.source.relativeTestFile,
                    request.testCase.source.queryNumber,
                    request.testCase.configurationVariant)});
        }
        if (execution->environment != environment)
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Failure,
                .code = ErrorCode::TestException,
                .message = "Prepared execution belongs to a different execution environment"});
        }
        if (const auto* failure = std::get_if<PlanningFailure>(&execution->prepared))
        {
            return std::variant<ExecutionHandle, StatementFailure>{StatementFailure{
                .stage = ExecutionStage::Planning,
                .error
                = ExecutionError{.kind = ExecutionErrorKind::Statement, .details = {{.code = failure->code, .message = failure->message}}},
                .artifacts = {}}};
        }

        const auto& action = **std::get_if<std::shared_ptr<const PreparedAction>>(&execution->prepared);
        const auto handle = ExecutionHandle{.value = nextHandle++};
        if (const auto* explain = std::get_if<PreparedExplain>(&action))
        {
            if (request.role != ExecutionRole::Primary || request.output.kind != OutputTargetKind::Text || !request.output.file.empty()
                || request.sql != explain->sql)
            {
                return std::unexpected(BackendFault{
                    .kind = BackendFaultKind::Failure,
                    .code = ErrorCode::TestException,
                    .message = "Execution request does not match its prepared EXPLAIN action"});
            }
            observedCompletions.push_back(
                StatementCompletion{.handle = handle, .result = TextArtifact{.text = explain->output}, .metrics = {}, .artifacts = {}});
            return std::variant<ExecutionHandle, StatementFailure>{handle};
        }

        const auto* statement = preparedStatement(action, request.role);
        if (statement == nullptr)
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Failure,
                .code = ErrorCode::TestException,
                .message = "Execution role does not match its prepared action"});
        }
        if (request.output != statement->output || request.sql != statement->sql)
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Failure,
                .code = ErrorCode::TestException,
                .message = "Execution request SQL or output does not match its prepared plan"});
        }

        auto plan = statement->plan;
        plan.setQueryId(DistributedQueryId(DistributedQueryId::INVALID));

        try
        {
            auto started = submitter.startQuery(plan, request.deadline, stopToken, request.cancellationGracePeriod);
            if (!started)
            {
                if (submitter.failedTeardownDeadline())
                {
                    auto fault = backendFault(started.error());
                    fault.kind = BackendFaultKind::TeardownFailed;
                    return std::unexpected(std::move(fault));
                }
                if (stopToken.stop_requested())
                {
                    return std::unexpected(BackendFault{
                        .kind = BackendFaultKind::Cancelled, .code = started.error().code(), .message = started.error().what()});
                }
                if (std::chrono::steady_clock::now() >= request.deadline)
                {
                    return std::unexpected(BackendFault{
                        .kind = BackendFaultKind::DeadlineReached, .code = started.error().code(), .message = started.error().what()});
                }
                if (isBackendError(started.error().code()))
                {
                    return std::unexpected(backendFault(started.error()));
                }
                return std::variant<ExecutionHandle, StatementFailure>{StatementFailure{
                    .stage = ExecutionStage::Starting,
                    .error = runtimeExecutionError(started.error()),
                    .artifacts = artifactsFor(request.output)}};
            }
            active.emplace(handle, ActiveStatement{.request = request, .queryId = *started, .started = std::chrono::system_clock::now()});
            handlesByQuery.emplace(*started, handle);
            return std::variant<ExecutionHandle, StatementFailure>{handle};
        }
        catch (const Exception& exception)
        {
            if (isBackendError(exception.code()))
            {
                return std::unexpected(backendFault(exception));
            }
            return std::variant<ExecutionHandle, StatementFailure>{StatementFailure{
                .stage = ExecutionStage::Starting, .error = runtimeExecutionError(exception), .artifacts = artifactsFor(request.output)}};
        }
        catch (const std::exception& exception)
        {
            return std::unexpected(backendFault(exception));
        }
    }

    std::expected<StatementCompletion, BackendFault> waitAny(
        const std::span<const ExecutionHandle> requested,
        const std::chrono::steady_clock::time_point deadline,
        const std::stop_token stopToken) override
    {
        const auto isRequested = [&](const ExecutionHandle handle) { return std::ranges::find(requested, handle) != requested.end(); };
        const auto takeObservedCompletion = [&]() -> std::optional<StatementCompletion>
        {
            const auto completion = std::ranges::find_if(observedCompletions, [&](const auto& item) { return isRequested(item.handle); });
            if (completion == observedCompletions.end())
            {
                return std::nullopt;
            }
            auto result = std::move(*completion);
            observedCompletions.erase(completion);
            return result;
        };
        const auto rememberFault = [&](BackendFault fault)
        {
            if (!pendingWaitFault)
            {
                pendingWaitFault = std::move(fault);
            }
        };
        const auto rememberException = [&](const Exception& exception)
        {
            if (stopToken.stop_requested())
            {
                rememberFault(BackendFault{.kind = BackendFaultKind::Cancelled, .code = exception.code(), .message = exception.what()});
                return;
            }
            if (std::chrono::steady_clock::now() >= deadline)
            {
                rememberFault(
                    BackendFault{.kind = BackendFaultKind::DeadlineReached, .code = exception.code(), .message = exception.what()});
                return;
            }
            auto fault = backendFault(exception);
            if (submitter.failedTeardownDeadline())
            {
                fault.kind = BackendFaultKind::TeardownFailed;
            }
            rememberFault(std::move(fault));
        };
        const auto rememberStandardException = [&](const std::exception& exception)
        {
            if (stopToken.stop_requested())
            {
                rememberFault(
                    BackendFault{.kind = BackendFaultKind::Cancelled, .code = ErrorCode::UnknownException, .message = exception.what()});
                return;
            }
            if (std::chrono::steady_clock::now() >= deadline)
            {
                rememberFault(BackendFault{
                    .kind = BackendFaultKind::DeadlineReached, .code = ErrorCode::UnknownException, .message = exception.what()});
                return;
            }
            auto fault = backendFault(exception);
            if (submitter.failedTeardownDeadline())
            {
                fault.kind = BackendFaultKind::TeardownFailed;
            }
            rememberFault(std::move(fault));
        };
        const auto returnPendingFault = [&]() -> std::expected<StatementCompletion, BackendFault>
        {
            auto fault = std::move(*pendingWaitFault);
            pendingWaitFault.reset();
            return std::unexpected(std::move(fault));
        };

        while (true)
        {
            if (auto completion = takeObservedCompletion())
            {
                return std::move(*completion);
            }
            if (pendingWaitFault)
            {
                return returnPendingFault();
            }
            if (stopToken.stop_requested())
            {
                return std::unexpected(BackendFault{
                    .kind = BackendFaultKind::Cancelled, .code = ErrorCode::QueryStatusFailed, .message = "Execution wait was cancelled"});
            }
            if (std::chrono::steady_clock::now() >= deadline)
            {
                return std::unexpected(BackendFault{
                    .kind = BackendFaultKind::DeadlineReached,
                    .code = ErrorCode::QueryStatusFailed,
                    .message = "Execution wait deadline reached"});
            }

            try
            {
                auto poll = submitter.pollFinishedQueriesRetained(deadline, stopToken);
                for (auto& status : poll.completions)
                {
                    try
                    {
                        const auto handle = handlesByQuery.at(status.queryId);
                        const auto statementIterator = active.find(handle);
                        INVARIANT(statementIterator != active.end(), "Missing active statement for query {}", status.queryId);
                        if (status.getGlobalQueryStatus() == DistributedQueryStatus::Stopped)
                        {
                            submitter.releaseQuery(status.queryId);
                        }
                        else
                        {
                            submitter.cleanupQuery(
                                status.queryId,
                                std::chrono::steady_clock::now() + statementIterator->second.request.cancellationGracePeriod,
                                {});
                        }
                        auto statement = active.extract(statementIterator);
                        handlesByQuery.erase(status.queryId);
                        const auto artifacts = artifactsFor(statement.mapped().request.output);
                        const auto& execution = preparedExecutions->at(statement.mapped().request.testCase);
                        const auto& action = **std::get_if<std::shared_ptr<const PreparedAction>>(&execution.prepared);
                        const auto* prepared = preparedStatement(action, statement.mapped().request.role);
                        INVARIANT(prepared != nullptr, "Active execution must have a prepared statement");
                        const auto metrics
                            = executionMetrics(status, *prepared, statement.mapped().started, statement.mapped().request.collectMetrics);

                        StatementCompletion completion{
                            .handle = handle,
                            .result = outputFor(statement.mapped().request.output),
                            .metrics = metrics,
                            .artifacts = artifacts};
                        if (status.getGlobalQueryStatus() != DistributedQueryStatus::Stopped)
                        {
                            const auto exception = status.coalesceException();
                            completion.result = StatementFailure{
                                .stage = ExecutionStage::Running,
                                .error = exception ? runtimeExecutionError(*exception)
                                                   : runtimeExecutionError(QueryStatusFailed("Query failed without an exception")),
                                .artifacts = artifacts};
                        }
                        observedCompletions.push_back(std::move(completion));
                    }
                    catch (const Exception& exception)
                    {
                        rememberException(exception);
                    }
                    catch (const std::exception& exception)
                    {
                        rememberStandardException(exception);
                    }
                }
                if (poll.error)
                {
                    rememberException(*poll.error);
                }
            }
            catch (const Exception& exception)
            {
                rememberException(exception);
            }
            catch (const std::exception& exception)
            {
                rememberStandardException(exception);
            }

            if (auto completion = takeObservedCompletion())
            {
                return std::move(*completion);
            }
            if (pendingWaitFault)
            {
                return returnPendingFault();
            }
            if (stopToken.stop_requested())
            {
                return std::unexpected(BackendFault{
                    .kind = BackendFaultKind::Cancelled, .code = ErrorCode::QueryStatusFailed, .message = "Execution wait was cancelled"});
            }
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline)
            {
                return std::unexpected(BackendFault{
                    .kind = BackendFaultKind::DeadlineReached,
                    .code = ErrorCode::QueryStatusFailed,
                    .message = "Execution wait deadline reached"});
            }
            std::this_thread::sleep_for(
                std::min(std::chrono::milliseconds(25), std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now)));
        }
    }

    std::expected<void, BackendFault> cancel(const ExecutionHandle handle, const std::chrono::steady_clock::time_point deadline) override
    {
        std::erase_if(observedCompletions, [&](const StatementCompletion& completion) { return completion.handle == handle; });
        const auto statement = active.find(handle);
        if (statement == active.end())
        {
            return {};
        }
        try
        {
            submitter.cleanupQuery(statement->second.queryId, deadline, {});
            handlesByQuery.erase(statement->second.queryId);
            active.erase(statement);
            return {};
        }
        catch (const Exception& exception)
        {
            return std::unexpected(backendFault(exception));
        }
        catch (const std::exception& exception)
        {
            return std::unexpected(backendFault(exception));
        }
    }

    std::expected<void, BackendFault> close(const std::chrono::steady_clock::time_point deadline) override
    {
        const auto failedTeardownDeadline = submitter.failedTeardownDeadline();
        const auto effectiveDeadline = failedTeardownDeadline ? std::min(deadline, *failedTeardownDeadline) : deadline;
        std::optional<BackendFault> firstFailure;
        const auto handles = active | std::views::keys | std::ranges::to<std::vector>();
        for (const auto handle : handles)
        {
            if (auto cancelled = cancel(handle, effectiveDeadline); !cancelled && !firstFailure)
            {
                firstFailure = cancelled.error();
            }
        }
        observedCompletions.clear();
        pendingWaitFault.reset();
        try
        {
            submitter.cleanup(effectiveDeadline);
        }
        catch (const Exception& exception)
        {
            if (!firstFailure)
            {
                firstFailure = backendFault(exception);
            }
        }
        catch (const std::exception& exception)
        {
            if (!firstFailure)
            {
                firstFailure = backendFault(exception);
            }
        }
        try
        {
            submitter.shutdown(effectiveDeadline);
        }
        catch (const Exception& exception)
        {
            if (!firstFailure)
            {
                auto failure = backendFault(exception);
                failure.kind = BackendFaultKind::TeardownFailed;
                firstFailure = std::move(failure);
            }
        }
        catch (const std::exception& exception)
        {
            if (!firstFailure)
            {
                auto failure = backendFault(exception);
                failure.kind = BackendFaultKind::TeardownFailed;
                firstFailure = std::move(failure);
            }
        }
        if (firstFailure)
        {
            return std::unexpected(std::move(*firstFailure));
        }
        return {};
    }

private:
    struct ActiveStatement
    {
        ExecutionRequest request;
        DistributedQueryId queryId{DistributedQueryId::INVALID};
        std::chrono::system_clock::time_point started;
    };

    QuerySubmitter submitter;
    std::shared_ptr<const PreparedExecutionCatalog> preparedExecutions;
    EnvironmentId environment;
    uint64_t nextHandle = 1;
    std::map<ExecutionHandle, ActiveStatement> active;
    std::map<DistributedQueryId, ExecutionHandle> handlesByQuery;
    std::vector<StatementCompletion> observedCompletions;
    std::optional<BackendFault> pendingWaitFault;
};

}

std::string ExecutionError::message() const
{
    return fmt::format("{}", fmt::join(details | std::views::transform([](const auto& detail) { return detail.message; }), "; "));
}

EmbeddedExecutionBackend::EmbeddedExecutionBackend(
    std::shared_ptr<const PreparedExecutionCatalog> preparedExecutions, SingleNodeWorkerConfiguration baseConfiguration)
    : preparedExecutions(std::move(preparedExecutions)), baseConfiguration(std::move(baseConfiguration))
{
}

BackendCapabilities EmbeddedExecutionBackend::capabilities() const
{
    return BackendCapabilities{
        .supportsConfigurationOverrides = true,
        .supportsRemoteFixtures = true,
        .supportsExplain = true,
        .maximumConcurrency = std::numeric_limits<size_t>::max()};
}

std::expected<std::unique_ptr<ExecutionSession>, BackendFault> EmbeddedExecutionBackend::open(const EnvironmentSpec& environment)
{
    try
    {
        auto configuration = baseConfiguration;
        for (const auto& [key, value] : environment.configuration.values)
        {
            configuration.overwriteConfigWithCommandLineInput({{key, value}});
        }
        auto workerCatalog = std::make_shared<WorkerCatalog>(environment.cluster.workers);
        auto queryManager = std::make_unique<QueryManager>(std::move(workerCatalog), createEmbeddedBackend(configuration));
        return std::unique_ptr<ExecutionSession>{
            std::make_unique<QueryManagerExecutionSession>(std::move(queryManager), preparedExecutions, environment.id)};
    }
    catch (const Exception& exception)
    {
        return std::unexpected(backendFault(exception));
    }
    catch (const std::exception& exception)
    {
        return std::unexpected(backendFault(exception));
    }
}

RemoteExecutionBackend::RemoteExecutionBackend(std::shared_ptr<const PreparedExecutionCatalog> preparedExecutions)
    : preparedExecutions(std::move(preparedExecutions))
{
}

BackendCapabilities RemoteExecutionBackend::capabilities() const
{
    return BackendCapabilities{
        .supportsConfigurationOverrides = false,
        .supportsRemoteFixtures = true,
        .supportsExplain = true,
        .maximumConcurrency = std::numeric_limits<size_t>::max()};
}

std::expected<std::unique_ptr<ExecutionSession>, BackendFault> RemoteExecutionBackend::open(const EnvironmentSpec& environment)
{
    if (!environment.configuration.values.empty())
    {
        return std::unexpected(BackendFault{
            .kind = BackendFaultKind::Failure,
            .code = ErrorCode::InvalidConfigParameter,
            .message = "Remote execution does not support worker configuration overrides"});
    }
    try
    {
        auto workerCatalog = std::make_shared<WorkerCatalog>(environment.cluster.workers);
        auto queryManager = std::make_unique<QueryManager>(std::move(workerCatalog), createGRPCBackend());
        return std::unique_ptr<ExecutionSession>{
            std::make_unique<QueryManagerExecutionSession>(std::move(queryManager), preparedExecutions, environment.id)};
    }
    catch (const Exception& exception)
    {
        return std::unexpected(backendFault(exception));
    }
    catch (const std::exception& exception)
    {
        return std::unexpected(backendFault(exception));
    }
}

}
