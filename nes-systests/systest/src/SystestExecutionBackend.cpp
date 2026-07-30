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

ExecutionError executionError(const Exception& exception)
{
    return ExecutionError{.kind = ExecutionErrorKind::Statement, .details = {{.code = exception.code(), .message = exception.what()}}};
}

ExecutionError executionError(const DistributedException& exception)
{
    ExecutionError result{.kind = ExecutionErrorKind::Statement, .details = {}};
    for (const auto& [host, exceptions] : exception.details())
    {
        for (const auto& detail : exceptions)
        {
            result.details.push_back(ExecutionErrorDetail{.code = detail.code(), .message = fmt::format("{}: {}", host, detail.what())});
        }
    }
    if (result.details.empty())
    {
        result.details.push_back(ExecutionErrorDetail{.code = ErrorCode::QueryStatusFailed, .message = exception.what()});
    }
    return result;
}

bool isBackendError(const ErrorCode code)
{
    return code == ErrorCode::QueryStartFailed || code == ErrorCode::QueryStopFailed || code == ErrorCode::QueryStatusFailed
        || code == ErrorCode::QueryNotFound || code == ErrorCode::CannotSerialize || code == ErrorCode::UnknownException;
}

BackendFault backendFault(const Exception& exception)
{
    return BackendFault{.kind = BackendFaultKind::Failure, .code = exception.code(), .message = exception.what()};
}

BackendFault backendFault(const std::exception& exception)
{
    return BackendFault{.kind = BackendFaultKind::Failure, .code = ErrorCode::UnknownException, .message = exception.what()};
}

std::string_view expectedSql(const ResolvedCase& testCase, const ExecutionRole role)
{
    if (const auto* query = std::get_if<QueryAction>(&testCase.action))
    {
        return role == ExecutionRole::Primary ? std::string_view{query->sql} : std::string_view{};
    }
    const auto& differential = std::get<DifferentialAction>(testCase.action);
    return role == ExecutionRole::Primary ? std::string_view{differential.leftSql} : std::string_view{differential.rightSql};
}

OutputTarget expectedOutput(const PreparedCase& prepared, const ExecutionRole role)
{
    if (const auto* action = std::get_if<QueryAction>(&prepared.definition->action))
    {
        if (action->kind == QueryKind::Explain)
        {
            return OutputTarget{.kind = OutputTargetKind::Text, .file = {}};
        }
        if (const auto* rows = std::get_if<RowsExpectation>(&prepared.definition->expectation); rows && rows->outputDiscarded)
        {
            return OutputTarget{.kind = OutputTargetKind::Discard, .file = {}};
        }
        return OutputTarget{.kind = OutputTargetKind::Table, .file = prepared.query.resultFile()};
    }
    return OutputTarget{
        .kind = OutputTargetKind::Table,
        .file = role == ExecutionRole::Primary ? prepared.query.resultFile() : prepared.query.resultFileForDifferentialQuery()};
}

ExecutionMetrics executionMetrics(
    const DistributedQueryStatusSnapshot& status,
    const SystestQuery& query,
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

    if (!collectInputMetrics || !query.planInfoOrException)
    {
        return result;
    }
    for (const auto& [source, occurrences] : query.planInfoOrException->sourcesToFilePathsAndCounts | std::views::values)
    {
        const auto path = source.getRawValue();
        std::error_code error;
        if (!std::filesystem::is_regular_file(path, error) || error)
        {
            continue;
        }
        result.bytesProcessed += std::filesystem::file_size(path, error) * occurrences;
        if (error)
        {
            result.bytesProcessed = 0;
            continue;
        }
        std::ifstream input(path);
        result.tuplesProcessed
            += static_cast<uint64_t>(std::count(std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>(), '\n'))
            * occurrences;
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
        std::shared_ptr<const PreparedCaseCatalog> preparedCases,
        const EnvironmentId environment)
        : submitter(std::move(queryManager)), preparedCases(std::move(preparedCases)), environment(environment)
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

        const auto* prepared = preparedCases->find(request.testCase);
        if (prepared == nullptr)
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
        if (prepared->definition->environment != environment)
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Failure,
                .code = ErrorCode::TestException,
                .message = "Prepared case belongs to a different execution environment"});
        }
        const auto expectedStatement = expectedSql(*prepared->definition, request.role);
        if (!expectedStatement.empty() && request.sql != expectedStatement)
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Failure,
                .code = ErrorCode::TestException,
                .message = "Execution request SQL does not match its resolved case"});
        }
        const auto output = expectedOutput(*prepared, request.role);
        if (request.output.kind != output.kind || request.output.file != output.file)
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Failure,
                .code = ErrorCode::TestException,
                .message = "Execution request output does not match its prepared plan"});
        }

        const auto handle = ExecutionHandle{.value = nextHandle++};
        if (const auto* queryAction = std::get_if<QueryAction>(&prepared->definition->action);
            queryAction && queryAction->kind == QueryKind::Explain)
        {
            if (prepared->query.actualExplainOutput)
            {
                syntheticCompletions.push_back(StatementCompletion{
                    .handle = handle,
                    .result = TextArtifact{.text = *prepared->query.actualExplainOutput},
                    .metrics = {},
                    .artifacts = {}});
                return std::variant<ExecutionHandle, StatementFailure>{handle};
            }
            const auto error = prepared->query.planInfoOrException ? executionError(TestException("EXPLAIN statement produced no output"))
                                                                   : executionError(prepared->query.planInfoOrException.error());
            return std::variant<ExecutionHandle, StatementFailure>{
                StatementFailure{.stage = ExecutionStage::Planning, .error = error, .artifacts = {}}};
        }

        if (!prepared->query.planInfoOrException)
        {
            return std::variant<ExecutionHandle, StatementFailure>{StatementFailure{
                .stage = ExecutionStage::Planning,
                .error = executionError(prepared->query.planInfoOrException.error()),
                .artifacts = artifactsFor(request.output)}};
        }

        std::optional<DistributedLogicalPlan> plan;
        if (request.role == ExecutionRole::Primary)
        {
            plan = prepared->query.planInfoOrException->queryPlan;
        }
        else if (prepared->query.differentialQueryPlan)
        {
            plan = *prepared->query.differentialQueryPlan;
        }
        else
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Failure,
                .code = ErrorCode::TestException,
                .message = "Differential execution requested for a non-differential case"});
        }
        plan->setQueryId(DistributedQueryId(DistributedQueryId::INVALID));

        try
        {
            auto started = submitter.startQuery(*plan, request.deadline, stopToken, request.cancellationGracePeriod);
            if (!started)
            {
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
                    auto fault = backendFault(started.error());
                    if (submitter.failedTeardownDeadline())
                    {
                        fault.kind = BackendFaultKind::TeardownFailed;
                    }
                    return std::unexpected(std::move(fault));
                }
                return std::variant<ExecutionHandle, StatementFailure>{StatementFailure{
                    .stage = ExecutionStage::Starting,
                    .error = executionError(started.error()),
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
                .stage = ExecutionStage::Starting, .error = executionError(exception), .artifacts = artifactsFor(request.output)}};
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
        while (true)
        {
            if (const auto completion
                = std::ranges::find_if(syntheticCompletions, [&](const auto& item) { return isRequested(item.handle); });
                completion != syntheticCompletions.end())
            {
                auto result = std::move(*completion);
                syntheticCompletions.erase(completion);
                return result;
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
                for (auto& status : submitter.pollFinishedQueriesRetained(deadline, stopToken))
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
                    const auto metrics = executionMetrics(
                        status,
                        preparedCases->at(statement.mapped().request.testCase).query,
                        statement.mapped().started,
                        statement.mapped().request.collectMetrics);

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
                            .error = exception ? executionError(*exception)
                                               : executionError(QueryStatusFailed("Query failed without an exception")),
                            .artifacts = artifacts};
                    }
                    pendingCompletions.push_back(std::move(completion));
                }
            }
            catch (const Exception& exception)
            {
                if (stopToken.stop_requested())
                {
                    return std::unexpected(
                        BackendFault{.kind = BackendFaultKind::Cancelled, .code = exception.code(), .message = exception.what()});
                }
                if (std::chrono::steady_clock::now() >= deadline)
                {
                    return std::unexpected(
                        BackendFault{.kind = BackendFaultKind::DeadlineReached, .code = exception.code(), .message = exception.what()});
                }
                auto fault = backendFault(exception);
                if (submitter.failedTeardownDeadline())
                {
                    fault.kind = BackendFaultKind::TeardownFailed;
                }
                return std::unexpected(std::move(fault));
            }
            catch (const std::exception& exception)
            {
                if (stopToken.stop_requested())
                {
                    return std::unexpected(BackendFault{
                        .kind = BackendFaultKind::Cancelled, .code = ErrorCode::UnknownException, .message = exception.what()});
                }
                if (std::chrono::steady_clock::now() >= deadline)
                {
                    return std::unexpected(BackendFault{
                        .kind = BackendFaultKind::DeadlineReached, .code = ErrorCode::UnknownException, .message = exception.what()});
                }
                auto fault = backendFault(exception);
                if (submitter.failedTeardownDeadline())
                {
                    fault.kind = BackendFaultKind::TeardownFailed;
                }
                return std::unexpected(std::move(fault));
            }

            if (const auto completion
                = std::ranges::find_if(pendingCompletions, [&](const auto& item) { return isRequested(item.handle); });
                completion != pendingCompletions.end())
            {
                auto result = std::move(*completion);
                pendingCompletions.erase(completion);
                return result;
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
        if (const auto synthetic = std::ranges::find(syntheticCompletions, handle, &StatementCompletion::handle);
            synthetic != syntheticCompletions.end())
        {
            syntheticCompletions.erase(synthetic);
            return {};
        }
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
        syntheticCompletions.clear();
        pendingCompletions.clear();
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
        submitter.shutdown(effectiveDeadline);
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
    std::shared_ptr<const PreparedCaseCatalog> preparedCases;
    EnvironmentId environment;
    uint64_t nextHandle = 1;
    std::map<ExecutionHandle, ActiveStatement> active;
    std::map<DistributedQueryId, ExecutionHandle> handlesByQuery;
    std::vector<StatementCompletion> syntheticCompletions;
    std::vector<StatementCompletion> pendingCompletions;
};

}

std::string ExecutionError::message() const
{
    return fmt::format("{}", fmt::join(details | std::views::transform([](const auto& detail) { return detail.message; }), "; "));
}

EmbeddedExecutionBackend::EmbeddedExecutionBackend(
    std::shared_ptr<const PreparedCaseCatalog> preparedCases, SingleNodeWorkerConfiguration baseConfiguration)
    : preparedCases(std::move(preparedCases)), baseConfiguration(std::move(baseConfiguration))
{
}

BackendCapabilities EmbeddedExecutionBackend::capabilities() const
{
    return BackendCapabilities{
        .supportsConfigurationOverrides = true,
        .supportsRemoteFixtures = false,
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
            std::make_unique<QueryManagerExecutionSession>(std::move(queryManager), preparedCases, environment.id)};
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

RemoteExecutionBackend::RemoteExecutionBackend(std::shared_ptr<const PreparedCaseCatalog> preparedCases)
    : preparedCases(std::move(preparedCases))
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
            std::make_unique<QueryManagerExecutionSession>(std::move(queryManager), preparedCases, environment.id)};
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
