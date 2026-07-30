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

#include <SystestCoordinator.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <ranges>
#include <set>
#include <span>
#include <stop_token>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <SystestExecutionBackend.hpp>
#include <SystestQueryModel.hpp>
#include <SystestResolver.hpp>
#include <SystestRun.hpp>
#include <SystestValidation.hpp>

namespace NES::Systest
{
namespace
{

ExecutionError executionError(const BackendFault& fault)
{
    return ExecutionError{.kind = ExecutionErrorKind::Backend, .details = {{.code = fault.code, .message = fault.message}}};
}

void mergeArtifacts(ArtifactSet& destination, const ArtifactSet& source)
{
    for (const auto& file : source.files)
    {
        if (std::ranges::find(destination.files, file) == destination.files.end())
        {
            destination.files.push_back(file);
        }
    }
}

void mergeMetrics(ExecutionMetrics& destination, const ExecutionMetrics& source)
{
    if (source.started && (!destination.started || *source.started < *destination.started))
    {
        destination.started = source.started;
    }
    if (source.finished && (!destination.finished || *source.finished > *destination.finished))
    {
        destination.finished = source.finished;
    }
    destination.bytesProcessed += source.bytesProcessed;
    destination.tuplesProcessed += source.tuplesProcessed;
}

std::chrono::steady_clock::time_point
caseDeadline(const std::chrono::steady_clock::time_point started, const std::chrono::milliseconds timeout)
{
    if (timeout == std::chrono::milliseconds::max())
    {
        return std::chrono::steady_clock::time_point::max();
    }
    return started + timeout;
}

ExecutionRequest requestFor(
    const ResolvedCase& testCase,
    const PreparedCaseCatalog& preparedCases,
    const ExecutionRole role,
    const bool collectMetrics,
    const std::chrono::steady_clock::time_point deadline,
    const std::chrono::milliseconds cancellationGracePeriod)
{
    const auto& query = preparedCases.at(testCase.id).query;
    ExecutionRequest request{
        .testCase = testCase.id,
        .role = role,
        .sql = {},
        .output = {},
        .collectMetrics = collectMetrics,
        .deadline = deadline,
        .cancellationGracePeriod = cancellationGracePeriod};
    if (const auto* action = std::get_if<QueryAction>(&testCase.action))
    {
        request.sql = action->sql;
        if (action->kind == QueryKind::Explain)
        {
            request.output = OutputTarget{.kind = OutputTargetKind::Text, .file = {}};
        }
        else if (const auto* rows = std::get_if<RowsExpectation>(&testCase.expectation); rows && rows->outputDiscarded)
        {
            request.output = OutputTarget{.kind = OutputTargetKind::Discard, .file = {}};
        }
        else
        {
            request.output = OutputTarget{.kind = OutputTargetKind::Table, .file = query.resultFile()};
        }
        return request;
    }

    const auto& action = std::get<DifferentialAction>(testCase.action);
    request.sql = role == ExecutionRole::Primary ? action.leftSql : action.rightSql;
    request.output = OutputTarget{
        .kind = OutputTargetKind::Table,
        .file = role == ExecutionRole::Primary ? query.resultFile() : query.resultFileForDifferentialQuery()};
    return request;
}

struct ActiveHandle
{
    ExecutionHandle handle;
    ExecutionRole role = ExecutionRole::Primary;
};

struct ActiveCase
{
    ScheduledCase scheduled;
    std::chrono::steady_clock::time_point started;
    std::chrono::steady_clock::time_point deadline;
    std::vector<ActiveHandle> handles;
    std::optional<StatementOutput> primaryOutput;
    std::optional<StatementOutput> differentialOutput;
    ExecutionMetrics metrics;
    ArtifactSet artifacts;
    std::set<std::filesystem::path> outputFiles;
};

std::optional<BackendFault>
cancelHandles(ExecutionSession& session, const std::vector<ActiveHandle>& handles, const std::chrono::steady_clock::time_point deadline)
{
    std::optional<BackendFault> firstFailure;
    for (const auto& handle : handles)
    {
        if (auto cancelled = session.cancel(handle.handle, deadline); !cancelled && !firstFailure)
        {
            firstFailure = cancelled.error();
        }
    }
    return firstFailure;
}

}

RunSummary RunCoordinator::run(
    const ResolvedRun& resolvedRun,
    RunSetup setup,
    ExecutionBackend& backend,
    const CaseValidator& validator,
    RunReporter& reporter,
    const std::stop_token stopToken) const
{
    const auto runStartedAt = std::chrono::steady_clock::now();
    const auto runDeadline
        = setup.deadlines.runTimeout ? runStartedAt + *setup.deadlines.runTimeout : std::chrono::steady_clock::time_point::max();
    if (setup.ordering.kind == OrderingKind::Shuffled && !setup.ordering.seed)
    {
        setup.ordering.seed = std::random_device{}();
    }
    std::mt19937_64 random(setup.ordering.seed.value_or(0));
    RunSummary summary{
        .setup = setup, .results = {}, .passed = 0, .failed = 0, .skipped = 0, .elapsed = {}, .cancelled = false, .diagnostics = {}};
    if (auto published = reporter.publish(RunStarted{.plan = setup}); !published)
    {
        summary.diagnostics.push_back(
            Diagnostic{.kind = DiagnosticKind::Reporting, .message = published.error().message, .source = std::nullopt});
    }

    uint64_t sequenceNumber = 0;
    bool failFast = false;
    const auto record = [&](const ResolvedCase& testCase,
                            const ExecutionOutcome& outcome,
                            std::map<TestCaseId, Verdict>& verdicts,
                            std::optional<Diagnostic> additionalDiagnostic = std::nullopt)
    {
        auto validated = setup.validation.enabled
            ? validator.validate(testCase, outcome)
            : ValidatedResult{
                  .id = testCase.id,
                  .verdict = std::holds_alternative<SkippedExecution>(outcome) ? Verdict::Skipped
                      : std::holds_alternative<CompletedExecution>(outcome)    ? Verdict::Passed
                                                                               : Verdict::Failed,
                  .diagnostics = {},
                  .metrics = std::holds_alternative<CompletedExecution>(outcome) ? std::get<CompletedExecution>(outcome).metrics
                                                                                 : ExecutionMetrics{},
                  .artifacts = std::visit(
                      [](const auto& execution) -> ArtifactSet
                      {
                          if constexpr (requires { execution.artifacts; })
                          {
                              return execution.artifacts;
                          }
                          return {};
                      },
                      outcome)};
        if (additionalDiagnostic)
        {
            validated.diagnostics.push_back(std::move(*additionalDiagnostic));
        }
        verdicts[testCase.id] = validated.verdict;
        switch (validated.verdict)
        {
            case Verdict::Passed:
                ++summary.passed;
                break;
            case Verdict::Failed:
                ++summary.failed;
                failFast = failFast || setup.failurePolicy == IndependentFailurePolicy::FailFast;
                break;
            case Verdict::Skipped:
                ++summary.skipped;
                break;
        }
        if (auto published = reporter.publish(CaseFinished{.result = validated}); !published)
        {
            summary.diagnostics.push_back(
                Diagnostic{.kind = DiagnosticKind::Reporting, .message = published.error().message, .source = testCase.source});
        }
        if (!std::holds_alternative<UntilCancelled>(setup.repetition) || validated.verdict != Verdict::Passed)
        {
            summary.results.push_back(std::move(validated));
        }
    };

    size_t repetition = 0;
    const auto shouldRunAnotherRepetition = [&]
    {
        if (std::holds_alternative<Once>(setup.repetition))
        {
            return repetition == 0;
        }
        if (const auto* fixed = std::get_if<FixedRepetitions>(&setup.repetition))
        {
            return repetition < fixed->count;
        }
        return !stopToken.stop_requested() && !failFast;
    };

    while (shouldRunAnotherRepetition())
    {
        ++repetition;
        const auto completedBeforeRepetition = summary.passed + summary.failed;
        std::map<TestCaseId, Verdict> verdicts;
        for (const auto& environment : resolvedRun.environments)
        {
            std::vector<std::shared_ptr<const ResolvedCase>> remaining = resolvedRun.cases
                | std::views::filter([&](const auto& testCase)
                                     { return testCase->environment == environment.id && setup.selection.contains(testCase->id); })
                | std::ranges::to<std::vector>();
            if (remaining.empty())
            {
                continue;
            }
            if (setup.ordering.kind == OrderingKind::Shuffled)
            {
                std::ranges::shuffle(remaining, random);
            }

            if (stopToken.stop_requested() || std::chrono::steady_clock::now() >= runDeadline || failFast)
            {
                summary.cancelled = stopToken.stop_requested() || std::chrono::steady_clock::now() >= runDeadline;
                for (const auto& testCase : remaining)
                {
                    record(*testCase, SkippedExecution{.id = testCase->id, .failedDependencies = {}}, verdicts);
                }
                continue;
            }

            const auto capabilities = backend.capabilities();
            if (!capabilities.supportsConfigurationOverrides && !environment.configuration.values.empty())
            {
                for (const auto& testCase : remaining)
                {
                    auto outcome = SkippedExecution{.id = testCase->id, .failedDependencies = {}};
                    record(
                        *testCase,
                        outcome,
                        verdicts,
                        Diagnostic{
                            .kind = DiagnosticKind::Scheduling,
                            .message = "Backend does not support configuration overrides",
                            .source = testCase->source});
                }
                continue;
            }

            auto opened = backend.open(environment);
            if (!opened)
            {
                for (const auto& testCase : remaining)
                {
                    record(
                        *testCase,
                        FailedExecution{
                            .id = testCase->id,
                            .stage = ExecutionStage::Starting,
                            .error = executionError(opened.error()),
                            .artifacts = {}},
                        verdicts);
                }
                continue;
            }
            auto session = std::move(*opened);
            const auto concurrency = std::min(setup.concurrency.maximumActiveCases, capabilities.maximumConcurrency);
            if (concurrency == 0)
            {
                for (const auto& testCase : remaining)
                {
                    record(
                        *testCase,
                        FailedExecution{
                            .id = testCase->id,
                            .stage = ExecutionStage::Starting,
                            .error
                            = ExecutionError{.kind = ExecutionErrorKind::Backend, .details = {{.code = ErrorCode::InvalidConfigParameter, .message = "maximumActiveCases must be greater than zero"}}},
                            .artifacts = {}},
                        verdicts);
                }
                continue;
            }

            std::vector<ActiveCase> active;
            bool sessionFatal = false;
            std::optional<std::chrono::steady_clock::time_point> environmentCloseDeadline;
            while (!remaining.empty() || !active.empty())
            {
                const auto schedulerNow = std::chrono::steady_clock::now();
                if (sessionFatal)
                {
                    summary.cancelled = true;
                    failFast = true;
                    const auto cancellationDeadline
                        = environmentCloseDeadline.value_or(std::chrono::steady_clock::now() + setup.deadlines.cancellationGracePeriod);
                    environmentCloseDeadline = cancellationDeadline;
                    for (auto& activeCase : active)
                    {
                        if (auto cancellationFailure = cancelHandles(*session, activeCase.handles, cancellationDeadline))
                        {
                            record(
                                *activeCase.scheduled.definition,
                                FailedExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .stage = ExecutionStage::Cancelling,
                                    .error = executionError(*cancellationFailure),
                                    .artifacts = activeCase.artifacts},
                                verdicts);
                        }
                        else
                        {
                            record(
                                *activeCase.scheduled.definition,
                                SkippedExecution{.id = activeCase.scheduled.definition->id, .failedDependencies = {}},
                                verdicts);
                        }
                    }
                    active.clear();
                    for (const auto& testCase : remaining)
                    {
                        record(*testCase, SkippedExecution{.id = testCase->id, .failedDependencies = {}}, verdicts);
                    }
                    remaining.clear();
                    break;
                }
                if (stopToken.stop_requested() || schedulerNow >= runDeadline)
                {
                    summary.cancelled = true;
                    const auto cancellationDeadline
                        = environmentCloseDeadline.value_or(schedulerNow + setup.deadlines.cancellationGracePeriod);
                    environmentCloseDeadline = cancellationDeadline;
                    for (auto& activeCase : active)
                    {
                        if (auto cancellationFailure = cancelHandles(*session, activeCase.handles, cancellationDeadline))
                        {
                            record(
                                *activeCase.scheduled.definition,
                                FailedExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .stage = ExecutionStage::Cancelling,
                                    .error = executionError(*cancellationFailure),
                                    .artifacts = activeCase.artifacts},
                                verdicts);
                        }
                        else if (schedulerNow >= runDeadline)
                        {
                            record(
                                *activeCase.scheduled.definition,
                                TimedOutExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .stage = ExecutionStage::Running,
                                    .elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(schedulerNow - activeCase.started),
                                    .artifacts = activeCase.artifacts},
                                verdicts);
                        }
                        else
                        {
                            record(
                                *activeCase.scheduled.definition,
                                SkippedExecution{.id = activeCase.scheduled.definition->id, .failedDependencies = {}},
                                verdicts);
                        }
                    }
                    active.clear();
                    for (const auto& testCase : remaining)
                    {
                        record(*testCase, SkippedExecution{.id = testCase->id, .failedDependencies = {}}, verdicts);
                    }
                    remaining.clear();
                    break;
                }
                if (failFast)
                {
                    const auto cancellationDeadline = std::chrono::steady_clock::now() + setup.deadlines.cancellationGracePeriod;
                    environmentCloseDeadline = cancellationDeadline;
                    std::vector<TestCaseId> failedCases = verdicts
                        | std::views::filter([](const auto& entry) { return entry.second == Verdict::Failed; }) | std::views::keys
                        | std::ranges::to<std::vector>();
                    for (auto& activeCase : active)
                    {
                        if (auto cancellationFailure = cancelHandles(*session, activeCase.handles, cancellationDeadline))
                        {
                            record(
                                *activeCase.scheduled.definition,
                                FailedExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .stage = ExecutionStage::Cancelling,
                                    .error = executionError(*cancellationFailure),
                                    .artifacts = activeCase.artifacts},
                                verdicts);
                        }
                        else
                        {
                            record(
                                *activeCase.scheduled.definition,
                                SkippedExecution{.id = activeCase.scheduled.definition->id, .failedDependencies = failedCases},
                                verdicts);
                        }
                    }
                    active.clear();
                    for (const auto& testCase : remaining)
                    {
                        record(*testCase, SkippedExecution{.id = testCase->id, .failedDependencies = failedCases}, verdicts);
                    }
                    remaining.clear();
                    break;
                }

                bool madeProgress = false;
                std::set<std::filesystem::path> activeOutputFiles;
                for (const auto& activeCase : active)
                {
                    activeOutputFiles.insert(activeCase.outputFiles.begin(), activeCase.outputFiles.end());
                }

                for (auto pending = remaining.begin(); pending != remaining.end() && active.size() < concurrency && !failFast
                     && !sessionFatal && !stopToken.stop_requested() && std::chrono::steady_clock::now() < runDeadline;)
                {
                    const auto& testCase = **pending;
                    const auto unresolvedDependency = std::ranges::find_if(
                        testCase.dependencies, [&](const TestCaseId& dependency) { return !verdicts.contains(dependency); });
                    if (unresolvedDependency != testCase.dependencies.end())
                    {
                        ++pending;
                        continue;
                    }
                    const auto candidateStarted = std::chrono::steady_clock::now();
                    const auto candidateDeadline = std::min(caseDeadline(candidateStarted, setup.deadlines.caseTimeout), runDeadline);
                    std::vector<ExecutionRequest> requests{requestFor(
                        testCase,
                        *resolvedRun.preparedCases,
                        ExecutionRole::Primary,
                        setup.metrics.collect,
                        candidateDeadline,
                        setup.deadlines.cancellationGracePeriod)};
                    if (std::holds_alternative<DifferentialAction>(testCase.action))
                    {
                        requests.push_back(requestFor(
                            testCase,
                            *resolvedRun.preparedCases,
                            ExecutionRole::Differential,
                            setup.metrics.collect,
                            candidateDeadline,
                            setup.deadlines.cancellationGracePeriod));
                    }
                    const auto collides = std::ranges::any_of(
                        requests,
                        [&](const ExecutionRequest& request)
                        { return !request.output.file.empty() && activeOutputFiles.contains(request.output.file); });
                    if (collides)
                    {
                        ++pending;
                        continue;
                    }

                    ActiveCase activeCase{
                        .scheduled = ScheduledCase{.definition = *pending, .sequenceNumber = sequenceNumber++},
                        .started = candidateStarted,
                        .deadline = candidateDeadline,
                        .handles = {},
                        .primaryOutput = std::nullopt,
                        .differentialOutput = std::nullopt,
                        .metrics = {},
                        .artifacts = {},
                        .outputFiles = {}};
                    std::optional<ExecutionOutcome> startOutcome;
                    for (const auto& request : requests)
                    {
                        if (!request.output.file.empty())
                        {
                            activeCase.outputFiles.insert(request.output.file);
                        }
                        auto started = session->start(request, stopToken);
                        if (!started)
                        {
                            if (started.error().kind == BackendFaultKind::TeardownFailed)
                            {
                                sessionFatal = true;
                                environmentCloseDeadline = std::chrono::steady_clock::now();
                            }
                            if (started.error().kind == BackendFaultKind::DeadlineReached)
                            {
                                startOutcome = TimedOutExecution{
                                    .id = testCase.id,
                                    .stage = ExecutionStage::Starting,
                                    .elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::steady_clock::now() - candidateStarted),
                                    .artifacts = {}};
                            }
                            else
                            {
                                summary.cancelled = summary.cancelled || started.error().kind == BackendFaultKind::Cancelled;
                                startOutcome = FailedExecution{
                                    .id = testCase.id,
                                    .stage = ExecutionStage::Starting,
                                    .error = executionError(started.error()),
                                    .artifacts = {}};
                            }
                            break;
                        }
                        if (const auto* failure = std::get_if<StatementFailure>(&*started))
                        {
                            startOutcome = FailedExecution{
                                .id = testCase.id, .stage = failure->stage, .error = failure->error, .artifacts = failure->artifacts};
                            break;
                        }
                        activeCase.handles.push_back(ActiveHandle{.handle = std::get<ExecutionHandle>(*started), .role = request.role});
                    }

                    pending = remaining.erase(pending);
                    madeProgress = true;
                    if (startOutcome)
                    {
                        const auto cancellationDeadline
                            = environmentCloseDeadline.value_or(std::chrono::steady_clock::now() + setup.deadlines.cancellationGracePeriod);
                        if (auto cancellationFailure = cancelHandles(*session, activeCase.handles, cancellationDeadline))
                        {
                            sessionFatal = true;
                            environmentCloseDeadline = cancellationDeadline;
                            startOutcome = FailedExecution{
                                .id = testCase.id,
                                .stage = ExecutionStage::Cancelling,
                                .error = executionError(*cancellationFailure),
                                .artifacts = activeCase.artifacts};
                        }
                        record(testCase, *startOutcome, verdicts);
                        if (failFast)
                        {
                            break;
                        }
                        continue;
                    }
                    activeOutputFiles.insert(activeCase.outputFiles.begin(), activeCase.outputFiles.end());
                    active.push_back(std::move(activeCase));
                }

                if (sessionFatal)
                {
                    continue;
                }
                if (active.empty())
                {
                    if (failFast || stopToken.stop_requested() || std::chrono::steady_clock::now() >= runDeadline)
                    {
                        continue;
                    }
                    if (!remaining.empty() && !madeProgress)
                    {
                        for (const auto& testCase : remaining)
                        {
                            record(*testCase, SkippedExecution{.id = testCase->id, .failedDependencies = testCase->dependencies}, verdicts);
                        }
                        remaining.clear();
                    }
                    continue;
                }

                std::vector<ExecutionHandle> handles;
                auto waitDeadline = runDeadline;
                for (const auto& activeCase : active)
                {
                    waitDeadline = std::min(waitDeadline, activeCase.deadline);
                    for (const auto& handle : activeCase.handles)
                    {
                        handles.push_back(handle.handle);
                    }
                }
                auto completion = session->waitAny(handles, waitDeadline, stopToken);
                if (!completion)
                {
                    const auto now = std::chrono::steady_clock::now();
                    if (completion.error().kind == BackendFaultKind::DeadlineReached)
                    {
                        const auto cancellationDeadline = now + setup.deadlines.cancellationGracePeriod;
                        for (auto current = active.begin(); current != active.end();)
                        {
                            if (now < current->deadline && now < runDeadline)
                            {
                                ++current;
                                continue;
                            }
                            if (auto cancellationFailure = cancelHandles(*session, current->handles, cancellationDeadline))
                            {
                                sessionFatal = true;
                                environmentCloseDeadline = cancellationDeadline;
                                record(
                                    *current->scheduled.definition,
                                    FailedExecution{
                                        .id = current->scheduled.definition->id,
                                        .stage = ExecutionStage::Cancelling,
                                        .error = executionError(*cancellationFailure),
                                        .artifacts = current->artifacts},
                                    verdicts);
                            }
                            else
                            {
                                record(
                                    *current->scheduled.definition,
                                    TimedOutExecution{
                                        .id = current->scheduled.definition->id,
                                        .stage = ExecutionStage::Running,
                                        .elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(now - current->started),
                                        .artifacts = current->artifacts},
                                    verdicts);
                            }
                            current = active.erase(current);
                        }
                        if (now >= runDeadline)
                        {
                            summary.cancelled = true;
                            environmentCloseDeadline = cancellationDeadline;
                        }
                        continue;
                    }
                    if (completion.error().kind == BackendFaultKind::Cancelled)
                    {
                        summary.cancelled = true;
                    }
                    const auto cancellationDeadline
                        = completion.error().kind == BackendFaultKind::TeardownFailed ? now : now + setup.deadlines.cancellationGracePeriod;
                    if (completion.error().kind == BackendFaultKind::Cancelled)
                    {
                        environmentCloseDeadline = cancellationDeadline;
                    }
                    if (completion.error().kind == BackendFaultKind::TeardownFailed)
                    {
                        sessionFatal = true;
                        environmentCloseDeadline = cancellationDeadline;
                    }
                    for (auto& activeCase : active)
                    {
                        auto failure = completion.error();
                        auto stage = ExecutionStage::Running;
                        if (auto cancellationFailure = cancelHandles(*session, activeCase.handles, cancellationDeadline))
                        {
                            sessionFatal = true;
                            environmentCloseDeadline = cancellationDeadline;
                            failure = *cancellationFailure;
                            stage = ExecutionStage::Cancelling;
                        }
                        record(
                            *activeCase.scheduled.definition,
                            FailedExecution{
                                .id = activeCase.scheduled.definition->id,
                                .stage = stage,
                                .error = executionError(failure),
                                .artifacts = activeCase.artifacts},
                            verdicts);
                    }
                    active.clear();
                    continue;
                }

                const auto activeCase = std::ranges::find_if(
                    active,
                    [&](const auto& candidate)
                    { return std::ranges::find(candidate.handles, completion->handle, &ActiveHandle::handle) != candidate.handles.end(); });
                if (activeCase == active.end())
                {
                    summary.diagnostics.push_back(Diagnostic{
                        .kind = DiagnosticKind::Execution,
                        .message = fmt::format("Received completion for unknown handle {}", completion->handle.value),
                        .source = std::nullopt});
                    continue;
                }
                const auto completedHandle = std::ranges::find(activeCase->handles, completion->handle, &ActiveHandle::handle);
                const auto completedRole = completedHandle->role;
                activeCase->handles.erase(completedHandle);
                mergeMetrics(activeCase->metrics, completion->metrics);
                mergeArtifacts(activeCase->artifacts, completion->artifacts);

                if (const auto* failure = std::get_if<StatementFailure>(&completion->result))
                {
                    auto artifacts = activeCase->artifacts;
                    mergeArtifacts(artifacts, failure->artifacts);
                    const auto cancellationDeadline = std::chrono::steady_clock::now() + setup.deadlines.cancellationGracePeriod;
                    if (auto cancellationFailure = cancelHandles(*session, activeCase->handles, cancellationDeadline))
                    {
                        sessionFatal = true;
                        environmentCloseDeadline = cancellationDeadline;
                        record(
                            *activeCase->scheduled.definition,
                            FailedExecution{
                                .id = activeCase->scheduled.definition->id,
                                .stage = ExecutionStage::Cancelling,
                                .error = executionError(*cancellationFailure),
                                .artifacts = std::move(artifacts)},
                            verdicts);
                    }
                    else
                    {
                        record(
                            *activeCase->scheduled.definition,
                            FailedExecution{
                                .id = activeCase->scheduled.definition->id,
                                .stage = failure->stage,
                                .error = failure->error,
                                .artifacts = std::move(artifacts)},
                            verdicts);
                    }
                    active.erase(activeCase);
                    continue;
                }

                if (completedRole == ExecutionRole::Primary)
                {
                    activeCase->primaryOutput = std::get<StatementOutput>(std::move(completion->result));
                }
                else
                {
                    activeCase->differentialOutput = std::get<StatementOutput>(std::move(completion->result));
                }
                if (!activeCase->handles.empty())
                {
                    continue;
                }

                std::vector<StatementOutput> outputs;
                if (activeCase->primaryOutput)
                {
                    outputs.push_back(std::move(*activeCase->primaryOutput));
                }
                if (activeCase->differentialOutput)
                {
                    outputs.push_back(std::move(*activeCase->differentialOutput));
                }
                record(
                    *activeCase->scheduled.definition,
                    CompletedExecution{
                        .id = activeCase->scheduled.definition->id,
                        .outputs = std::move(outputs),
                        .metrics = activeCase->metrics,
                        .artifacts = activeCase->artifacts},
                    verdicts);
                active.erase(activeCase);
            }

            const auto closeDeadline
                = environmentCloseDeadline.value_or(std::chrono::steady_clock::now() + setup.deadlines.cancellationGracePeriod);
            if (auto closed = session->close(closeDeadline); !closed)
            {
                summary.cancelled = true;
                failFast = true;
                summary.diagnostics.push_back(
                    Diagnostic{.kind = DiagnosticKind::Execution, .message = closed.error().message, .source = std::nullopt});
            }
        }
        if (std::holds_alternative<UntilCancelled>(setup.repetition) && completedBeforeRepetition == summary.passed + summary.failed)
        {
            summary.diagnostics.push_back(Diagnostic{
                .kind = DiagnosticKind::Scheduling, .message = "Endless execution has no runnable cases", .source = std::nullopt});
            failFast = true;
        }
    }

    summary.elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - runStartedAt);
    if (auto published = reporter.publish(RunFinished{.summary = summary}); !published)
    {
        summary.diagnostics.push_back(
            Diagnostic{.kind = DiagnosticKind::Reporting, .message = published.error().message, .source = std::nullopt});
    }
    return summary;
}

}
