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
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <ranges>
#include <set>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <system_error>
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
    const PreparedExecutionCatalog& preparedExecutions,
    const uint64_t sequenceNumber,
    const ExecutionRole role,
    const bool collectMetrics,
    const std::chrono::steady_clock::time_point deadline,
    const std::chrono::milliseconds cancellationGracePeriod)
{
    ExecutionRequest request{
        .testCase = testCase.id,
        .sequenceNumber = sequenceNumber,
        .role = role,
        .sql = {},
        .output = {},
        .collectMetrics = collectMetrics,
        .deadline = deadline,
        .cancellationGracePeriod = cancellationGracePeriod};
    if (const auto* action = std::get_if<QueryAction>(&testCase.action))
    {
        request.sql = action->sql;
    }
    else
    {
        const auto& differentialAction = std::get<DifferentialAction>(testCase.action);
        request.sql = role == ExecutionRole::Primary ? differentialAction.leftSql : differentialAction.rightSql;
    }

    const auto& execution = preparedExecutions.at(testCase.id);
    const auto* prepared = std::get_if<std::shared_ptr<const PreparedAction>>(&execution.prepared);
    if (prepared == nullptr)
    {
        return request;
    }
    if (std::holds_alternative<PreparedExplain>(**prepared))
    {
        request.output = OutputTarget{.kind = OutputTargetKind::Text, .file = {}};
    }
    else if (const auto* query = std::get_if<PreparedQuery>(&**prepared))
    {
        request.output = query->statement.output;
    }
    else
    {
        const auto& differential = std::get<PreparedDifferential>(**prepared);
        request.output = role == ExecutionRole::Primary ? differential.primary.output : differential.differential.output;
    }
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

std::optional<std::string>
unsupportedCapability(const ResolvedCase& testCase, const EnvironmentSpec& environment, const BackendCapabilities& capabilities)
{
    std::optional<std::string_view> capability;
    if (!capabilities.supportsConfigurationOverrides && !environment.configuration.values.empty())
    {
        capability = "supportsConfigurationOverrides";
    }
    else if (!capabilities.supportsExplain)
    {
        const auto* query = std::get_if<QueryAction>(&testCase.action);
        if (query != nullptr && query->kind == QueryKind::Explain)
        {
            capability = "supportsExplain";
        }
    }
    if (!capability && !capabilities.supportsRemoteFixtures
        && std::ranges::any_of(environment.setupStatements, [](const FixtureStatement& fixture) { return fixture.attachment.has_value(); }))
    {
        capability = "supportsRemoteFixtures";
    }
    if (!capability)
    {
        return std::nullopt;
    }
    return fmt::format(
        "Case {}:{} variant {} in environment {} requires backend capability {}, but the backend does not support it",
        testCase.id.source.relativeTestFile.generic_string(),
        testCase.id.source.queryNumber,
        testCase.id.configurationVariant,
        environment.id.value,
        *capability);
}

std::expected<std::vector<std::filesystem::path>, std::string>
retainExecutionArtifacts(ExecutionOutcome& outcome, const uint64_t sequenceNumber)
{
    std::map<std::filesystem::path, std::filesystem::path> retained;
    const auto retain = [&](const std::filesystem::path& file) -> std::expected<std::optional<std::filesystem::path>, std::string>
    {
        if (file.empty())
        {
            return std::nullopt;
        }
        std::error_code statusError;
        const auto status = std::filesystem::status(file, statusError);
        if (statusError == std::errc::no_such_file_or_directory)
        {
            return std::nullopt;
        }
        if (statusError)
        {
            return std::unexpected(fmt::format("Failed to inspect execution artifact {}: {}", file, statusError.message()));
        }
        if (!std::filesystem::exists(status))
        {
            return std::nullopt;
        }
        if (!std::filesystem::is_regular_file(status))
        {
            return std::unexpected(fmt::format("Execution artifact {} is not a regular file", file));
        }
        if (const auto existing = retained.find(file); existing != retained.end())
        {
            return std::optional<std::filesystem::path>{existing->second};
        }
        const auto directory = file.parent_path() / "executions";
        std::error_code error;
        std::filesystem::create_directories(directory, error);
        if (error)
        {
            return std::unexpected(fmt::format("Failed to create artifact directory {}: {}", directory, error.message()));
        }
        const auto retainedFile
            = directory / fmt::format("{}-execution-{}{}", file.stem().string(), sequenceNumber, file.extension().string());
        std::filesystem::copy_file(file, retainedFile, std::filesystem::copy_options::overwrite_existing, error);
        if (error)
        {
            return std::unexpected(fmt::format("Failed to retain execution artifact {}: {}", file, error.message()));
        }
        retained.emplace(file, retainedFile);
        return std::optional<std::filesystem::path>{std::move(retainedFile)};
    };

    if (auto* execution = std::get_if<CompletedExecution>(&outcome))
    {
        for (auto& output : execution->outputs)
        {
            if (auto* table = std::get_if<TableArtifact>(&output))
            {
                auto retainedFile = retain(table->file);
                if (!retainedFile)
                {
                    return std::unexpected(std::move(retainedFile).error());
                }
                if (*retainedFile)
                {
                    table->file = std::move(**retainedFile);
                }
            }
        }
    }
    auto* artifacts = std::visit(
        [](auto& execution) -> ArtifactSet*
        {
            if constexpr (requires { execution.artifacts; })
            {
                return &execution.artifacts;
            }
            return nullptr;
        },
        outcome);
    if (artifacts != nullptr)
    {
        std::vector<std::filesystem::path> retainedArtifacts;
        retainedArtifacts.reserve(artifacts->files.size());
        for (const auto& file : artifacts->files)
        {
            auto retainedFile = retain(file);
            if (!retainedFile)
            {
                return std::unexpected(std::move(retainedFile).error());
            }
            if (*retainedFile)
            {
                retainedArtifacts.push_back(std::move(**retainedFile));
            }
        }
        artifacts->files = std::move(retainedArtifacts);
    }
    return retained | std::views::values | std::ranges::to<std::vector>();
}

}

RunSummary RunCoordinator::run(
    const PreparedRun& preparedRun,
    RunSetup setup,
    ExecutionBackend& backend,
    const CaseValidator& validator,
    RunReporter& reporter,
    const std::stop_token stopToken) const
{
    const auto& resolvedRun = preparedRun.resolved;
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
                            ExecutionOutcome outcome,
                            std::map<TestCaseId, Verdict>& verdicts,
                            std::optional<Diagnostic> additionalDiagnostic = std::nullopt,
                            std::optional<uint64_t> scheduledSequenceNumber = std::nullopt)
    {
        std::vector<std::filesystem::path> retainedFiles;
        const auto* fixed = std::get_if<FixedRepetitions>(&setup.repetition);
        const auto retainArtifacts = (fixed != nullptr && fixed->count > 1) || std::holds_alternative<UntilCancelled>(setup.repetition);
        if (retainArtifacts && scheduledSequenceNumber)
        {
            if (auto retained = retainExecutionArtifacts(outcome, *scheduledSequenceNumber))
            {
                retainedFiles = std::move(*retained);
            }
            else
            {
                outcome = FailedExecution{
                    .id = testCase.id,
                    .stage = ExecutionStage::Closing,
                    .error = ExecutionError{
                        .kind = ExecutionErrorKind::Backend,
                        .details = {{.code = ErrorCode::TestException, .message = std::move(retained).error()}}},
                    .artifacts = {}};
            }
        }
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
        if (std::holds_alternative<UntilCancelled>(setup.repetition) && validated.verdict == Verdict::Passed)
        {
            for (const auto& file : retainedFiles)
            {
                std::error_code error;
                std::filesystem::remove(file, error);
            }
        }
        if (!std::holds_alternative<UntilCancelled>(setup.repetition) || validated.verdict != Verdict::Passed)
        {
            summary.results.push_back(std::move(validated));
        }
    };
    const auto recordScheduled = [&](const ActiveCase& activeCase, ExecutionOutcome outcome, std::map<TestCaseId, Verdict>& verdicts)
    { record(*activeCase.scheduled.definition, std::move(outcome), verdicts, std::nullopt, activeCase.scheduled.sequenceNumber); };

    const auto capabilities = backend.capabilities();
    const auto concurrency = std::min(setup.concurrency.maximumActiveCases, capabilities.maximumConcurrency);
    if (const auto* fixed = std::get_if<FixedRepetitions>(&setup.repetition); fixed != nullptr && fixed->count == 0)
    {
        summary.diagnostics.push_back(Diagnostic{
            .kind = DiagnosticKind::Scheduling, .message = "Fixed repetition count must be greater than zero", .source = std::nullopt});
    }

    size_t repetition = 0;
    const auto shouldRunAnotherRepetition = [&]
    {
        if (std::holds_alternative<Once>(setup.repetition) && repetition != 0)
        {
            return false;
        }
        if (const auto* fixed = std::get_if<FixedRepetitions>(&setup.repetition); fixed != nullptr && repetition >= fixed->count)
        {
            return false;
        }
        const auto now = std::chrono::steady_clock::now();
        if (stopToken.stop_requested() || now >= runDeadline)
        {
            summary.cancelled = true;
            return false;
        }
        return !failFast;
    };

    while (shouldRunAnotherRepetition())
    {
        ++repetition;
        const auto completedBeforeRepetition = summary.passed + summary.failed;
        std::map<TestCaseId, Verdict> verdicts;
        std::map<EnvironmentId, BackendFault> unavailableEnvironments;
        std::vector<std::shared_ptr<const ResolvedCase>> remaining = resolvedRun.cases
            | std::views::filter([&](const auto& testCase) { return setup.selection.contains(testCase->id); })
            | std::ranges::to<std::vector>();
        std::ranges::sort(remaining, {}, [](const auto& testCase) { return testCase->id; });
        if (setup.ordering.kind == OrderingKind::Shuffled)
        {
            std::ranges::shuffle(remaining, random);
        }

        while (!remaining.empty())
        {
            const auto schedulerNow = std::chrono::steady_clock::now();
            if (stopToken.stop_requested() || schedulerNow >= runDeadline || failFast)
            {
                summary.cancelled = stopToken.stop_requested() || schedulerNow >= runDeadline;
                const auto reason = summary.cancelled ? "Skipped because the run was cancelled" : "Skipped because fail-fast was triggered";
                for (const auto& testCase : remaining)
                {
                    record(*testCase, SkippedExecution{.id = testCase->id, .failedDependencies = {}, .reason = reason}, verdicts);
                }
                remaining.clear();
                break;
            }

            const auto nextReady = std::ranges::find_if(
                remaining,
                [&](const auto& testCase)
                {
                    return std::ranges::all_of(
                        testCase->dependencies, [&](const TestCaseId& dependency) { return verdicts.contains(dependency); });
                });
            if (nextReady == remaining.end())
            {
                for (const auto& testCase : remaining)
                {
                    record(
                        *testCase,
                        SkippedExecution{
                            .id = testCase->id,
                            .failedDependencies = testCase->dependencies,
                            .reason = "Skipped because dependencies could not be resolved"},
                        verdicts);
                }
                remaining.clear();
                break;
            }

            const auto& nextCase = **nextReady;
            if (const auto reason = setup.selection.skipReason(nextCase.id))
            {
                record(nextCase, SkippedExecution{.id = nextCase.id, .failedDependencies = {}, .reason = std::string{*reason}}, verdicts);
                remaining.erase(nextReady);
                continue;
            }

            const auto& environment = resolvedRun.environment(nextCase.environment);
            if (const auto unsupported = unsupportedCapability(nextCase, environment, capabilities))
            {
                record(
                    nextCase,
                    FailedExecution{
                        .id = nextCase.id,
                        .stage = ExecutionStage::Planning,
                        .error
                        = ExecutionError{.kind = ExecutionErrorKind::Backend, .details = {{.code = ErrorCode::InvalidConfigParameter, .message = *unsupported}}},
                        .artifacts = {}},
                    verdicts);
                remaining.erase(nextReady);
                continue;
            }
            if (concurrency == 0)
            {
                record(
                    nextCase,
                    FailedExecution{
                        .id = nextCase.id,
                        .stage = ExecutionStage::Starting,
                        .error
                        = ExecutionError{.kind = ExecutionErrorKind::Backend, .details = {{.code = ErrorCode::InvalidConfigParameter, .message = "maximumActiveCases must be greater than zero"}}},
                        .artifacts = {}},
                    verdicts);
                remaining.erase(nextReady);
                continue;
            }

            if (const auto unavailable = unavailableEnvironments.find(environment.id); unavailable != unavailableEnvironments.end())
            {
                record(
                    nextCase,
                    FailedExecution{
                        .id = nextCase.id,
                        .stage = ExecutionStage::Starting,
                        .error = executionError(unavailable->second),
                        .artifacts = {}},
                    verdicts);
                remaining.erase(nextReady);
                continue;
            }
            auto opened = backend.open(environment);
            if (!opened)
            {
                unavailableEnvironments.emplace(environment.id, opened.error());
                record(
                    nextCase,
                    FailedExecution{
                        .id = nextCase.id, .stage = ExecutionStage::Starting, .error = executionError(opened.error()), .artifacts = {}},
                    verdicts);
                remaining.erase(nextReady);
                continue;
            }
            auto session = std::move(*opened);
            std::vector<ActiveCase> active;
            bool sessionFatal = false;
            bool drainBeforeScheduling = false;
            std::optional<std::chrono::steady_clock::time_point> environmentCloseDeadline;
            while (!remaining.empty() || !active.empty())
            {
                const auto schedulerNow = std::chrono::steady_clock::now();
                std::optional<std::expected<StatementCompletion, BackendFault>> completionResult;
                if (drainBeforeScheduling && !sessionFatal)
                {
                    drainBeforeScheduling = false;
                    std::vector<ExecutionHandle> handles;
                    for (const auto& activeCase : active)
                    {
                        for (const auto& handle : activeCase.handles)
                        {
                            handles.push_back(handle.handle);
                        }
                    }
                    auto pending = session->waitAny(handles, std::chrono::steady_clock::time_point::min(), {});
                    if (pending || pending.error().kind != BackendFaultKind::DeadlineReached)
                    {
                        completionResult = std::move(pending);
                    }
                }
                if (!completionResult && sessionFatal)
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
                                SkippedExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .failedDependencies = {},
                                    .reason = "Skipped because the execution session became unusable"},
                                verdicts);
                        }
                    }
                    active.clear();
                    for (const auto& testCase : remaining)
                    {
                        record(
                            *testCase,
                            SkippedExecution{
                                .id = testCase->id,
                                .failedDependencies = {},
                                .reason = "Skipped because the execution session became unusable"},
                            verdicts);
                    }
                    remaining.clear();
                    break;
                }
                if (!completionResult && (stopToken.stop_requested() || schedulerNow >= runDeadline))
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
                                SkippedExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .failedDependencies = {},
                                    .reason = "Skipped because the run was cancelled"},
                                verdicts);
                        }
                    }
                    active.clear();
                    for (const auto& testCase : remaining)
                    {
                        record(
                            *testCase,
                            SkippedExecution{
                                .id = testCase->id, .failedDependencies = {}, .reason = "Skipped because the run was cancelled"},
                            verdicts);
                    }
                    remaining.clear();
                    break;
                }
                if (!completionResult && failFast)
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
                                SkippedExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .failedDependencies = failedCases,
                                    .reason = "Skipped because fail-fast was triggered"},
                                verdicts);
                        }
                    }
                    active.clear();
                    for (const auto& testCase : remaining)
                    {
                        record(
                            *testCase,
                            SkippedExecution{
                                .id = testCase->id, .failedDependencies = failedCases, .reason = "Skipped because fail-fast was triggered"},
                            verdicts);
                    }
                    remaining.clear();
                    break;
                }

                if (!completionResult)
                {
                    std::set<std::filesystem::path> activeOutputFiles;
                    for (const auto& activeCase : active)
                    {
                        activeOutputFiles.insert(activeCase.outputFiles.begin(), activeCase.outputFiles.end());
                    }

                    while (active.size() < concurrency && !failFast && !sessionFatal && !stopToken.stop_requested()
                           && std::chrono::steady_clock::now() < runDeadline)
                    {
                        auto pending = std::ranges::find_if(
                            remaining,
                            [&](const auto& testCase)
                            {
                                return std::ranges::all_of(
                                    testCase->dependencies, [&](const TestCaseId& dependency) { return verdicts.contains(dependency); });
                            });
                        if (pending == remaining.end())
                        {
                            break;
                        }
                        const auto& testCase = **pending;
                        if (const auto reason = setup.selection.skipReason(testCase.id))
                        {
                            record(
                                testCase,
                                SkippedExecution{.id = testCase.id, .failedDependencies = {}, .reason = std::string{*reason}},
                                verdicts);
                            remaining.erase(pending);
                            continue;
                        }
                        const auto& candidateEnvironment = resolvedRun.environment(testCase.environment);
                        if (const auto unsupported = unsupportedCapability(testCase, candidateEnvironment, capabilities))
                        {
                            record(
                                testCase,
                                FailedExecution{
                                    .id = testCase.id,
                                    .stage = ExecutionStage::Planning,
                                    .error
                                    = ExecutionError{.kind = ExecutionErrorKind::Backend, .details = {{.code = ErrorCode::InvalidConfigParameter, .message = *unsupported}}},
                                    .artifacts = {}},
                                verdicts);
                            remaining.erase(pending);
                            continue;
                        }
                        if (testCase.environment != environment.id)
                        {
                            break;
                        }

                        const auto candidateStarted = std::chrono::steady_clock::now();
                        const auto candidateDeadline = std::min(caseDeadline(candidateStarted, setup.deadlines.caseTimeout), runDeadline);
                        const auto candidateSequenceNumber = sequenceNumber++;
                        std::vector<ExecutionRequest> requests{requestFor(
                            testCase,
                            *preparedRun.executions,
                            candidateSequenceNumber,
                            ExecutionRole::Primary,
                            setup.metrics.collect,
                            candidateDeadline,
                            setup.deadlines.cancellationGracePeriod)};
                        if (std::holds_alternative<DifferentialAction>(testCase.action))
                        {
                            requests.push_back(requestFor(
                                testCase,
                                *preparedRun.executions,
                                candidateSequenceNumber,
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
                            break;
                        }

                        ActiveCase activeCase{
                            .scheduled = ScheduledCase{.definition = *pending, .sequenceNumber = candidateSequenceNumber},
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
                                std::error_code error;
                                std::filesystem::remove(request.output.file, error);
                                if (error)
                                {
                                    startOutcome = FailedExecution{
                                    .id = testCase.id,
                                    .stage = ExecutionStage::Starting,
                                    .error = ExecutionError{
                                        .kind = ExecutionErrorKind::Backend,
                                        .details
                                        = {{.code = ErrorCode::TestException,
                                            .message = fmt::format(
                                                "Failed to clear output file {} before execution: {}",
                                                request.output.file,
                                                error.message())}}},
                                    .artifacts = {}};
                                    break;
                                }
                            }
                            auto started = session->start(request, stopToken);
                            if (!started)
                            {
                                if (started.error().kind == BackendFaultKind::TeardownFailed)
                                {
                                    sessionFatal = true;
                                    environmentCloseDeadline = std::chrono::steady_clock::now();
                                }
                                if (started.error().kind == BackendFaultKind::Cancelled)
                                {
                                    summary.cancelled = true;
                                    startOutcome = SkippedExecution{
                                        .id = testCase.id, .failedDependencies = {}, .reason = "Skipped because the run was cancelled"};
                                }
                                else if (started.error().kind == BackendFaultKind::DeadlineReached)
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

                        remaining.erase(pending);
                        if (startOutcome)
                        {
                            const auto cancellationDeadline = environmentCloseDeadline.value_or(
                                std::chrono::steady_clock::now() + setup.deadlines.cancellationGracePeriod);
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
                            record(testCase, *startOutcome, verdicts, std::nullopt, activeCase.scheduled.sequenceNumber);
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
                    if (failFast)
                    {
                        drainBeforeScheduling = !active.empty();
                        continue;
                    }
                    if (active.empty())
                    {
                        if (stopToken.stop_requested() || std::chrono::steady_clock::now() >= runDeadline)
                        {
                            continue;
                        }
                        break;
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
                    completionResult = session->waitAny(handles, waitDeadline, stopToken);
                }
                auto completion = std::move(*completionResult);
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
                                recordScheduled(
                                    *current,
                                    FailedExecution{
                                        .id = current->scheduled.definition->id,
                                        .stage = ExecutionStage::Cancelling,
                                        .error = executionError(*cancellationFailure),
                                        .artifacts = current->artifacts},
                                    verdicts);
                            }
                            else
                            {
                                recordScheduled(
                                    *current,
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
                        if (auto cancellationFailure = cancelHandles(*session, activeCase.handles, cancellationDeadline))
                        {
                            sessionFatal = true;
                            environmentCloseDeadline = cancellationDeadline;
                            recordScheduled(
                                activeCase,
                                FailedExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .stage = ExecutionStage::Cancelling,
                                    .error = executionError(*cancellationFailure),
                                    .artifacts = activeCase.artifacts},
                                verdicts);
                        }
                        else if (completion.error().kind == BackendFaultKind::Cancelled)
                        {
                            recordScheduled(
                                activeCase,
                                SkippedExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .failedDependencies = {},
                                    .reason = "Skipped because the run was cancelled"},
                                verdicts);
                        }
                        else
                        {
                            recordScheduled(
                                activeCase,
                                FailedExecution{
                                    .id = activeCase.scheduled.definition->id,
                                    .stage = ExecutionStage::Running,
                                    .error = executionError(completion.error()),
                                    .artifacts = activeCase.artifacts},
                                verdicts);
                        }
                    }
                    active.clear();
                    continue;
                }

                drainBeforeScheduling = true;
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
                        recordScheduled(
                            *activeCase,
                            FailedExecution{
                                .id = activeCase->scheduled.definition->id,
                                .stage = ExecutionStage::Cancelling,
                                .error = executionError(*cancellationFailure),
                                .artifacts = std::move(artifacts)},
                            verdicts);
                    }
                    else
                    {
                        recordScheduled(
                            *activeCase,
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
                    verdicts,
                    std::nullopt,
                    activeCase->scheduled.sequenceNumber);
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
        if (std::holds_alternative<UntilCancelled>(setup.repetition) && !summary.cancelled && !stopToken.stop_requested()
            && completedBeforeRepetition == summary.passed + summary.failed)
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
