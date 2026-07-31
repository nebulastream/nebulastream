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

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <gtest/gtest.h>
#include <DistributedLogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestCoordinator.hpp>
#include <SystestExecutionBackend.hpp>
#include <SystestPreparation.hpp>
#include <SystestQueryModel.hpp>
#include <SystestResolver.hpp>
#include <SystestRun.hpp>
#include <SystestValidation.hpp>
#include <WorkerConfig.hpp>

namespace NES::Systest
{
namespace
{

ResultSchema schema(const std::string_view name, const DataType::Type type)
{
    return ResultSchema{
        std::vector{UnqualifiedUnboundField{Identifier::parse(std::string{name}), DataTypeProvider::provideDataType(type)}}};
}

DistributedLogicalPlan emptyDistributedPlan(const std::string_view name)
{
    auto globalPlan = LogicalPlan{QueryId::createDistributed(DistributedQueryId{std::string{name}}), std::vector<LogicalOperator>{}};
    std::unordered_map<Host, std::vector<LogicalPlan>> localPlans;
    localPlans.emplace(Host{"localhost"}, std::vector<LogicalPlan>{globalPlan});
    return DistributedLogicalPlan{std::move(localPlans), std::move(globalPlan)};
}

PreparedStatement preparedStatement(const std::string_view name, OutputTarget output, ResultSchema outputSchema = {}, std::string sql = {})
{
    return PreparedStatement{
        .sql = std::move(sql),
        .plan = emptyDistributedPlan(name),
        .outputSchema = std::move(outputSchema),
        .output = std::move(output),
        .sourceMetrics = {}};
}

TestCaseId id(const std::filesystem::path& file, const uint64_t queryNumber, const uint32_t variant = 0)
{
    return TestCaseId{
        .source = CaseKey{.relativeTestFile = file, .queryNumber = SystestQueryId{queryNumber}}, .configurationVariant = variant};
}

TestCaseId id(const uint64_t queryNumber)
{
    return id("suite/contracts.test", queryNumber);
}

Origin origin(const uint64_t queryNumber)
{
    return Origin{.file = "suite/contracts.test", .firstLine = queryNumber, .lastLine = queryNumber};
}

class CollectingReporter final : public RunReporter
{
public:
    std::expected<void, ReportingDiagnostic> publish(const RunEvent& event) override
    {
        events.push_back(event);
        if (onPublish)
        {
            onPublish(event);
        }
        return {};
    }

    std::function<void(const RunEvent&)> onPublish;
    std::vector<RunEvent> events;
};

class RecordingDecoder final : public ResultDecoder
{
public:
    explicit RecordingDecoder(DecodedTable result) : result(std::move(result)) { }

    std::expected<DecodedTable, ValidationDiagnostic> decode(const TableArtifact& artifact) const override
    {
        files.push_back(artifact.file);
        return result;
    }

    mutable std::vector<std::filesystem::path> files;

private:
    DecodedTable result;
};

struct FakeScript
{
    std::optional<BackendFault> openFault;
    std::optional<BackendFault> startFault;
    std::optional<BackendFault> waitFault;
    std::optional<BackendFault> waitFaultAfterCompletion;
    std::optional<BackendFault> cancelFault;
    std::optional<BackendFault> closeFault;
    std::function<std::optional<StatementFailure>(const ExecutionRequest&)> immediateFailure;
    std::function<bool(const ExecutionRequest&)> suppressCompletion;
    std::function<void()> beforeWait;
    std::chrono::milliseconds waitDelay{0};
    size_t maximumConcurrency = 4;
    bool supportsConfigurationOverrides = true;
    bool supportsRemoteFixtures = true;
    bool supportsExplain = true;
    bool reverseCompletions = false;
    bool detailedDifferentialCompletions = false;
    bool writeTableOutputs = false;
};

struct FakeState
{
    std::vector<EnvironmentSpec> openedEnvironments;
    std::vector<ExecutionRequest> requests;
    std::vector<StatementFailure> immediateFailures;
    std::vector<std::chrono::steady_clock::time_point> waitDeadlines;
    std::vector<ExecutionHandle> cancellations;
    std::vector<std::chrono::steady_clock::time_point> cancellationDeadlines;
    std::vector<std::chrono::steady_clock::time_point> closeDeadlines;
};

BackendFault contractFault(std::string message)
{
    return BackendFault{.kind = BackendFaultKind::Failure, .code = ErrorCode::TestException, .message = std::move(message)};
}

const PreparedStatement* statementFor(const PreparedAction& action, const ExecutionRole role)
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

StatementOutput outputFor(const OutputTarget& output)
{
    switch (output.kind)
    {
        case OutputTargetKind::Table:
            return TableArtifact{.file = output.file};
        case OutputTargetKind::Text:
            return TextArtifact{};
        case OutputTargetKind::Discard:
            return DiscardedOutput{};
    }
    std::unreachable();
}

class FakeExecutionSession final : public ExecutionSession
{
public:
    FakeExecutionSession(
        std::shared_ptr<const PreparedExecutionCatalog> executions,
        const EnvironmentId environment,
        FakeScript script,
        std::shared_ptr<FakeState> state)
        : executions(std::move(executions)), environment(environment), script(std::move(script)), state(std::move(state))
    {
    }

    std::expected<std::variant<ExecutionHandle, StatementFailure>, BackendFault>
    start(const ExecutionRequest& request, std::stop_token) override
    {
        state->requests.push_back(request);
        if (script.startFault)
        {
            return std::unexpected(*script.startFault);
        }
        if (script.immediateFailure)
        {
            if (auto failure = script.immediateFailure(request))
            {
                state->immediateFailures.push_back(*failure);
                return std::variant<ExecutionHandle, StatementFailure>{std::move(*failure)};
            }
        }

        const auto* execution = executions->find(request.testCase);
        if (execution == nullptr)
        {
            return std::unexpected(contractFault("missing prepared execution"));
        }
        if (execution->environment != environment)
        {
            return std::unexpected(contractFault("prepared execution belongs to another environment"));
        }
        if (const auto* failure = std::get_if<PlanningFailure>(&execution->prepared))
        {
            auto statementFailure = StatementFailure{
                .stage = ExecutionStage::Planning,
                .error
                = ExecutionError{.kind = ExecutionErrorKind::Statement, .details = {{.code = failure->code, .message = failure->message}}},
                .artifacts = {}};
            state->immediateFailures.push_back(statementFailure);
            return std::variant<ExecutionHandle, StatementFailure>{std::move(statementFailure)};
        }

        const auto& action = **std::get_if<std::shared_ptr<const PreparedAction>>(&execution->prepared);
        const auto handle = ExecutionHandle{.value = nextHandle++};
        if (const auto* explain = std::get_if<PreparedExplain>(&action))
        {
            if (request.role != ExecutionRole::Primary || request.output != OutputTarget{.kind = OutputTargetKind::Text, .file = {}}
                || request.sql != explain->sql)
            {
                return std::unexpected(contractFault("EXPLAIN request role or output mismatch"));
            }
            completions.push_back(
                StatementCompletion{.handle = handle, .result = TextArtifact{.text = explain->output}, .metrics = {}, .artifacts = {}});
            return std::variant<ExecutionHandle, StatementFailure>{handle};
        }

        const auto* statement = statementFor(action, request.role);
        if (statement == nullptr)
        {
            return std::unexpected(contractFault("execution role does not match the prepared action"));
        }
        if (request.output != statement->output || request.sql != statement->sql)
        {
            return std::unexpected(contractFault("execution SQL or output does not match the prepared action"));
        }

        auto metrics = ExecutionMetrics{};
        auto artifacts = ArtifactSet{};
        if (script.detailedDifferentialCompletions)
        {
            const auto base = std::chrono::system_clock::time_point{std::chrono::seconds{100}};
            if (request.role == ExecutionRole::Primary)
            {
                metrics = ExecutionMetrics{
                    .started = base + std::chrono::milliseconds{1},
                    .finished = base + std::chrono::milliseconds{2},
                    .bytesProcessed = 5,
                    .tuplesProcessed = 1};
                artifacts.files = {"shared.log", "primary.log"};
            }
            else
            {
                metrics = ExecutionMetrics{
                    .started = base, .finished = base + std::chrono::milliseconds{3}, .bytesProcessed = 7, .tuplesProcessed = 2};
                artifacts.files = {"shared.log", "differential.log"};
            }
        }
        if (script.writeTableOutputs && statement->output.kind == OutputTargetKind::Table)
        {
            std::filesystem::create_directories(statement->output.file.parent_path());
            std::ofstream(statement->output.file) << "value:UINT64:NOT_NULLABLE\n1\n";
            artifacts.files.push_back(statement->output.file);
        }
        if (!script.suppressCompletion || !script.suppressCompletion(request))
        {
            completions.push_back(
                StatementCompletion{.handle = handle, .result = outputFor(statement->output), .metrics = metrics, .artifacts = artifacts});
        }
        return std::variant<ExecutionHandle, StatementFailure>{handle};
    }

    std::expected<StatementCompletion, BackendFault> waitAny(
        const std::span<const ExecutionHandle> active,
        const std::chrono::steady_clock::time_point deadline,
        const std::stop_token stopToken) override
    {
        state->waitDeadlines.push_back(deadline);
        if (script.beforeWait)
        {
            script.beforeWait();
        }
        if (script.waitDelay > std::chrono::milliseconds{0})
        {
            std::this_thread::sleep_for(script.waitDelay);
        }
        if (stopToken.stop_requested())
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::Cancelled, .code = ErrorCode::QueryStatusFailed, .message = "fake execution was cancelled"});
        }
        if (script.waitFault)
        {
            return std::unexpected(*script.waitFault);
        }
        if (script.waitFaultAfterCompletion && returnedCompletions != 0)
        {
            auto fault = std::move(*script.waitFaultAfterCompletion);
            script.waitFaultAfterCompletion.reset();
            return std::unexpected(std::move(fault));
        }
        const auto requested
            = [&](const StatementCompletion& completion) { return std::ranges::find(active, completion.handle) != active.end(); };
        if (script.reverseCompletions)
        {
            for (auto completion = completions.end(); completion != completions.begin();)
            {
                --completion;
                if (requested(*completion))
                {
                    auto result = std::move(*completion);
                    completions.erase(completion);
                    ++returnedCompletions;
                    return result;
                }
            }
        }
        else if (const auto completion = std::ranges::find_if(completions, requested); completion != completions.end())
        {
            auto result = std::move(*completion);
            completions.erase(completion);
            ++returnedCompletions;
            return result;
        }
        if (std::chrono::steady_clock::now() >= deadline)
        {
            return std::unexpected(BackendFault{
                .kind = BackendFaultKind::DeadlineReached, .code = ErrorCode::QueryStatusFailed, .message = "fake wait deadline reached"});
        }
        return std::unexpected(contractFault("no requested completion"));
    }

    std::expected<void, BackendFault> cancel(const ExecutionHandle handle, const std::chrono::steady_clock::time_point deadline) override
    {
        state->cancellations.push_back(handle);
        state->cancellationDeadlines.push_back(deadline);
        if (script.cancelFault)
        {
            return std::unexpected(*script.cancelFault);
        }
        std::erase_if(completions, [&](const StatementCompletion& completion) { return completion.handle == handle; });
        return {};
    }

    std::expected<void, BackendFault> close(const std::chrono::steady_clock::time_point deadline) override
    {
        state->closeDeadlines.push_back(deadline);
        if (script.closeFault)
        {
            return std::unexpected(*script.closeFault);
        }
        completions.clear();
        return {};
    }

private:
    std::shared_ptr<const PreparedExecutionCatalog> executions;
    EnvironmentId environment;
    FakeScript script;
    std::shared_ptr<FakeState> state;
    uint64_t nextHandle = 1;
    size_t returnedCompletions = 0;
    std::vector<StatementCompletion> completions;
};

class FakeExecutionBackend final : public ExecutionBackend
{
public:
    FakeExecutionBackend(std::shared_ptr<const PreparedExecutionCatalog> executions, FakeScript script = {})
        : executions(std::move(executions)), script(std::move(script)), state(std::make_shared<FakeState>())
    {
    }

    BackendCapabilities capabilities() const override
    {
        return BackendCapabilities{
            .supportsConfigurationOverrides = script.supportsConfigurationOverrides,
            .supportsRemoteFixtures = script.supportsRemoteFixtures,
            .supportsExplain = script.supportsExplain,
            .maximumConcurrency = script.maximumConcurrency};
    }

    std::expected<std::unique_ptr<ExecutionSession>, BackendFault> open(const EnvironmentSpec& environment) override
    {
        state->openedEnvironments.push_back(environment);
        if (script.openFault)
        {
            return std::unexpected(*script.openFault);
        }
        return std::unique_ptr<ExecutionSession>{std::make_unique<FakeExecutionSession>(executions, environment.id, script, state)};
    }

    std::shared_ptr<const PreparedExecutionCatalog> executions;
    FakeScript script;
    std::shared_ptr<FakeState> state;
};

PreparedRun makePreparedRun(
    std::vector<EnvironmentSpec> environments,
    std::vector<std::shared_ptr<const ResolvedCase>> cases,
    std::map<TestCaseId, PreparedExecution> executions)
{
    std::map<EnvironmentId, PreparedEnvironment> preparedEnvironments;
    for (const auto& environment : environments)
    {
        preparedEnvironments.emplace(
            environment.id, PreparedEnvironment{.id = environment.id, .context = std::shared_ptr<PreparedEnvironmentContext>{}});
    }
    return PreparedRun{
        .resolved = ResolvedRun{.environments = std::move(environments), .cases = std::move(cases)},
        .environments = std::make_shared<const PreparedEnvironmentCatalog>(std::move(preparedEnvironments)),
        .executions = std::make_shared<const PreparedExecutionCatalog>(std::move(executions))};
}

PreparedRun makePreparedRun(
    EnvironmentSpec environment, std::vector<std::shared_ptr<const ResolvedCase>> cases, std::map<TestCaseId, PreparedExecution> executions)
{
    return makePreparedRun(std::vector<EnvironmentSpec>{std::move(environment)}, std::move(cases), std::move(executions));
}

struct SimpleCaseSpec
{
    TestCaseId testCaseId;
    EnvironmentId environment;
    std::vector<TestCaseId> dependencies;
};

PreparedRun simpleRun(std::vector<EnvironmentSpec> environments, const std::vector<SimpleCaseSpec>& caseSpecs)
{
    std::vector<std::shared_ptr<const ResolvedCase>> cases;
    std::map<TestCaseId, PreparedExecution> executions;
    for (const auto& specification : caseSpecs)
    {
        const auto sql = fmt::format("SELECT {} INTO Void();", specification.testCaseId.source.queryNumber);
        cases.push_back(std::make_shared<const ResolvedCase>(ResolvedCase{
            .id = specification.testCaseId,
            .environment = specification.environment,
            .source = Origin{
                .file = specification.testCaseId.source.relativeTestFile,
                .firstLine = specification.testCaseId.source.queryNumber.getRawValue(),
                .lastLine = specification.testCaseId.source.queryNumber.getRawValue()},
            .action = QueryAction{.sql = sql, .kind = QueryKind::Execute},
            .expectation = RowsExpectation{.rows = {}, .comparison = ComparisonPolicy::UnorderedTypedRows},
            .dependencies = specification.dependencies}));
        auto action = std::make_shared<const PreparedAction>(PreparedQuery{
            .statement = preparedStatement(
                fmt::format("case-{}", specification.testCaseId.source.queryNumber),
                OutputTarget{.kind = OutputTargetKind::Discard, .file = {}},
                {},
                sql)});
        executions.emplace(
            specification.testCaseId, PreparedExecution{.environment = specification.environment, .prepared = std::move(action)});
    }
    return makePreparedRun(std::move(environments), std::move(cases), std::move(executions));
}

PreparedRun dependentRun(const bool expectedFirstFailure, const bool planningFailure)
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = {},
        .cluster = {}};
    auto first = std::make_shared<const ResolvedCase>(ResolvedCase{
        .id = id(1),
        .environment = environment.id,
        .source = origin(1),
        .action = QueryAction{.sql = "SELECT first INTO Void();", .kind = QueryKind::Execute},
        .expectation = expectedFirstFailure
            ? CaseExpectation{ErrorExpectation{.code = ErrorCode::TestException, .message = std::nullopt}}
            : CaseExpectation{RowsExpectation{.rows = {}, .comparison = ComparisonPolicy::UnorderedTypedRows}},
        .dependencies = {}});
    auto second = std::make_shared<const ResolvedCase>(ResolvedCase{
        .id = id(2),
        .environment = environment.id,
        .source = origin(2),
        .action = QueryAction{.sql = "EXPLAIN (LOGICAL) FORMAT TEXT SELECT second INTO File();", .kind = QueryKind::Explain},
        .expectation = TextExpectation{.lines = {"ok"}, .matching = TextMatchPolicy::Exact},
        .dependencies = {first->id}});

    std::map<TestCaseId, PreparedExecution> executions;
    if (planningFailure)
    {
        executions.emplace(
            first->id,
            PreparedExecution{
                .environment = environment.id,
                .prepared = PlanningFailure{.code = ErrorCode::TestException, .message = "expected planning failure"}});
    }
    else
    {
        auto action = std::make_shared<const PreparedAction>(PreparedQuery{
            .statement = preparedStatement(
                "contract-first", OutputTarget{.kind = OutputTargetKind::Discard, .file = {}}, {}, "SELECT first INTO Void();")});
        executions.emplace(first->id, PreparedExecution{.environment = environment.id, .prepared = std::move(action)});
    }
    auto explain = std::make_shared<const PreparedAction>(
        PreparedExplain{.sql = "EXPLAIN (LOGICAL) FORMAT TEXT SELECT second INTO File();", .output = "ok"});
    executions.emplace(second->id, PreparedExecution{.environment = environment.id, .prepared = std::move(explain)});
    return makePreparedRun(environment, {std::move(first), std::move(second)}, std::move(executions));
}

PreparedRun differentialRun(const std::filesystem::path& outputDirectory = {})
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 7},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = EffectiveConfiguration{.values = {{"worker.mode", "A"}}},
        .cluster = {}};
    auto testCase = std::make_shared<const ResolvedCase>(ResolvedCase{
        .id = id(1),
        .environment = environment.id,
        .source = origin(1),
        .action
        = DifferentialAction{.leftSql = "SELECT value FROM input INTO File();", .rightSql = "SELECT value + UINT64(0) AS value FROM input INTO File();"},
        .expectation = DifferentialExpectation{},
        .dependencies = {}});
    const auto resultSchema = schema("value", DataType::Type::UINT64);
    const auto primaryOutput = OutputTarget{
        .kind = OutputTargetKind::Table,
        .file = outputDirectory.empty() ? std::filesystem::path{"primary.csv"} : outputDirectory / "primary.csv"};
    const auto differentialOutput = OutputTarget{
        .kind = OutputTargetKind::Table,
        .file = outputDirectory.empty() ? std::filesystem::path{"differential.csv"} : outputDirectory / "differential.csv"};
    auto action = std::make_shared<const PreparedAction>(PreparedDifferential{
        .primary = preparedStatement("contract-primary", primaryOutput, resultSchema, "SELECT value FROM input INTO File();"),
        .differential = preparedStatement(
            "contract-differential", differentialOutput, resultSchema, "SELECT value + UINT64(0) AS value FROM input INTO File();")});
    std::map<TestCaseId, PreparedExecution> executions;
    executions.emplace(testCase->id, PreparedExecution{.environment = environment.id, .prepared = std::move(action)});
    return makePreparedRun(environment, {std::move(testCase)}, std::move(executions));
}

PreparedRun explainRun()
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 8},
        .relativeTestFile = "suite/explain.test",
        .setupStatements = {},
        .configuration = {},
        .cluster = {}};
    const auto testCaseId = id("suite/explain.test", 1);
    const auto sql = std::string{"EXPLAIN (LOGICAL) FORMAT TEXT SELECT 1 INTO File();"};
    auto testCase = std::make_shared<const ResolvedCase>(ResolvedCase{
        .id = testCaseId,
        .environment = environment.id,
        .source = Origin{.file = "suite/explain.test", .firstLine = 1, .lastLine = 3},
        .action = QueryAction{.sql = sql, .kind = QueryKind::Explain},
        .expectation = TextExpectation{.lines = {"ok"}, .matching = TextMatchPolicy::Exact},
        .dependencies = {}});
    std::map<TestCaseId, PreparedExecution> executions;
    executions.emplace(
        testCaseId,
        PreparedExecution{
            .environment = environment.id,
            .prepared = std::make_shared<const PreparedAction>(PreparedExplain{.sql = sql, .output = "ok"})});
    return makePreparedRun(environment, {std::move(testCase)}, std::move(executions));
}

RunSetup runSetup(const PreparedRun& run)
{
    return RunSetup{
        .selection = TestSelection{.includeAll = false, .cases = run.resolved.ids(), .intentionalSkips = {}},
        .ordering = OrderingPolicy{.kind = OrderingKind::SourceOrder, .seed = std::nullopt},
        .concurrency = ConcurrencyPolicy{.maximumActiveCases = 2},
        .repetition = Once{},
        .failurePolicy = IndependentFailurePolicy::Continue,
        .deadlines
        = DeadlinePolicy{.caseTimeout = std::chrono::milliseconds::max(), .runTimeout = std::nullopt, .cancellationGracePeriod = std::chrono::milliseconds{10}},
        .validation = ValidationPolicy{.enabled = true},
        .metrics = MetricsPolicy{.collect = false, .report = false}};
}

}

TEST(SystestExecutionContractTest, ExpectedPlanningFailurePassesAndReleasesDependency)
{
    auto run = dependentRun(true, true);
    FakeExecutionBackend backend{run.executions};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 2);
    EXPECT_EQ(summary.failed, 0);
    EXPECT_EQ(summary.skipped, 0);
    ASSERT_EQ(backend.state->requests.size(), 2);
    EXPECT_EQ(backend.state->requests[0].testCase, id(1));
    EXPECT_EQ(backend.state->requests[1].testCase, id(2));
    EXPECT_EQ(backend.state->requests[1].role, ExecutionRole::Primary);
    EXPECT_EQ(backend.state->requests[1].output, (OutputTarget{.kind = OutputTargetKind::Text, .file = {}}));
    ASSERT_EQ(backend.state->immediateFailures.size(), 1);
    EXPECT_EQ(backend.state->immediateFailures.front().stage, ExecutionStage::Planning);
    EXPECT_EQ(backend.state->immediateFailures.front().error.kind, ExecutionErrorKind::Statement);
    EXPECT_TRUE(backend.state->immediateFailures.front().error.contains(ErrorCode::TestException));
    ASSERT_EQ(reporter.events.size(), 4);
    EXPECT_TRUE(std::holds_alternative<RunStarted>(reporter.events[0]));
    EXPECT_TRUE(std::holds_alternative<CaseFinished>(reporter.events[1]));
    EXPECT_TRUE(std::holds_alternative<CaseFinished>(reporter.events[2]));
    EXPECT_TRUE(std::holds_alternative<RunFinished>(reporter.events[3]));
}

TEST(SystestExecutionContractTest, BackendFaultDoesNotSatisfyExpectedPlanningError)
{
    auto run = dependentRun(true, true);
    auto script = FakeScript{};
    script.startFault
        = BackendFault{.kind = BackendFaultKind::Failure, .code = ErrorCode::TestException, .message = "backend infrastructure failure"};
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 0);
    EXPECT_EQ(summary.failed, 2);
    EXPECT_EQ(summary.skipped, 0);
    ASSERT_EQ(summary.results.size(), 2);
    ASSERT_FALSE(summary.results[0].diagnostics.empty());
    EXPECT_EQ(summary.results[0].diagnostics.front().kind, DiagnosticKind::Execution);
    EXPECT_TRUE(summary.results[0].diagnostics.front().message.contains("backend infrastructure failure"));
    EXPECT_EQ(backend.state->requests.size(), 2);
}

TEST(SystestExecutionContractTest, RuntimeInfrastructureFailuresAreClassifiedAsBackendErrors)
{
    const auto missingDetails = runtimeExecutionError(QueryStatusFailed("query failed without an exception"));
    EXPECT_EQ(missingDetails.kind, ExecutionErrorKind::Backend);
    ASSERT_EQ(missingDetails.details.size(), 1);
    EXPECT_EQ(missingDetails.details.front().code, ErrorCode::QueryStatusFailed);

    EXPECT_EQ(runtimeExecutionError(CannotDeserialize("prepared plan round trip failed")).kind, ExecutionErrorKind::Backend);

    const auto statement = runtimeExecutionError(
        DistributedException{std::unordered_map<Host, std::vector<Exception>>{{Host{"worker"}, {TestException("statement failure")}}}});
    EXPECT_EQ(statement.kind, ExecutionErrorKind::Statement);

    const auto mixed = runtimeExecutionError(DistributedException{std::unordered_map<Host, std::vector<Exception>>{
        {Host{"worker"}, {TestException("statement failure"), QueryStatusFailed("status infrastructure failure")}}}});
    EXPECT_EQ(mixed.kind, ExecutionErrorKind::Backend);
    EXPECT_EQ(mixed.details.size(), 2);

    const auto empty = runtimeExecutionError(DistributedException{std::unordered_map<Host, std::vector<Exception>>{}});
    EXPECT_EQ(empty.kind, ExecutionErrorKind::Backend);
    ASSERT_EQ(empty.details.size(), 1);
    EXPECT_EQ(empty.details.front().code, ErrorCode::QueryStatusFailed);
}

TEST(SystestExecutionContractTest, BackendOpenFaultFailsEveryCaseWithoutCreatingASession)
{
    auto run = dependentRun(true, true);
    auto script = FakeScript{};
    script.openFault
        = BackendFault{.kind = BackendFaultKind::Failure, .code = ErrorCode::QueryStartFailed, .message = "environment open failed"};
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 0);
    EXPECT_EQ(summary.failed, 2);
    EXPECT_EQ(summary.skipped, 0);
    ASSERT_EQ(backend.state->openedEnvironments.size(), 1);
    EXPECT_EQ(backend.state->openedEnvironments.front().id, EnvironmentId{.value = 1});
    EXPECT_TRUE(backend.state->requests.empty());
    EXPECT_TRUE(backend.state->closeDeadlines.empty());
    ASSERT_EQ(summary.results.size(), 2);
    ASSERT_FALSE(summary.results.front().diagnostics.empty());
    EXPECT_TRUE(summary.results.front().diagnostics.front().message.contains("environment open failed"));
}

TEST(SystestExecutionContractTest, UnsupportedConfigurationOverridesFailWithCaseAndEnvironmentDiagnostic)
{
    auto run = differentialRun();
    auto script = FakeScript{};
    script.supportsConfigurationOverrides = false;
    FakeExecutionBackend backend{run.executions, std::move(script)};
    RecordingDecoder decoder{DecodedTable{.schema = schema("value", DataType::Type::UINT64), .rows = {"1"}}};
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 0);
    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 0);
    EXPECT_TRUE(backend.state->openedEnvironments.empty());
    EXPECT_TRUE(backend.state->requests.empty());
    ASSERT_EQ(summary.results.size(), 1);
    ASSERT_EQ(summary.results.front().diagnostics.size(), 1);
    EXPECT_EQ(summary.results.front().diagnostics.front().kind, DiagnosticKind::Execution);
    EXPECT_TRUE(summary.results.front().diagnostics.front().message.contains("suite/contracts.test:1 variant 0"));
    EXPECT_TRUE(summary.results.front().diagnostics.front().message.contains("environment 7"));
    EXPECT_TRUE(summary.results.front().diagnostics.front().message.contains("supportsConfigurationOverrides"));
}

TEST(SystestExecutionContractTest, UnsupportedExplainFailsBeforeOpeningOrStartingExecution)
{
    auto run = explainRun();
    auto script = FakeScript{};
    script.supportsExplain = false;
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.failed, 1);
    EXPECT_TRUE(backend.state->openedEnvironments.empty());
    EXPECT_TRUE(backend.state->requests.empty());
    ASSERT_EQ(summary.results.size(), 1);
    ASSERT_FALSE(summary.results.front().diagnostics.empty());
    EXPECT_TRUE(summary.results.front().diagnostics.front().message.contains("supportsExplain"));
}

TEST(SystestExecutionContractTest, FixtureBackedEnvironmentRequiresFixtureCapability)
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "suite/fixture.test",
        .setupStatements = {FixtureStatement{
            .sql = "CREATE PHYSICAL SOURCE FOR input TYPE File;",
            .attachment = InlineSourceData{.rows = {"1"}},
            .source = Origin{.file = "suite/fixture.test", .firstLine = 1, .lastLine = 3}}},
        .configuration = {},
        .cluster = {}};
    auto run = simpleRun({environment}, {{.testCaseId = id("suite/fixture.test", 1), .environment = environment.id, .dependencies = {}}});
    auto script = FakeScript{};
    script.supportsRemoteFixtures = false;
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.failed, 1);
    EXPECT_TRUE(backend.state->requests.empty());
    ASSERT_EQ(summary.results.size(), 1);
    ASSERT_FALSE(summary.results.front().diagnostics.empty());
    EXPECT_TRUE(summary.results.front().diagnostics.front().message.contains("supportsRemoteFixtures"));
}

TEST(SystestExecutionContractTest, SupportedExplainExecutesNormally)
{
    auto run = explainRun();
    FakeExecutionBackend backend{run.executions};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 1);
    EXPECT_EQ(summary.failed, 0);
    ASSERT_EQ(backend.state->requests.size(), 1);
    EXPECT_EQ(backend.state->requests.front().testCase, id("suite/explain.test", 1));
}

TEST(SystestExecutionContractTest, FailedDependencyStillReleasesSuccessorUnderContinuePolicy)
{
    auto run = dependentRun(false, true);
    FakeExecutionBackend backend{run.executions};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 1);
    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 0);
    ASSERT_EQ(backend.state->requests.size(), 2);
    EXPECT_EQ(backend.state->requests[0].testCase, id(1));
    EXPECT_EQ(backend.state->requests[1].testCase, id(2));
}

TEST(SystestExecutionContractTest, SourceOrderIsGlobalAcrossReusedEnvironments)
{
    const auto environmentA = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = EffectiveConfiguration{.values = {{"mode", "A"}}},
        .cluster = {}};
    const auto environmentB = EnvironmentSpec{
        .id = EnvironmentId{.value = 2},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = EffectiveConfiguration{.values = {{"mode", "B"}}},
        .cluster = {}};
    auto run = simpleRun(
        {environmentA, environmentB},
        {{.testCaseId = id(1), .environment = environmentA.id, .dependencies = {}},
         {.testCaseId = id(2), .environment = environmentB.id, .dependencies = {}},
         {.testCaseId = id(3), .environment = environmentA.id, .dependencies = {}}});
    FakeExecutionBackend backend{run.executions};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.concurrency.maximumActiveCases = 1;

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);

    EXPECT_EQ(summary.passed, 3);
    ASSERT_EQ(backend.state->requests.size(), 3);
    EXPECT_EQ(
        backend.state->requests | std::views::transform(&ExecutionRequest::testCase) | std::ranges::to<std::vector>(),
        (std::vector<TestCaseId>{id(1), id(2), id(3)}));
    ASSERT_EQ(backend.state->openedEnvironments.size(), 3);
    EXPECT_EQ(backend.state->openedEnvironments[0].id, environmentA.id);
    EXPECT_EQ(backend.state->openedEnvironments[1].id, environmentB.id);
    EXPECT_EQ(backend.state->openedEnvironments[2].id, environmentA.id);
}

TEST(SystestExecutionContractTest, FixedShuffleSeedIsDeterministicAcrossTheGlobalCaseSet)
{
    const auto environmentA = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = EffectiveConfiguration{.values = {{"mode", "A"}}},
        .cluster = {}};
    const auto environmentB = EnvironmentSpec{
        .id = EnvironmentId{.value = 2},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = EffectiveConfiguration{.values = {{"mode", "B"}}},
        .cluster = {}};
    auto run = simpleRun(
        {environmentA, environmentB},
        {{.testCaseId = id(1), .environment = environmentA.id, .dependencies = {}},
         {.testCaseId = id(2), .environment = environmentB.id, .dependencies = {}},
         {.testCaseId = id(3), .environment = environmentA.id, .dependencies = {}},
         {.testCaseId = id(4), .environment = environmentB.id, .dependencies = {}},
         {.testCaseId = id(5), .environment = environmentA.id, .dependencies = {}}});
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    auto setup = runSetup(run);
    setup.ordering = OrderingPolicy{.kind = OrderingKind::Shuffled, .seed = 1729};
    setup.concurrency.maximumActiveCases = 3;

    FakeExecutionBackend firstBackend{run.executions};
    CollectingReporter firstReporter;
    const auto firstSummary = RunCoordinator{}.run(run, setup, firstBackend, validator, firstReporter);
    FakeExecutionBackend secondBackend{run.executions};
    CollectingReporter secondReporter;
    const auto secondSummary = RunCoordinator{}.run(run, setup, secondBackend, validator, secondReporter);

    EXPECT_EQ(firstSummary.passed, 5);
    EXPECT_EQ(secondSummary.passed, 5);
    const auto firstOrder
        = firstBackend.state->requests | std::views::transform(&ExecutionRequest::testCase) | std::ranges::to<std::vector>();
    const auto secondOrder
        = secondBackend.state->requests | std::views::transform(&ExecutionRequest::testCase) | std::ranges::to<std::vector>();
    EXPECT_EQ(firstOrder, secondOrder);
    EXPECT_NE(firstOrder, (std::vector<TestCaseId>{id(1), id(2), id(3), id(4), id(5)}));
}

TEST(SystestExecutionContractTest, DependenciesAreRespectedUnderSourceAndShuffledOrdering)
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = {},
        .cluster = {}};
    auto run = simpleRun(
        {environment},
        {{.testCaseId = id(1), .environment = environment.id, .dependencies = {}},
         {.testCaseId = id(2), .environment = environment.id, .dependencies = {id(1)}},
         {.testCaseId = id(3), .environment = environment.id, .dependencies = {id(2)}},
         {.testCaseId = id(4), .environment = environment.id, .dependencies = {}}});
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};

    for (const auto ordering :
         {OrderingPolicy{.kind = OrderingKind::SourceOrder, .seed = std::nullopt},
          OrderingPolicy{.kind = OrderingKind::Shuffled, .seed = 91}})
    {
        FakeExecutionBackend backend{run.executions};
        CollectingReporter reporter;
        auto setup = runSetup(run);
        setup.ordering = ordering;
        const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);
        ASSERT_EQ(summary.passed, 4);
        const auto order = backend.state->requests | std::views::transform(&ExecutionRequest::testCase) | std::ranges::to<std::vector>();
        const auto position
            = [&](const TestCaseId& testCaseId) { return static_cast<size_t>(std::ranges::find(order, testCaseId) - order.begin()); };
        EXPECT_LT(position(id(1)), position(id(2)));
        EXPECT_LT(position(id(2)), position(id(3)));
    }
}

TEST(SystestExecutionContractTest, SourceOrderIsDeterministicAcrossMultipleFiles)
{
    const auto environmentA = EnvironmentSpec{
        .id = EnvironmentId{.value = 1}, .relativeTestFile = "a/same.test", .setupStatements = {}, .configuration = {}, .cluster = {}};
    const auto environmentB = EnvironmentSpec{
        .id = EnvironmentId{.value = 2}, .relativeTestFile = "b/same.test", .setupStatements = {}, .configuration = {}, .cluster = {}};
    auto run = simpleRun(
        {environmentB, environmentA},
        {{.testCaseId = id("b/same.test", 2), .environment = environmentB.id, .dependencies = {}},
         {.testCaseId = id("a/same.test", 2), .environment = environmentA.id, .dependencies = {}},
         {.testCaseId = id("b/same.test", 1), .environment = environmentB.id, .dependencies = {}},
         {.testCaseId = id("a/same.test", 1), .environment = environmentA.id, .dependencies = {}}});
    FakeExecutionBackend backend{run.executions};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 4);
    EXPECT_EQ(
        backend.state->requests | std::views::transform(&ExecutionRequest::testCase) | std::ranges::to<std::vector>(),
        (std::vector<TestCaseId>{id("a/same.test", 1), id("a/same.test", 2), id("b/same.test", 1), id("b/same.test", 2)}));
}

TEST(SystestExecutionContractTest, CaseDeadlinesCancelHandlesAndProduceTimedOutVerdicts)
{
    auto run = dependentRun(true, false);
    auto script = FakeScript{};
    script.waitFault
        = BackendFault{.kind = BackendFaultKind::DeadlineReached, .code = ErrorCode::QueryStatusFailed, .message = "deadline reached"};
    script.maximumConcurrency = 1;
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.deadlines.caseTimeout = std::chrono::milliseconds{0};

    const auto summary = RunCoordinator{}.run(run, std::move(setup), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 0);
    EXPECT_EQ(summary.failed, 2);
    EXPECT_EQ(summary.skipped, 0);
    EXPECT_FALSE(summary.cancelled);
    ASSERT_EQ(backend.state->requests.size(), 2);
    ASSERT_EQ(backend.state->waitDeadlines.size(), 2);
    ASSERT_EQ(backend.state->cancellations.size(), 2);
    for (size_t index = 0; index < 2; ++index)
    {
        EXPECT_NE(backend.state->requests[index].deadline, std::chrono::steady_clock::time_point::max());
        EXPECT_EQ(backend.state->waitDeadlines[index], backend.state->requests[index].deadline);
        EXPECT_EQ(backend.state->requests[index].cancellationGracePeriod, std::chrono::milliseconds{10});
        EXPECT_GE(backend.state->cancellationDeadlines[index], backend.state->requests[index].deadline);
    }
    ASSERT_EQ(summary.results.size(), 2);
    ASSERT_FALSE(summary.results[0].diagnostics.empty());
    ASSERT_FALSE(summary.results[1].diagnostics.empty());
    EXPECT_TRUE(summary.results[0].diagnostics.front().message.contains("timed out"));
    EXPECT_TRUE(summary.results[1].diagnostics.front().message.contains("timed out"));
}

TEST(SystestExecutionContractTest, FailedCancellationMakesTheSessionFatal)
{
    auto run = dependentRun(true, false);
    auto script = FakeScript{};
    script.waitFault
        = BackendFault{.kind = BackendFaultKind::DeadlineReached, .code = ErrorCode::QueryStatusFailed, .message = "deadline reached"};
    script.cancelFault
        = BackendFault{.kind = BackendFaultKind::Failure, .code = ErrorCode::QueryStopFailed, .message = "cancellation failed"};
    script.maximumConcurrency = 1;
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.deadlines.caseTimeout = std::chrono::milliseconds{0};

    const auto summary = RunCoordinator{}.run(run, std::move(setup), backend, validator, reporter);

    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 1);
    EXPECT_TRUE(summary.cancelled);
    ASSERT_EQ(backend.state->cancellations.size(), 1);
    ASSERT_EQ(backend.state->closeDeadlines.size(), 1);
    EXPECT_EQ(backend.state->closeDeadlines.front(), backend.state->cancellationDeadlines.front());
    ASSERT_EQ(summary.results.size(), 2);
    ASSERT_FALSE(summary.results.front().diagnostics.empty());
    EXPECT_TRUE(summary.results.front().diagnostics.front().message.contains("cancellation failed"));
}

TEST(SystestExecutionContractTest, TeardownFailureWhileStartingMakesTheSessionFatal)
{
    auto run = dependentRun(true, false);
    auto script = FakeScript{};
    script.startFault = BackendFault{
        .kind = BackendFaultKind::TeardownFailed, .code = ErrorCode::QueryStopFailed, .message = "partial start cleanup failed"};
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_TRUE(summary.cancelled);
    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 1);
    EXPECT_EQ(backend.state->requests.size(), 1);
    EXPECT_EQ(backend.state->closeDeadlines.size(), 1);
    ASSERT_FALSE(summary.results.front().diagnostics.empty());
    EXPECT_TRUE(summary.results.front().diagnostics.front().message.contains("partial start cleanup failed"));
}

TEST(SystestExecutionContractTest, DifferentialRequestsCarryEnvironmentRoleOutputMetricsAndArtifacts)
{
    auto run = differentialRun();
    auto script = FakeScript{};
    script.reverseCompletions = true;
    script.detailedDifferentialCompletions = true;
    FakeExecutionBackend backend{run.executions, std::move(script)};
    const auto resultSchema = schema("value", DataType::Type::UINT64);
    RecordingDecoder decoder{DecodedTable{.schema = resultSchema, .rows = {"1"}}};
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.metrics.collect = true;
    setup.deadlines.cancellationGracePeriod = std::chrono::milliseconds{23};

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);

    EXPECT_EQ(summary.passed, 1);
    EXPECT_EQ(summary.failed, 0);
    ASSERT_EQ(backend.state->openedEnvironments.size(), 1);
    EXPECT_EQ(backend.state->openedEnvironments.front().id, EnvironmentId{.value = 7});
    EXPECT_EQ(backend.state->openedEnvironments.front().relativeTestFile, "suite/contracts.test");
    EXPECT_EQ(
        backend.state->openedEnvironments.front().configuration.values,
        (std::vector<std::pair<std::string, std::string>>{{"worker.mode", "A"}}));

    ASSERT_EQ(backend.state->requests.size(), 2);
    const auto& primary = backend.state->requests[0];
    const auto& differential = backend.state->requests[1];
    EXPECT_EQ(primary.testCase, id(1));
    EXPECT_EQ(differential.testCase, id(1));
    EXPECT_EQ(primary.role, ExecutionRole::Primary);
    EXPECT_EQ(differential.role, ExecutionRole::Differential);
    EXPECT_EQ(primary.sql, "SELECT value FROM input INTO File();");
    EXPECT_EQ(differential.sql, "SELECT value + UINT64(0) AS value FROM input INTO File();");
    EXPECT_EQ(primary.output, (OutputTarget{.kind = OutputTargetKind::Table, .file = "primary.csv"}));
    EXPECT_EQ(differential.output, (OutputTarget{.kind = OutputTargetKind::Table, .file = "differential.csv"}));
    EXPECT_TRUE(primary.collectMetrics);
    EXPECT_TRUE(differential.collectMetrics);
    EXPECT_EQ(primary.deadline, differential.deadline);
    EXPECT_EQ(primary.cancellationGracePeriod, std::chrono::milliseconds{23});
    EXPECT_EQ(differential.cancellationGracePeriod, std::chrono::milliseconds{23});

    EXPECT_EQ(
        decoder.files,
        (std::vector<std::filesystem::path>{std::filesystem::path{"primary.csv"}, std::filesystem::path{"differential.csv"}}));
    ASSERT_EQ(summary.results.size(), 1);
    EXPECT_EQ(summary.results.front().metrics.bytesProcessed, 12);
    EXPECT_EQ(summary.results.front().metrics.tuplesProcessed, 3);
    EXPECT_EQ(summary.results.front().metrics.started, std::chrono::system_clock::time_point{std::chrono::seconds{100}});
    EXPECT_EQ(
        summary.results.front().metrics.finished,
        std::chrono::system_clock::time_point{std::chrono::seconds{100}} + std::chrono::milliseconds{3});
    EXPECT_EQ(
        summary.results.front().artifacts.files, (std::vector<std::filesystem::path>{"shared.log", "differential.log", "primary.log"}));
    EXPECT_TRUE(backend.state->cancellations.empty());
    EXPECT_EQ(backend.state->closeDeadlines.size(), 1);
}

TEST(SystestExecutionContractTest, FixedRepetitionsUseSequenceNumbersForRetainedArtifacts)
{
    const auto outputDirectory = std::filesystem::temp_directory_path()
        / fmt::format("systest-fixed-artifacts-{}", std::chrono::steady_clock::now().time_since_epoch().count());
    auto run = differentialRun(outputDirectory);
    auto script = FakeScript{};
    script.writeTableOutputs = true;
    FakeExecutionBackend backend{run.executions, std::move(script)};
    RecordingDecoder decoder{DecodedTable{.schema = schema("value", DataType::Type::UINT64), .rows = {"1"}}};
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.repetition = FixedRepetitions{.count = 2};

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);

    EXPECT_EQ(summary.passed, 2);
    ASSERT_EQ(summary.results.size(), 2);
    ASSERT_EQ(backend.state->requests.size(), 4);
    EXPECT_EQ(backend.state->requests[0].sequenceNumber, 0);
    EXPECT_EQ(backend.state->requests[1].sequenceNumber, 0);
    EXPECT_EQ(backend.state->requests[2].sequenceNumber, 1);
    EXPECT_EQ(backend.state->requests[3].sequenceNumber, 1);
    ASSERT_EQ(summary.results[0].artifacts.files.size(), 2);
    ASSERT_EQ(summary.results[1].artifacts.files.size(), 2);
    EXPECT_NE(summary.results[0].artifacts.files, summary.results[1].artifacts.files);
    for (const auto& result : summary.results)
    {
        for (const auto& file : result.artifacts.files)
        {
            EXPECT_TRUE(std::filesystem::is_regular_file(file));
            EXPECT_EQ(file.parent_path().filename(), "executions");
        }
    }
    std::error_code error;
    std::filesystem::remove_all(outputDirectory, error);
}

TEST(SystestExecutionContractTest, MissingArtifactsAreNotRetainedAcrossRepetitions)
{
    const auto outputDirectory = std::filesystem::temp_directory_path()
        / fmt::format("systest-missing-artifacts-{}", std::chrono::steady_clock::now().time_since_epoch().count());
    auto run = differentialRun(outputDirectory);
    auto script = FakeScript{};
    script.writeTableOutputs = true;
    script.immediateFailure = [](const ExecutionRequest& request) -> std::optional<StatementFailure>
    {
        if (request.sequenceNumber != 0)
        {
            return std::nullopt;
        }
        return StatementFailure{
            .stage = ExecutionStage::Starting,
            .error
            = ExecutionError{.kind = ExecutionErrorKind::Statement, .details = {{.code = ErrorCode::TestException, .message = "failed before creating output"}}},
            .artifacts = ArtifactSet{.files = {request.output.file}}};
    };
    FakeExecutionBackend backend{run.executions, std::move(script)};
    RecordingDecoder decoder{DecodedTable{.schema = schema("value", DataType::Type::UINT64), .rows = {"1"}}};
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.repetition = FixedRepetitions{.count = 2};

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);

    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.passed, 1);
    ASSERT_EQ(summary.results.size(), 2);
    EXPECT_TRUE(summary.results[0].artifacts.files.empty());
    ASSERT_FALSE(summary.results[0].diagnostics.empty());
    EXPECT_TRUE(summary.results[0].diagnostics.front().message.contains("failed before creating output"));
    ASSERT_EQ(summary.results[1].artifacts.files.size(), 2);
    for (const auto& file : summary.results[1].artifacts.files)
    {
        EXPECT_TRUE(std::filesystem::is_regular_file(file));
        EXPECT_EQ(file.parent_path().filename(), "executions");
    }
    std::error_code error;
    std::filesystem::remove_all(outputDirectory, error);
}

TEST(SystestExecutionContractTest, FailFastStopsBeforeAnotherFixedRepetition)
{
    auto run = dependentRun(false, true);
    auto script = FakeScript{};
    script.startFault = contractFault("first repetition failed");
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.repetition = FixedRepetitions{.count = 5};
    setup.failurePolicy = IndependentFailurePolicy::FailFast;

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);

    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 1);
    EXPECT_EQ(summary.results.size(), 2);
    EXPECT_EQ(backend.state->requests.size(), 1);
    EXPECT_EQ(backend.state->closeDeadlines.size(), 1);
}

TEST(SystestExecutionContractTest, FailFastCancelsConcurrentNeverCompletingHandleAndStopsFixedRepetitions)
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = {},
        .cluster = {}};
    auto run = simpleRun(
        {environment},
        {{.testCaseId = id(1), .environment = environment.id, .dependencies = {}},
         {.testCaseId = id(2), .environment = environment.id, .dependencies = {}}});
    auto script = FakeScript{};
    script.suppressCompletion = [](const ExecutionRequest& request) { return request.testCase == id(1); };
    script.immediateFailure = [](const ExecutionRequest& request) -> std::optional<StatementFailure>
    {
        if (request.testCase != id(2))
        {
            return std::nullopt;
        }
        return StatementFailure{
            .stage = ExecutionStage::Starting,
            .error
            = ExecutionError{.kind = ExecutionErrorKind::Statement, .details = {{.code = ErrorCode::TestException, .message = "concurrent statement failed"}}},
            .artifacts = {}};
    };
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.repetition = FixedRepetitions{.count = 5};
    setup.failurePolicy = IndependentFailurePolicy::FailFast;
    setup.deadlines.cancellationGracePeriod = std::chrono::milliseconds{37};

    const auto started = std::chrono::steady_clock::now();
    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);
    const auto finished = std::chrono::steady_clock::now();

    EXPECT_EQ(summary.passed, 0);
    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 1);
    EXPECT_FALSE(summary.cancelled);
    ASSERT_EQ(summary.results.size(), 2);
    ASSERT_EQ(backend.state->requests.size(), 2);
    EXPECT_EQ(backend.state->requests[0].testCase, id(1));
    EXPECT_EQ(backend.state->requests[1].testCase, id(2));
    ASSERT_EQ(backend.state->waitDeadlines.size(), 1);
    EXPECT_EQ(backend.state->waitDeadlines.front(), std::chrono::steady_clock::time_point::min());
    ASSERT_EQ(backend.state->cancellations.size(), 1);
    ASSERT_EQ(backend.state->cancellationDeadlines.size(), 1);
    EXPECT_GE(backend.state->cancellationDeadlines.front(), started + setup.deadlines.cancellationGracePeriod);
    EXPECT_LE(backend.state->cancellationDeadlines.front(), finished + setup.deadlines.cancellationGracePeriod);
    ASSERT_EQ(backend.state->closeDeadlines.size(), 1);
    EXPECT_EQ(backend.state->closeDeadlines.front(), backend.state->cancellationDeadlines.front());
    const auto activeResult = std::ranges::find(summary.results, id(1), &ValidatedResult::id);
    const auto failedResult = std::ranges::find(summary.results, id(2), &ValidatedResult::id);
    ASSERT_NE(activeResult, summary.results.end());
    ASSERT_NE(failedResult, summary.results.end());
    EXPECT_EQ(activeResult->verdict, Verdict::Skipped);
    EXPECT_EQ(failedResult->verdict, Verdict::Failed);
    ASSERT_FALSE(activeResult->diagnostics.empty());
    EXPECT_TRUE(activeResult->diagnostics.front().message.contains("fail-fast"));
    EXPECT_EQ(reporter.events.size(), 4);
}

TEST(SystestExecutionContractTest, FailFastPreservesObservedConcurrentCompletion)
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = {},
        .cluster = {}};
    auto run = simpleRun(
        {environment},
        {{.testCaseId = id(1), .environment = environment.id, .dependencies = {}},
         {.testCaseId = id(2), .environment = environment.id, .dependencies = {}}});
    auto script = FakeScript{};
    script.immediateFailure = [](const ExecutionRequest& request) -> std::optional<StatementFailure>
    {
        if (request.testCase != id(2))
        {
            return std::nullopt;
        }
        return StatementFailure{
            .stage = ExecutionStage::Starting,
            .error
            = ExecutionError{.kind = ExecutionErrorKind::Statement, .details = {{.code = ErrorCode::TestException, .message = "concurrent statement failed"}}},
            .artifacts = {}};
    };
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.repetition = FixedRepetitions{.count = 3};
    setup.failurePolicy = IndependentFailurePolicy::FailFast;

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);

    EXPECT_EQ(summary.passed, 1);
    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 0);
    EXPECT_FALSE(summary.cancelled);
    EXPECT_EQ(summary.results.size(), 2);
    EXPECT_EQ(backend.state->requests.size(), 2);
    ASSERT_EQ(backend.state->waitDeadlines.size(), 1);
    EXPECT_EQ(backend.state->waitDeadlines.front(), std::chrono::steady_clock::time_point::min());
    EXPECT_TRUE(backend.state->cancellations.empty());
    EXPECT_EQ(backend.state->closeDeadlines.size(), 1);
}

TEST(SystestExecutionContractTest, StopRequestPreventsRemainingFixedRepetitions)
{
    auto run = simpleRun(
        {EnvironmentSpec{
            .id = EnvironmentId{.value = 1},
            .relativeTestFile = "suite/contracts.test",
            .setupStatements = {},
            .configuration = {},
            .cluster = {}}},
        {{.testCaseId = id(1), .environment = EnvironmentId{.value = 1}, .dependencies = {}}});
    std::stop_source stopSource;
    auto script = FakeScript{};
    script.beforeWait = [&] { stopSource.request_stop(); };
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.repetition = FixedRepetitions{.count = 5};

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter, stopSource.get_token());

    EXPECT_TRUE(summary.cancelled);
    EXPECT_EQ(backend.state->requests.size(), 1);
    EXPECT_EQ(backend.state->cancellations.size(), 1);
    EXPECT_EQ(backend.state->closeDeadlines.size(), 1);
    EXPECT_LE(summary.results.size(), 1);
}

TEST(SystestExecutionContractTest, RunDeadlinePreventsRemainingFixedRepetitions)
{
    auto run = simpleRun(
        {EnvironmentSpec{
            .id = EnvironmentId{.value = 1},
            .relativeTestFile = "suite/contracts.test",
            .setupStatements = {},
            .configuration = {},
            .cluster = {}}},
        {{.testCaseId = id(1), .environment = EnvironmentId{.value = 1}, .dependencies = {}}});
    auto script = FakeScript{};
    script.waitDelay = std::chrono::milliseconds{5};
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.repetition = FixedRepetitions{.count = 5};
    setup.deadlines.runTimeout = std::chrono::milliseconds{1};

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);

    EXPECT_TRUE(summary.cancelled);
    EXPECT_EQ(summary.passed, 1);
    EXPECT_EQ(summary.results.size(), 1);
    EXPECT_EQ(backend.state->requests.size(), 1);
    EXPECT_EQ(backend.state->closeDeadlines.size(), 1);
}

TEST(SystestExecutionContractTest, UntilCancelledCancelsActiveHandlesAndClosesSessionOnce)
{
    auto run = simpleRun(
        {EnvironmentSpec{
            .id = EnvironmentId{.value = 1},
            .relativeTestFile = "suite/contracts.test",
            .setupStatements = {},
            .configuration = {},
            .cluster = {}}},
        {{.testCaseId = id(1), .environment = EnvironmentId{.value = 1}, .dependencies = {}}});
    std::stop_source stopSource;
    auto script = FakeScript{};
    script.beforeWait = [&] { stopSource.request_stop(); };
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.repetition = UntilCancelled{};

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter, stopSource.get_token());

    EXPECT_TRUE(summary.cancelled);
    EXPECT_EQ(backend.state->requests.size(), 1);
    EXPECT_EQ(backend.state->cancellations.size(), 1);
    EXPECT_EQ(backend.state->closeDeadlines.size(), 1);
}

TEST(SystestExecutionContractTest, ObservedConcurrentCompletionWinsOverLateCancellation)
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = {},
        .cluster = {}};
    auto run = simpleRun(
        {environment},
        {{.testCaseId = id(1), .environment = environment.id, .dependencies = {}},
         {.testCaseId = id(2), .environment = environment.id, .dependencies = {}}});
    std::stop_source stopSource;
    FakeExecutionBackend backend{run.executions};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    reporter.onPublish = [&](const RunEvent& event)
    {
        if (std::holds_alternative<CaseFinished>(event))
        {
            stopSource.request_stop();
        }
    };

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter, stopSource.get_token());

    EXPECT_EQ(summary.passed, 2);
    EXPECT_EQ(summary.failed, 0);
    EXPECT_EQ(summary.skipped, 0);
    EXPECT_FALSE(summary.cancelled);
    EXPECT_TRUE(backend.state->cancellations.empty());
    EXPECT_EQ(backend.state->closeDeadlines.size(), 1);
}

TEST(SystestExecutionContractTest, DeferredWaitFaultDoesNotPoisonNewlyScheduledCases)
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "suite/contracts.test",
        .setupStatements = {},
        .configuration = {},
        .cluster = {}};
    auto run = simpleRun(
        {environment},
        {{.testCaseId = id(1), .environment = environment.id, .dependencies = {}},
         {.testCaseId = id(2), .environment = environment.id, .dependencies = {}},
         {.testCaseId = id(3), .environment = environment.id, .dependencies = {}}});
    auto script = FakeScript{};
    script.waitFaultAfterCompletion = contractFault("deferred poll fault");
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 2);
    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 0);
    ASSERT_EQ(backend.state->requests.size(), 3);
    EXPECT_EQ(backend.state->requests[2].testCase, id(3));
    const auto thirdResult = std::ranges::find(summary.results, id(3), &ValidatedResult::id);
    ASSERT_NE(thirdResult, summary.results.end());
    EXPECT_EQ(thirdResult->verdict, Verdict::Passed);
}

TEST(SystestExecutionContractTest, LateStopAfterFinalCompletionDoesNotCancelCompletedRun)
{
    auto run = simpleRun(
        {EnvironmentSpec{
            .id = EnvironmentId{.value = 1},
            .relativeTestFile = "suite/contracts.test",
            .setupStatements = {},
            .configuration = {},
            .cluster = {}}},
        {{.testCaseId = id(1), .environment = EnvironmentId{.value = 1}, .dependencies = {}}});
    std::stop_source stopSource;
    FakeExecutionBackend backend{run.executions};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    reporter.onPublish = [&](const RunEvent& event)
    {
        if (std::holds_alternative<CaseFinished>(event))
        {
            stopSource.request_stop();
        }
    };

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter, stopSource.get_token());

    EXPECT_EQ(summary.passed, 1);
    EXPECT_FALSE(summary.cancelled);
}

TEST(SystestExecutionContractTest, ZeroFixedRepetitionsIsRejected)
{
    auto run = dependentRun(true, true);
    FakeExecutionBackend backend{run.executions};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.repetition = FixedRepetitions{.count = 0};

    const auto summary = RunCoordinator{}.run(run, setup, backend, validator, reporter);

    EXPECT_EQ(summary.passed, 0);
    EXPECT_EQ(summary.failed, 0);
    EXPECT_EQ(summary.skipped, 0);
    EXPECT_TRUE(backend.state->openedEnvironments.empty());
    ASSERT_EQ(summary.diagnostics.size(), 1);
    EXPECT_TRUE(summary.diagnostics.front().message.contains("greater than zero"));
}

TEST(SystestExecutionContractTest, CloseFaultIsReportedAfterCompletedCases)
{
    auto run = dependentRun(true, true);
    auto script = FakeScript{};
    script.closeFault
        = BackendFault{.kind = BackendFaultKind::TeardownFailed, .code = ErrorCode::QueryStopFailed, .message = "session close failed"};
    FakeExecutionBackend backend{run.executions, std::move(script)};
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator{decoder, resultComparator, textComparator, *run.executions};
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 2);
    EXPECT_TRUE(summary.cancelled);
    ASSERT_EQ(summary.diagnostics.size(), 1);
    EXPECT_EQ(summary.diagnostics.front().kind, DiagnosticKind::Execution);
    EXPECT_EQ(summary.diagnostics.front().message, "session close failed");
    EXPECT_EQ(backend.state->closeDeadlines.size(), 1);
}

TEST(SystestExecutionContractTest, ObservedCompletionsAreReturnedBeforeANewExpiredDeadline)
{
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 11},
        .relativeTestFile = "suite/observed.test",
        .setupStatements = {},
        .configuration = {},
        .cluster = {}};
    const auto firstId = id("suite/observed.test", 1);
    const auto secondId = id("suite/observed.test", 2);
    const auto firstSql = std::string{"EXPLAIN (LOGICAL) FORMAT TEXT SELECT 1 INTO File();"};
    const auto secondSql = std::string{"EXPLAIN (LOGICAL) FORMAT TEXT SELECT 2 INTO File();"};
    std::map<TestCaseId, PreparedExecution> prepared;
    prepared.emplace(
        firstId,
        PreparedExecution{
            .environment = environment.id,
            .prepared = std::make_shared<const PreparedAction>(PreparedExplain{.sql = firstSql, .output = "first"})});
    prepared.emplace(
        secondId,
        PreparedExecution{
            .environment = environment.id,
            .prepared = std::make_shared<const PreparedAction>(PreparedExplain{.sql = secondSql, .output = "second"})});
    auto executions = std::make_shared<const PreparedExecutionCatalog>(std::move(prepared));
    EmbeddedExecutionBackend backend{executions, SingleNodeWorkerConfiguration{}};
    auto opened = backend.open(environment);
    ASSERT_TRUE(opened) << opened.error().message;
    auto session = std::move(*opened);
    const auto makeRequest = [](const TestCaseId& testCaseId, std::string sql)
    {
        return ExecutionRequest{
            .testCase = testCaseId,
            .sequenceNumber = 0,
            .role = ExecutionRole::Primary,
            .sql = std::move(sql),
            .output = OutputTarget{.kind = OutputTargetKind::Text, .file = {}},
            .collectMetrics = false,
            .deadline = std::chrono::steady_clock::time_point::max(),
            .cancellationGracePeriod = std::chrono::milliseconds{10}};
    };
    auto expiredRequest = makeRequest(firstId, firstSql);
    expiredRequest.deadline = std::chrono::steady_clock::time_point::min();
    auto expired = session->start(expiredRequest, {});
    ASSERT_FALSE(expired);
    EXPECT_EQ(expired.error().kind, BackendFaultKind::DeadlineReached);
    auto mismatched = session->start(makeRequest(firstId, "EXPLAIN SELECT 99 INTO File();"), {});
    ASSERT_FALSE(mismatched);
    EXPECT_TRUE(mismatched.error().message.contains("does not match"));
    auto firstStarted = session->start(makeRequest(firstId, firstSql), {});
    auto secondStarted = session->start(makeRequest(secondId, secondSql), {});
    ASSERT_TRUE(firstStarted);
    ASSERT_TRUE(secondStarted);
    ASSERT_TRUE(std::holds_alternative<ExecutionHandle>(*firstStarted));
    ASSERT_TRUE(std::holds_alternative<ExecutionHandle>(*secondStarted));
    const std::array handles{std::get<ExecutionHandle>(*firstStarted), std::get<ExecutionHandle>(*secondStarted)};

    auto first = session->waitAny(handles, std::chrono::steady_clock::time_point::max(), {});
    ASSERT_TRUE(first);
    auto second = session->waitAny(handles, std::chrono::steady_clock::time_point::min(), {});
    ASSERT_TRUE(second) << second.error().message;
    EXPECT_NE(first->handle, second->handle);
    auto exhausted = session->waitAny(handles, std::chrono::steady_clock::time_point::min(), {});
    ASSERT_FALSE(exhausted);
    EXPECT_EQ(exhausted.error().kind, BackendFaultKind::DeadlineReached);

    auto cancelledStarted = session->start(makeRequest(firstId, firstSql), {});
    ASSERT_TRUE(cancelledStarted);
    const auto cancelledHandle = std::get<ExecutionHandle>(*cancelledStarted);
    EXPECT_TRUE(session->cancel(cancelledHandle, std::chrono::steady_clock::time_point::max()));
    const std::array cancelledHandles{cancelledHandle};
    auto cancelledCompletion = session->waitAny(cancelledHandles, std::chrono::steady_clock::time_point::min(), {});
    ASSERT_FALSE(cancelledCompletion);
    EXPECT_EQ(cancelledCompletion.error().kind, BackendFaultKind::DeadlineReached);
    EXPECT_TRUE(session->close(std::chrono::steady_clock::time_point::max()));
}

TEST(SystestExecutionContractTest, EmbeddedCloseReportsWorkerShutdownDeadline)
{
    auto executions = std::make_shared<const PreparedExecutionCatalog>(std::map<TestCaseId, PreparedExecution>{});
    EmbeddedExecutionBackend backend{executions, SingleNodeWorkerConfiguration{}};
    const auto host = Host{"localhost:8080"};
    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 12},
        .relativeTestFile = "suite/embedded-close.test",
        .setupStatements = {},
        .configuration = {},
        .cluster = SystestClusterConfiguration{
            .workers = {WorkerConfig{
                .host = host,
                .dataAddress = "localhost:9090",
                .maxOperators = CapacityKind::Limited{1000},
                .downstream = {},
                .config = {}}},
            .allowSourcePlacement = {host},
            .allowSinkPlacement = {host}}};
    auto opened = backend.open(environment);
    ASSERT_TRUE(opened) << opened.error().message;
    auto session = std::move(*opened);

    const auto closed = session->close(std::chrono::steady_clock::time_point::min());

    ASSERT_FALSE(closed);
    EXPECT_EQ(closed.error().kind, BackendFaultKind::TeardownFailed);
    EXPECT_EQ(closed.error().code, ErrorCode::QueryStopFailed);
    EXPECT_TRUE(closed.error().message.contains("Embedded worker shutdown reached its deadline"));

    const auto closedAgain = session->close(std::chrono::steady_clock::time_point::max());
    ASSERT_FALSE(closedAgain);
    EXPECT_EQ(closedAgain.error().kind, BackendFaultKind::TeardownFailed);
}

TEST(SystestExecutionContractTest, ProductionBackendsPublishCapabilitiesAndRemoteOverrideFault)
{
    auto executions = std::make_shared<const PreparedExecutionCatalog>(std::map<TestCaseId, PreparedExecution>{});
    EmbeddedExecutionBackend embedded{executions, SingleNodeWorkerConfiguration{}};
    RemoteExecutionBackend remote{executions};

    const auto embeddedCapabilities = embedded.capabilities();
    EXPECT_TRUE(embeddedCapabilities.supportsConfigurationOverrides);
    EXPECT_TRUE(embeddedCapabilities.supportsRemoteFixtures);
    EXPECT_TRUE(embeddedCapabilities.supportsExplain);
    EXPECT_GT(embeddedCapabilities.maximumConcurrency, 0);

    const auto remoteCapabilities = remote.capabilities();
    EXPECT_FALSE(remoteCapabilities.supportsConfigurationOverrides);
    EXPECT_TRUE(remoteCapabilities.supportsRemoteFixtures);
    EXPECT_TRUE(remoteCapabilities.supportsExplain);
    EXPECT_GT(remoteCapabilities.maximumConcurrency, 0);

    const auto environment = EnvironmentSpec{
        .id = EnvironmentId{.value = 9},
        .relativeTestFile = "suite/remote.test",
        .setupStatements = {},
        .configuration = EffectiveConfiguration{.values = {{"worker.mode", "A"}}},
        .cluster = {}};
    auto opened = remote.open(environment);
    ASSERT_FALSE(opened);
    EXPECT_EQ(opened.error().kind, BackendFaultKind::Failure);
    EXPECT_EQ(opened.error().code, ErrorCode::InvalidConfigParameter);
    EXPECT_EQ(opened.error().message, "Remote execution does not support worker configuration overrides");
}

}
