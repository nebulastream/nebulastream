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

#include <chrono>
#include <expected>
#include <filesystem>
#include <fstream>
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

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>
#include <SystestCoordinator.hpp>
#include <SystestExecutionBackend.hpp>
#include <SystestParser.hpp>
#include <SystestQueryModel.hpp>
#include <SystestResolver.hpp>
#include <SystestRun.hpp>
#include <SystestState.hpp>
#include <SystestValidation.hpp>

namespace NES::Systest
{
namespace
{

std::filesystem::path temporaryPath(const std::string_view name)
{
    return std::filesystem::temp_directory_path() / fmt::format("{}-{}", name, std::chrono::steady_clock::now().time_since_epoch().count());
}

SystestQuery preparedQuery(
    const std::filesystem::path& testFile,
    const std::filesystem::path& workingDirectory,
    const SystestQueryId queryId,
    ParsedCase parsedCase,
    std::variant<std::vector<std::string>, ExpectedError> expectation)
{
    return SystestQuery{
        .testName = testFile.stem().string(),
        .queryIdInFile = queryId,
        .testFilePath = testFile,
        .workingDir = workingDirectory,
        .queryDefinition = std::get<QueryAction>(parsedCase.action).sql,
        .planInfoOrException = std::unexpected<Exception>{TestException("not used by the fake backend")},
        .expectedResultsOrExpectedError = std::move(expectation),
        .additionalSourceThreads = std::make_shared<std::vector<std::jthread>>(),
        .configurationOverride = ConfigurationOverride{},
        .differentialQueryPlan = std::nullopt,
        .runAfter = std::nullopt,
        .actualExplainOutput = std::nullopt,
        .parsedCase = std::move(parsedCase),
        .fixtureStatements = {}};
}

class CollectingReporter final : public RunReporter
{
public:
    std::expected<void, ReportingDiagnostic> publish(const RunEvent& event) override
    {
        events.push_back(event);
        return {};
    }

    std::vector<RunEvent> events;
};

class FakeExecutionSession final : public ExecutionSession
{
public:
    explicit FakeExecutionSession(std::vector<TestCaseId>& starts) : starts(starts) { }

    std::expected<std::variant<ExecutionHandle, StatementFailure>, BackendFault>
    start(const ExecutionRequest& request, std::stop_token) override
    {
        starts.push_back(request.testCase);
        if (request.testCase.source.queryNumber == SystestQueryId{1})
        {
            return std::variant<ExecutionHandle, StatementFailure>{StatementFailure{
                .stage = ExecutionStage::Planning,
                .error
                = ExecutionError{.kind = ExecutionErrorKind::Statement, .details = {{.code = ErrorCode::TestException, .message = "expected planning failure"}}},
                .artifacts = {}}};
        }
        const auto handle = ExecutionHandle{.value = nextHandle++};
        completions.push_back(StatementCompletion{.handle = handle, .result = TextArtifact{.text = "ok"}, .metrics = {}, .artifacts = {}});
        return std::variant<ExecutionHandle, StatementFailure>{handle};
    }

    std::expected<StatementCompletion, BackendFault>
    waitAny(std::span<const ExecutionHandle> active, std::chrono::steady_clock::time_point, std::stop_token) override
    {
        for (auto completion = completions.begin(); completion != completions.end(); ++completion)
        {
            if (std::ranges::find(active, completion->handle) != active.end())
            {
                auto result = std::move(*completion);
                completions.erase(completion);
                return result;
            }
        }
        return std::unexpected(
            BackendFault{.kind = BackendFaultKind::DeadlineReached, .code = ErrorCode::QueryStatusFailed, .message = "no completion"});
    }

    std::expected<void, BackendFault> cancel(ExecutionHandle, std::chrono::steady_clock::time_point) override { return {}; }

    std::expected<void, BackendFault> close(std::chrono::steady_clock::time_point) override { return {}; }

private:
    std::vector<TestCaseId>& starts;
    uint64_t nextHandle = 1;
    std::vector<StatementCompletion> completions;
};

class FakeExecutionBackend final : public ExecutionBackend
{
public:
    BackendCapabilities capabilities() const override
    {
        return BackendCapabilities{
            .supportsConfigurationOverrides = true, .supportsRemoteFixtures = true, .supportsExplain = true, .maximumConcurrency = 4};
    }

    std::expected<std::unique_ptr<ExecutionSession>, BackendFault> open(const EnvironmentSpec&) override
    {
        return std::unique_ptr<ExecutionSession>{std::make_unique<FakeExecutionSession>(starts)};
    }

    std::vector<TestCaseId> starts;
};

class FaultExecutionSession final : public ExecutionSession
{
public:
    FaultExecutionSession(const BackendFaultKind kind, const bool cancellationFails) : kind(kind), cancellationFails(cancellationFails) { }

    std::expected<std::variant<ExecutionHandle, StatementFailure>, BackendFault> start(const ExecutionRequest&, std::stop_token) override
    {
        if (kind == BackendFaultKind::Failure)
        {
            return std::unexpected(
                BackendFault{.kind = kind, .code = ErrorCode::TestException, .message = "backend infrastructure failure"});
        }
        return std::variant<ExecutionHandle, StatementFailure>{ExecutionHandle{.value = 1}};
    }

    std::expected<StatementCompletion, BackendFault>
    waitAny(std::span<const ExecutionHandle>, std::chrono::steady_clock::time_point, std::stop_token) override
    {
        return std::unexpected(BackendFault{.kind = kind, .code = ErrorCode::TestException, .message = "backend wait failure"});
    }

    std::expected<void, BackendFault> cancel(ExecutionHandle, std::chrono::steady_clock::time_point) override
    {
        if (cancellationFails)
        {
            return std::unexpected(
                BackendFault{.kind = BackendFaultKind::Failure, .code = ErrorCode::QueryStopFailed, .message = "cancellation failed"});
        }
        return {};
    }

    std::expected<void, BackendFault> close(std::chrono::steady_clock::time_point) override { return {}; }

private:
    BackendFaultKind kind;
    bool cancellationFails;
};

class FaultExecutionBackend final : public ExecutionBackend
{
public:
    explicit FaultExecutionBackend(const BackendFaultKind kind, const bool cancellationFails = false)
        : kind(kind), cancellationFails(cancellationFails)
    {
    }

    BackendCapabilities capabilities() const override
    {
        return BackendCapabilities{
            .supportsConfigurationOverrides = true, .supportsRemoteFixtures = true, .supportsExplain = true, .maximumConcurrency = 1};
    }

    std::expected<std::unique_ptr<ExecutionSession>, BackendFault> open(const EnvironmentSpec&) override
    {
        return std::unique_ptr<ExecutionSession>{std::make_unique<FaultExecutionSession>(kind, cancellationFails)};
    }

private:
    BackendFaultKind kind;
    bool cancellationFails;
};

ResolvedRun coordinatorRun(const bool expectedFirstFailure)
{
    const auto testFile = std::filesystem::path{"suite/model.test"};
    const auto workingDirectory = temporaryPath("systest-coordinator");
    const auto firstId
        = TestCaseId{.source = CaseKey{.relativeTestFile = testFile, .queryNumber = SystestQueryId{1}}, .configurationVariant = 0};
    const auto secondId
        = TestCaseId{.source = CaseKey{.relativeTestFile = testFile, .queryNumber = SystestQueryId{2}}, .configurationVariant = 0};
    auto first = std::make_shared<ResolvedCase>(ResolvedCase{
        .id = firstId,
        .environment = EnvironmentId{.value = 1},
        .source = Origin{.file = testFile, .firstLine = 1, .lastLine = 2},
        .action = QueryAction{.sql = "SELECT first;", .kind = QueryKind::Execute},
        .expectation = expectedFirstFailure
            ? CaseExpectation{ErrorExpectation{.code = ErrorCode::TestException, .message = std::nullopt}}
            : CaseExpectation{RowsExpectation{
                  .rows = {}, .comparison = ComparisonPolicy::UnorderedTypedRows, .schema = std::nullopt, .outputDiscarded = false}},
        .dependencies = {}});
    auto second = std::make_shared<ResolvedCase>(ResolvedCase{
        .id = secondId,
        .environment = EnvironmentId{.value = 1},
        .source = Origin{.file = testFile, .firstLine = 3, .lastLine = 4},
        .action = QueryAction{.sql = "EXPLAIN SELECT second;", .kind = QueryKind::Explain},
        .expectation = TextExpectation{.lines = {"ok"}, .matching = TextMatchPolicy::Exact},
        .dependencies = {firstId}});

    auto catalog = std::make_shared<PreparedCaseCatalog>();
    std::variant<std::vector<std::string>, ExpectedError> firstExpectation = expectedFirstFailure
        ? std::variant<std::vector<std::string>, ExpectedError>{ExpectedError{.code = ErrorCode::TestException, .message = std::nullopt}}
        : std::variant<std::vector<std::string>, ExpectedError>{std::vector<std::string>{}};
    catalog->insert(
        firstId,
        PreparedCase{
            .definition = first,
            .query = preparedQuery(
                testFile,
                workingDirectory,
                SystestQueryId{1},
                ParsedCase{
                    .key = firstId.source,
                    .source = first->source,
                    .action = first->action,
                    .expectation = first->expectation,
                    .runAfter = std::nullopt,
                    .configuration = {}},
                std::move(firstExpectation))});
    catalog->insert(
        secondId,
        PreparedCase{
            .definition = second,
            .query = preparedQuery(
                testFile,
                workingDirectory,
                SystestQueryId{2},
                ParsedCase{
                    .key = secondId.source,
                    .source = second->source,
                    .action = second->action,
                    .expectation = second->expectation,
                    .runAfter = firstId.source,
                    .configuration = {}},
                std::vector<std::string>{"ok"})});

    return ResolvedRun{
        .environments = {EnvironmentSpec{.id = EnvironmentId{.value = 1}, .setupStatements = {}, .configuration = {}, .cluster = {}}},
        .cases = {first, second},
        .preparedCases = std::move(catalog)};
}

RunSetup runSetup(const ResolvedRun& run)
{
    return RunSetup{
        .selection = TestSelection{.includeAll = false, .cases = run.preparedCases->ids()},
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

TEST(SystestRedesignTest, ParserProducesCasesFixturesOriginsAndDependencies)
{
    const auto file = temporaryPath("systest-parser-model");
    std::ofstream(file) << R"(
# ignored
GlobalConfiguration worker.mode: [A, B]
CREATE LOGICAL SOURCE input(id UINT64);

SELECT id FROM input INTO File();
----
1

SEQUENTIAL_EXECUTION
Configuration worker.size: 8
SELECT id FROM input INTO File();
====
SELECT id + UINT64(0) FROM input INTO File();

SEQUENTIAL_EXECUTION

EXPLAIN (LOGICAL) FORMAT TEXT SELECT id FROM input INTO File();
----
<REGEX>SOURCE</REGEX>
==END==
)";

    SystestParser parser;
    parser.registerOnCreateCallback([](std::string, auto) { });
    parser.registerOnQueryCallback([](std::string, SystestQueryId, bool) { });
    parser.registerOnExplainQueryCallback([](std::string, SystestQueryId) { });
    parser.registerOnResultTuplesCallback([](std::vector<std::string>&&, SystestQueryId) { });
    parser.registerOnDifferentialQueryBlockCallback([](std::string, std::string, SystestQueryId, SystestQueryId) { });
    parser.registerOnConfigurationCallback([](const auto&) { });
    parser.registerOnGlobalConfigurationCallback([](const auto&) { });

    ASSERT_TRUE(parser.loadFile(file, "parser/model.test"));
    parser.parse();

    ASSERT_EQ(parser.fixtureStatements().size(), 1);
    ASSERT_EQ(parser.parsedCases().size(), 3);
    const auto& first = parser.parsedCases()[0];
    const auto& differential = parser.parsedCases()[1];
    const auto& explain = parser.parsedCases()[2];
    EXPECT_EQ(first.key.relativeTestFile, "parser/model.test");
    EXPECT_EQ(first.key.queryNumber, SystestQueryId{1});
    EXPECT_EQ(first.source.file, std::filesystem::weakly_canonical(file));
    EXPECT_LT(first.source.firstLine, first.source.lastLine);
    ASSERT_EQ(first.configuration.size(), 1);
    EXPECT_TRUE(first.configuration.front().global);
    ASSERT_TRUE(differential.runAfter);
    EXPECT_EQ(differential.runAfter->queryNumber, SystestQueryId{1});
    EXPECT_TRUE(std::holds_alternative<DifferentialAction>(differential.action));
    EXPECT_TRUE(std::holds_alternative<DifferentialExpectation>(differential.expectation));
    ASSERT_EQ(differential.configuration.size(), 2);
    EXPECT_TRUE(std::holds_alternative<QueryAction>(explain.action));
    EXPECT_EQ(std::get<QueryAction>(explain.action).kind, QueryKind::Explain);
    EXPECT_TRUE(std::holds_alternative<TextExpectation>(explain.expectation));
    EXPECT_TRUE(explain.configuration.empty());
    std::filesystem::remove(file);
}

TEST(SystestRedesignTest, ResolverPreservesSequentialConfigurationEpochs)
{
    const auto root = temporaryPath("systest-resolver-epochs");
    std::filesystem::create_directories(root);
    const auto testFile = root / "epochs.test";
    const auto workingDirectory = root / "working";
    const auto makeQuery
        = [&](const SystestQueryId queryId, ConfigurationOverride configuration, const std::optional<SystestQueryId> dependency)
    {
        const auto key = CaseKey{.relativeTestFile = "epochs.test", .queryNumber = queryId};
        auto query = preparedQuery(
            testFile,
            workingDirectory,
            queryId,
            ParsedCase{
                .key = key,
                .source = Origin{.file = testFile, .firstLine = queryId.getRawValue(), .lastLine = queryId.getRawValue()},
                .action = QueryAction{.sql = "SELECT 1 INTO File();", .kind = QueryKind::Execute},
                .expectation
                = RowsExpectation{.rows = {"1"}, .comparison = ComparisonPolicy::UnorderedTypedRows, .schema = std::nullopt, .outputDiscarded = false},
                .runAfter = dependency.transform([&](const SystestQueryId predecessor)
                                                 { return CaseKey{.relativeTestFile = "epochs.test", .queryNumber = predecessor}; }),
                .configuration = {}},
            std::vector<std::string>{"1"});
        query.configurationOverride = std::move(configuration);
        return query;
    };

    ConfigurationOverride base;
    base.overrideParameters.emplace("worker.mode", "A");
    ConfigurationOverride local = base;
    local.overrideParameters.emplace("worker.size", "8");

    std::vector<SystestQuery> queries;
    queries.push_back(makeQuery(SystestQueryId{1}, base, std::nullopt));
    queries.push_back(makeQuery(SystestQueryId{2}, local, SystestQueryId{1}));
    queries.push_back(makeQuery(SystestQueryId{3}, base, SystestQueryId{2}));

    auto resolved = resolveSystestQueries(std::move(queries), root, SystestClusterConfiguration{});
    ASSERT_TRUE(resolved) << resolved.error().what();
    ASSERT_EQ(resolved->cases.size(), 3);
    ASSERT_EQ(resolved->environments.size(), 3);
    EXPECT_EQ(resolved->cases[1]->dependencies, std::vector<TestCaseId>{resolved->cases[0]->id});
    EXPECT_EQ(resolved->cases[2]->dependencies, std::vector<TestCaseId>{resolved->cases[1]->id});
    EXPECT_NE(resolved->cases[0]->environment, resolved->cases[2]->environment);
}

TEST(SystestRedesignTest, ResolverKeepsGlobalConfigurationLineagesIndependent)
{
    const auto root = temporaryPath("systest-resolver-lineages");
    std::filesystem::create_directories(root);
    const auto testFile = root / "lineages.test";
    const auto workingDirectory = root / "working";
    const auto makeQuery = [&](const SystestQueryId queryId,
                               const std::string& globalValue,
                               const bool hasLocalConfiguration,
                               const std::optional<SystestQueryId> dependency)
    {
        const auto key = CaseKey{.relativeTestFile = "lineages.test", .queryNumber = queryId};
        auto query = preparedQuery(
            testFile,
            workingDirectory,
            queryId,
            ParsedCase{
                .key = key,
                .source = Origin{.file = testFile, .firstLine = queryId.getRawValue(), .lastLine = queryId.getRawValue()},
                .action = QueryAction{.sql = "SELECT 1 INTO File();", .kind = QueryKind::Execute},
                .expectation
                = RowsExpectation{.rows = {"1"}, .comparison = ComparisonPolicy::UnorderedTypedRows, .schema = std::nullopt, .outputDiscarded = false},
                .runAfter = dependency.transform([&](const SystestQueryId predecessor)
                                                 { return CaseKey{.relativeTestFile = "lineages.test", .queryNumber = predecessor}; }),
                .configuration
                = {ConfigurationDirective{.key = "worker.mode", .values = {"A", "B"}, .global = true, .source = Origin{.file = testFile}}}},
            std::vector<std::string>{"1"});
        query.configurationOverride.overrideParameters.emplace("worker.mode", globalValue);
        if (hasLocalConfiguration)
        {
            query.configurationOverride.overrideParameters.emplace("worker.size", "8");
        }
        return query;
    };

    std::vector<SystestQuery> queries;
    queries.push_back(makeQuery(SystestQueryId{1}, "A", true, std::nullopt));
    queries.push_back(makeQuery(SystestQueryId{1}, "B", true, std::nullopt));
    queries.push_back(makeQuery(SystestQueryId{2}, "A", false, SystestQueryId{1}));
    queries.push_back(makeQuery(SystestQueryId{2}, "B", false, SystestQueryId{1}));

    auto resolved = resolveSystestQueries(std::move(queries), root, SystestClusterConfiguration{});
    ASSERT_TRUE(resolved) << resolved.error().what();
    ASSERT_EQ(resolved->cases.size(), 4);
    EXPECT_EQ(resolved->cases[2]->dependencies, std::vector<TestCaseId>{resolved->cases[0]->id});
    EXPECT_EQ(resolved->cases[3]->dependencies, std::vector<TestCaseId>{resolved->cases[1]->id});
}

TEST(SystestRedesignTest, ResolverAssignsStableCaseVariantsAndEnvironments)
{
    const auto root = temporaryPath("systest-resolver");
    const auto testFile = root / "suite" / "variants.test";
    std::filesystem::create_directories(testFile.parent_path());
    std::ofstream(testFile) << "";
    const auto origin = Origin{.file = testFile, .firstLine = 1, .lastLine = 2};
    const auto parsed = ParsedCase{
        .key = CaseKey{.relativeTestFile = "variants.test", .queryNumber = SystestQueryId{1}},
        .source = origin,
        .action = QueryAction{.sql = "SELECT value;", .kind = QueryKind::Execute},
        .expectation = ErrorExpectation{.code = ErrorCode::TestException, .message = std::nullopt},
        .runAfter = std::nullopt,
        .configuration = {}};
    auto first = preparedQuery(
        testFile, root / "work", SystestQueryId{1}, parsed, ExpectedError{.code = ErrorCode::TestException, .message = std::nullopt});
    auto second = preparedQuery(
        testFile, root / "work", SystestQueryId{1}, parsed, ExpectedError{.code = ErrorCode::TestException, .message = std::nullopt});
    first.configurationOverride["worker.mode"] = "A";
    second.configurationOverride["worker.mode"] = "B";

    auto resolved = resolveSystestQueries({std::move(first), std::move(second)}, root, {});

    ASSERT_TRUE(resolved);
    ASSERT_EQ(resolved->cases.size(), 2);
    ASSERT_EQ(resolved->environments.size(), 2);
    EXPECT_EQ(resolved->cases[0]->id.source.relativeTestFile, "suite/variants.test");
    EXPECT_EQ(resolved->cases[0]->id.configurationVariant, 0);
    EXPECT_EQ(resolved->cases[1]->id.configurationVariant, 1);
    EXPECT_NE(resolved->cases[0]->environment, resolved->cases[1]->environment);
    std::filesystem::remove_all(root);
}

TEST(SystestRedesignTest, ExpectedFailureVerdictReleasesDependency)
{
    auto run = coordinatorRun(true);
    FakeExecutionBackend backend;
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator(decoder, resultComparator, textComparator);
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 2);
    EXPECT_EQ(summary.failed, 0);
    EXPECT_EQ(summary.skipped, 0);
    ASSERT_EQ(backend.starts.size(), 2);
    EXPECT_EQ(backend.starts[0].source.queryNumber, SystestQueryId{1});
    EXPECT_EQ(backend.starts[1].source.queryNumber, SystestQueryId{2});
}

TEST(SystestRedesignTest, BackendFaultDoesNotSatisfyErrorExpectation)
{
    auto run = coordinatorRun(true);
    FaultExecutionBackend backend(BackendFaultKind::Failure);
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator(decoder, resultComparator, textComparator);
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 0);
    EXPECT_EQ(summary.failed, 2);
    EXPECT_EQ(summary.skipped, 0);
}

TEST(SystestRedesignTest, DeadlineProducesTimedOutVerdict)
{
    auto run = coordinatorRun(true);
    FaultExecutionBackend backend(BackendFaultKind::DeadlineReached);
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator(decoder, resultComparator, textComparator);
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.deadlines.caseTimeout = std::chrono::milliseconds{1};

    const auto summary = RunCoordinator{}.run(run, std::move(setup), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 0);
    EXPECT_EQ(summary.failed, 2);
    EXPECT_EQ(summary.skipped, 0);
    ASSERT_FALSE(summary.results.empty());
    EXPECT_TRUE(summary.results.front().diagnostics.front().message.contains("timed out"));
}

TEST(SystestRedesignTest, FailedCancellationStopsTheSession)
{
    auto run = coordinatorRun(true);
    FaultExecutionBackend backend(BackendFaultKind::DeadlineReached, true);
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator(decoder, resultComparator, textComparator);
    CollectingReporter reporter;
    auto setup = runSetup(run);
    setup.deadlines.caseTimeout = std::chrono::milliseconds{1};

    const auto summary = RunCoordinator{}.run(run, std::move(setup), backend, validator, reporter);

    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 1);
    EXPECT_TRUE(summary.cancelled);
}

TEST(SystestRedesignTest, FailedDependencyStillReleasesSuccessor)
{
    auto run = coordinatorRun(false);
    FakeExecutionBackend backend;
    FileResultDecoder decoder;
    ResultComparator resultComparator;
    TextComparator textComparator;
    CaseValidator validator(decoder, resultComparator, textComparator);
    CollectingReporter reporter;

    const auto summary = RunCoordinator{}.run(run, runSetup(run), backend, validator, reporter);

    EXPECT_EQ(summary.passed, 1);
    EXPECT_EQ(summary.failed, 1);
    EXPECT_EQ(summary.skipped, 0);
    ASSERT_EQ(backend.starts.size(), 2);
    EXPECT_EQ(backend.starts.front().source.queryNumber, SystestQueryId{1});
}

}
