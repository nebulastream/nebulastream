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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include <gtest/gtest.h>
#include <SystestQueryModel.hpp>
#include <SystestReporter.hpp>
#include <SystestResolver.hpp>
#include <SystestRun.hpp>

namespace NES::Systest
{
namespace
{

ResolvedRun makeResolvedRun(
    std::filesystem::path relativeTestFile,
    const uint64_t queryNumber,
    CaseAction action,
    CaseExpectation expectation,
    std::vector<std::pair<std::string, std::string>> configuration)
{
    const auto environmentId = EnvironmentId{.value = 17};
    const auto testCaseId = TestCaseId{
        .source = CaseKey{.relativeTestFile = relativeTestFile, .queryNumber = SystestQueryId{queryNumber}}, .configurationVariant = 0};
    auto testCase = std::make_shared<const ResolvedCase>(ResolvedCase{
        .id = testCaseId,
        .environment = environmentId,
        .source = Origin{.file = relativeTestFile, .firstLine = 21, .lastLine = 24},
        .action = std::move(action),
        .expectation = std::move(expectation),
        .dependencies = {}});
    auto environment = EnvironmentSpec{
        .id = environmentId,
        .relativeTestFile = std::move(relativeTestFile),
        .setupStatements = {},
        .configuration = EffectiveConfiguration{.values = std::move(configuration)},
        .cluster = {}};
    return ResolvedRun{.environments = {std::move(environment)}, .cases = {std::move(testCase)}};
}

RunSetup setupFor(const ResolvedRun& run, RepetitionPolicy repetition)
{
    return RunSetup{
        .selection = TestSelection{.includeAll = false, .cases = run.ids(), .intentionalSkips = {}},
        .ordering = OrderingPolicy{.kind = OrderingKind::SourceOrder, .seed = std::nullopt},
        .concurrency = ConcurrencyPolicy{.maximumActiveCases = 1},
        .repetition = std::move(repetition),
        .failurePolicy = IndependentFailurePolicy::Continue,
        .deadlines
        = DeadlinePolicy{.caseTimeout = std::chrono::milliseconds{30000}, .runTimeout = std::chrono::milliseconds{60000}, .cancellationGracePeriod = std::chrono::milliseconds{1000}},
        .validation = ValidationPolicy{.enabled = true},
        .metrics = MetricsPolicy{.collect = true, .report = true}};
}

ExecutionMetrics measuredMetrics(const std::chrono::milliseconds elapsed, const uint64_t bytes, const uint64_t tuples)
{
    const auto started = std::chrono::system_clock::time_point{std::chrono::seconds{100}};
    return ExecutionMetrics{.started = started, .finished = started + elapsed, .bytesProcessed = bytes, .tuplesProcessed = tuples};
}

std::string publishAndCapture(RunReporter& reporter, const std::initializer_list<RunEvent> events)
{
    testing::internal::CaptureStdout();
    std::optional<std::string> reportingError;
    for (const auto& event : events)
    {
        if (const auto published = reporter.publish(event); !published && !reportingError)
        {
            reportingError = published.error().message;
        }
    }
    std::cout.flush();
    auto output = testing::internal::GetCapturedStdout();
    EXPECT_FALSE(reportingError.has_value()) << reportingError.value_or("");
    return output;
}

class TemporaryDirectory
{
public:
    TemporaryDirectory()
    {
        static std::atomic_uint64_t sequence = 0;
        const auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
        path = std::filesystem::temp_directory_path()
            / ("nes-systest-reporter-" + std::to_string(timestamp) + "-" + std::to_string(sequence.fetch_add(1)));
        std::filesystem::create_directories(path);
    }

    ~TemporaryDirectory()
    {
        std::error_code error;
        std::filesystem::remove_all(path, error);
    }

    TemporaryDirectory(const TemporaryDirectory&) = delete;
    TemporaryDirectory& operator=(const TemporaryDirectory&) = delete;
    TemporaryDirectory(TemporaryDirectory&&) = delete;
    TemporaryDirectory& operator=(TemporaryDirectory&&) = delete;

    [[nodiscard]] const std::filesystem::path& get() const { return path; }

private:
    std::filesystem::path path;
};

}

TEST(SystestReporterTest, ConsoleUsesResolvedCaseLabelProgressAndSortedConfigurationOverrides)
{
    const auto run = makeResolvedRun(
        "reporting/temperature-window.test",
        7,
        QueryAction{.sql = "SELECT AVG(temperature) FROM sensor INTO Void();", .kind = QueryKind::Execute},
        RowsExpectation{.rows = {}, .comparison = ComparisonPolicy::UnorderedTypedRows},
        {{"worker.threads", "8"}, {"query.compiler", "NAUTILUS"}});
    const auto setup = setupFor(run, FixedRepetitions{.count = 2});
    const auto result
        = ValidatedResult{.id = run.cases.front()->id, .verdict = Verdict::Passed, .diagnostics = {}, .metrics = {}, .artifacts = {}};
    ConsoleRunReporter reporter{run, false};

    const auto output = publishAndCapture(reporter, {RunStarted{.plan = setup}, CaseFinished{.result = result}});

    EXPECT_NE(
        output.find("1/2 ( 50.0%) reporting/temperature-window.test:07 [query.compiler=NAUTILUS, worker.threads=8]"), std::string::npos)
        << output;
}

TEST(SystestReporterTest, ConsoleUsesResolvedPrimarySqlAndPreservesFailureDiagnostics)
{
    const auto primarySql = std::string{"SELECT temperature FROM primary_sensor INTO File();"};
    const auto comparisonSql = std::string{"SELECT temperature FROM comparison_sensor INTO File();"};
    const auto run = makeResolvedRun(
        "reporting/differential-temperature.test",
        3,
        DifferentialAction{.leftSql = primarySql, .rightSql = comparisonSql},
        DifferentialExpectation{},
        {});
    const auto setup = setupFor(run, Once{});
    const auto result = ValidatedResult{
        .id = run.cases.front()->id,
        .verdict = Verdict::Failed,
        .diagnostics
        = {{.kind = DiagnosticKind::Validation, .message = "expected 12 rows but received 11", .source = std::nullopt},
           {.kind = DiagnosticKind::Execution, .message = "result checksum differed", .source = std::nullopt}},
        .metrics = {},
        .artifacts = {}};
    ConsoleRunReporter reporter{run, false};

    const auto output = publishAndCapture(reporter, {RunStarted{.plan = setup}, CaseFinished{.result = result}});

    EXPECT_NE(output.find(primarySql), std::string::npos) << output;
    EXPECT_EQ(output.find(comparisonSql), std::string::npos) << output;
    EXPECT_NE(output.find("Error: expected 12 rows but received 11\nresult checksum differed"), std::string::npos) << output;
}

TEST(SystestReporterTest, ConsoleIncludesElapsedTimeAndThroughputWhenPerformanceIsEnabled)
{
    const auto run = makeResolvedRun(
        "reporting/throughput.test",
        12,
        QueryAction{.sql = "SELECT value FROM measurements INTO Void();", .kind = QueryKind::Execute},
        RowsExpectation{.rows = {}, .comparison = ComparisonPolicy::UnorderedTypedRows},
        {});
    const auto setup = setupFor(run, Once{});
    const auto result = ValidatedResult{
        .id = run.cases.front()->id,
        .verdict = Verdict::Passed,
        .diagnostics = {},
        .metrics = measuredMetrics(std::chrono::milliseconds{2000}, 4000, 6000),
        .artifacts = {}};
    ConsoleRunReporter reporter{run, true};

    const auto output = publishAndCapture(reporter, {RunStarted{.plan = setup}, CaseFinished{.result = result}});

    EXPECT_NE(output.find("in 2s (2.000 kB/s / 3.000 kTup/s)"), std::string::npos) << output;
}

TEST(SystestReporterTest, ConsoleCountsAndExplainsIntentionalSkips)
{
    const auto run = makeResolvedRun(
        "benchmarks/skipped.test",
        1,
        QueryAction{.sql = "EXPLAIN (LOGICAL) SELECT 1 INTO File();", .kind = QueryKind::Explain},
        TextExpectation{.lines = {}, .matching = TextMatchPolicy::Exact},
        {});
    auto setup = setupFor(run, Once{});
    setup.selection.intentionalSkips = {{.id = run.cases.front()->id, .reason = "not benchmarkable"}};
    const auto result = ValidatedResult{
        .id = run.cases.front()->id,
        .verdict = Verdict::Skipped,
        .diagnostics = {{.kind = DiagnosticKind::Scheduling, .message = "not benchmarkable", .source = std::nullopt}},
        .metrics = {},
        .artifacts = {}};
    ConsoleRunReporter reporter{run, false};

    const auto output = publishAndCapture(reporter, {RunStarted{.plan = setup}, CaseFinished{.result = result}});

    EXPECT_NE(output.find("1/1 (100.0%)"), std::string::npos) << output;
    EXPECT_NE(output.find("SKIPPED (not benchmarkable)"), std::string::npos) << output;
}

TEST(SystestReporterTest, ConsoleUsesGrowingTotalsForUnboundedRuns)
{
    const auto run = makeResolvedRun(
        "reporting/endless.test",
        1,
        QueryAction{.sql = "SELECT 1 INTO Void();", .kind = QueryKind::Execute},
        RowsExpectation{.rows = {}, .comparison = ComparisonPolicy::UnorderedTypedRows},
        {});
    const auto setup = setupFor(run, UntilCancelled{});
    const auto result
        = ValidatedResult{.id = run.cases.front()->id, .verdict = Verdict::Passed, .diagnostics = {}, .metrics = {}, .artifacts = {}};
    ConsoleRunReporter reporter{run, false};

    const auto output
        = publishAndCapture(reporter, {RunStarted{.plan = setup}, CaseFinished{.result = result}, CaseFinished{.result = result}});

    EXPECT_NE(output.find("1/1 (100.0%)"), std::string::npos) << output;
    EXPECT_NE(output.find("2/2 (100.0%)"), std::string::npos) << output;
    EXPECT_EQ(output.find("2/1"), std::string::npos) << output;
}

TEST(SystestReporterTest, BenchmarkWritesRenamedQueryKeyToSuppliedResultsPath)
{
    const auto run = makeResolvedRun(
        "benchmarks/window-throughput.test",
        4,
        QueryAction{.sql = "SELECT SUM(value) FROM measurements INTO Void();", .kind = QueryKind::Execute},
        RowsExpectation{.rows = {}, .comparison = ComparisonPolicy::UnorderedTypedRows},
        {});
    const auto setup = setupFor(run, Once{});
    const auto result = ValidatedResult{
        .id = run.cases.front()->id,
        .verdict = Verdict::Passed,
        .diagnostics = {},
        .metrics = measuredMetrics(std::chrono::milliseconds{2000}, 4000, 6000),
        .artifacts = {}};
    const auto summary = RunSummary{
        .setup = setup,
        .results = {result},
        .passed = 1,
        .failed = 0,
        .skipped = 0,
        .elapsed = std::chrono::milliseconds{2000},
        .cancelled = false,
        .diagnostics = {}};
    const TemporaryDirectory directory;
    const auto outputFile = directory.get() / "BenchmarkResults.json";
    BenchmarkRunReporter reporter{run, outputFile};

    const auto consoleOutput
        = publishAndCapture(reporter, {RunStarted{.plan = setup}, CaseFinished{.result = result}, RunFinished{.summary = summary}});

    ASSERT_TRUE(std::filesystem::is_regular_file(outputFile));
    std::ifstream input{outputFile};
    ASSERT_TRUE(input.is_open());
    std::ostringstream contents;
    contents << input.rdbuf();
    const auto serialized = contents.str();
    EXPECT_EQ(serialized, consoleOutput);
    EXPECT_NE(serialized.find("\"query name\": \"benchmarks/window-throughput.test:4:variant=0\""), std::string::npos) << serialized;
    EXPECT_EQ(serialized.find("\"queryName\""), std::string::npos) << serialized;
}

TEST(SystestReporterTest, BenchmarkReportsBufferedWriteFailure)
{
    const auto outputFile = std::filesystem::path{"/dev/full"};
    std::error_code error;
    if (!std::filesystem::is_character_file(outputFile, error) || error)
    {
        GTEST_SKIP() << "/dev/full is unavailable";
    }
    const auto run = makeResolvedRun(
        "benchmarks/write-failure.test",
        1,
        QueryAction{.sql = "SELECT 1 INTO Void();", .kind = QueryKind::Execute},
        RowsExpectation{.rows = {}, .comparison = ComparisonPolicy::UnorderedTypedRows},
        {});
    const auto setup = setupFor(run, Once{});
    const auto summary = RunSummary{
        .setup = setup, .results = {}, .passed = 0, .failed = 0, .skipped = 0, .elapsed = {}, .cancelled = false, .diagnostics = {}};
    BenchmarkRunReporter reporter{run, outputFile};
    ASSERT_TRUE(reporter.publish(RunStarted{.plan = setup}));

    testing::internal::CaptureStdout();
    const auto published = reporter.publish(RunFinished{.summary = summary});
    static_cast<void>(testing::internal::GetCapturedStdout());

    ASSERT_FALSE(published);
    EXPECT_TRUE(published.error().message.contains("Failed to write benchmark output"));
    EXPECT_TRUE(published.error().message.contains("/dev/full"));
}

TEST(SystestReporterTest, BenchmarkLabelsDistinguishDirectoriesAndConfigurationVariantsAndExcludeSkips)
{
    const auto firstEnvironment = EnvironmentSpec{
        .id = EnvironmentId{.value = 1},
        .relativeTestFile = "first/same.test",
        .setupStatements = {},
        .configuration = EffectiveConfiguration{.values = {{"mode", "A"}}},
        .cluster = {}};
    const auto secondEnvironment = EnvironmentSpec{
        .id = EnvironmentId{.value = 2},
        .relativeTestFile = "second/same.test",
        .setupStatements = {},
        .configuration = EffectiveConfiguration{.values = {{"mode", "B"}}},
        .cluster = {}};
    const auto firstId
        = TestCaseId{.source = CaseKey{.relativeTestFile = "first/same.test", .queryNumber = SystestQueryId{1}}, .configurationVariant = 0};
    const auto variantId
        = TestCaseId{.source = CaseKey{.relativeTestFile = "first/same.test", .queryNumber = SystestQueryId{1}}, .configurationVariant = 1};
    const auto secondId = TestCaseId{
        .source = CaseKey{.relativeTestFile = "second/same.test", .queryNumber = SystestQueryId{1}}, .configurationVariant = 0};
    const auto skippedId = TestCaseId{
        .source = CaseKey{.relativeTestFile = "second/same.test", .queryNumber = SystestQueryId{2}}, .configurationVariant = 0};
    const auto failedId = TestCaseId{
        .source = CaseKey{.relativeTestFile = "second/same.test", .queryNumber = SystestQueryId{3}}, .configurationVariant = 0};
    const auto makeCase = [](const TestCaseId& testCaseId, const EnvironmentId environment)
    {
        return std::make_shared<const ResolvedCase>(ResolvedCase{
            .id = testCaseId,
            .environment = environment,
            .source = Origin{.file = testCaseId.source.relativeTestFile, .firstLine = 1, .lastLine = 3},
            .action = QueryAction{.sql = "SELECT 1 INTO Void();", .kind = QueryKind::Execute},
            .expectation = RowsExpectation{.rows = {}, .comparison = ComparisonPolicy::UnorderedTypedRows},
            .dependencies = {}});
    };
    const auto run = ResolvedRun{
        .environments = {firstEnvironment, secondEnvironment},
        .cases
        = {makeCase(firstId, firstEnvironment.id),
           makeCase(variantId, firstEnvironment.id),
           makeCase(secondId, secondEnvironment.id),
           makeCase(skippedId, secondEnvironment.id),
           makeCase(failedId, secondEnvironment.id)}};
    auto setup = setupFor(run, Once{});
    const auto measured = measuredMetrics(std::chrono::milliseconds{1000}, 100, 10);
    const auto result = [&](const TestCaseId& testCaseId, const Verdict verdict)
    { return ValidatedResult{.id = testCaseId, .verdict = verdict, .diagnostics = {}, .metrics = measured, .artifacts = {}}; };
    const TemporaryDirectory directory;
    const auto outputFile = directory.get() / "BenchmarkResults.json";
    BenchmarkRunReporter reporter{run, outputFile};
    const auto summary = RunSummary{
        .setup = setup,
        .results = {},
        .passed = 3,
        .failed = 1,
        .skipped = 1,
        .elapsed = std::chrono::milliseconds{1000},
        .cancelled = false,
        .diagnostics = {}};

    const auto output = publishAndCapture(
        reporter,
        {RunStarted{.plan = setup},
         CaseFinished{.result = result(firstId, Verdict::Passed)},
         CaseFinished{.result = result(variantId, Verdict::Passed)},
         CaseFinished{.result = result(secondId, Verdict::Passed)},
         CaseFinished{.result = result(skippedId, Verdict::Skipped)},
         CaseFinished{.result = result(failedId, Verdict::Failed)},
         RunFinished{.summary = summary}});

    EXPECT_NE(output.find("first/same.test:1:variant=0"), std::string::npos) << output;
    EXPECT_NE(output.find("second/same.test:1:variant=0"), std::string::npos) << output;
    EXPECT_NE(output.find("first/same.test:1:variant=1"), std::string::npos) << output;
    EXPECT_EQ(output.find("second/same.test:2:variant=0"), std::string::npos) << output;
    EXPECT_EQ(output.find("second/same.test:3:variant=0"), std::string::npos) << output;
}

}
