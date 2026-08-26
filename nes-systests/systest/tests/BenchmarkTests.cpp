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
#include <iterator>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Model/Expectation.hpp>
#include <Model/RunnableTestFile.hpp>
#include <Model/TestCaseId.hpp>
#include <Model/Verdict.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <Benchmark.hpp>
#include <ErrorHandling.hpp>
#include <TemporaryDirectory.hpp>

namespace NES
{

namespace
{

RewrittenTestCase createQueryCase(Expectation expected)
{
    return RewrittenTestCase{
        .action = RewrittenQuery{
            .sql = "", .id = SystestQueryId{1}, .resultFile = std::nullopt, .inputFiles = {}, .expected = std::move(expected)}};
}

ReportEntry createEntryWith(Verdict verdict)
{
    return ReportEntry{
        .id = TestCaseId{.originFile = "errors/Measured", .queryIdInFile = SystestQueryId{1}, .overrides = {}},
        .outcome = std::move(verdict)};
}

std::vector<StatementTiming> createOneTiming(const std::chrono::milliseconds execution)
{
    return {StatementTiming{.execution = execution}};
}

}

class BenchmarkTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("BenchmarkTest.log", LogLevel::LOG_DEBUG); }

    static std::filesystem::path writeFileWith(const std::filesystem::path& dir, const std::string& name, const std::string& content)
    {
        const auto path = dir / name;
        std::ofstream out{path};
        out << content;
        return path;
    }
};

TEST_F(BenchmarkTest, DropsAQueryThatTookNoMeasurableTime)
{
    Benchmark benchmark;
    benchmark.record("q", {}, std::chrono::milliseconds{0});
    benchmark.record("q", {}, std::chrono::milliseconds{-5});
    EXPECT_TRUE(benchmark.buildRows().empty());
}

TEST_F(BenchmarkTest, KeepsTheBestTimeAcrossRounds)
{
    Benchmark benchmark;
    benchmark.record("q", {}, std::chrono::milliseconds{500});
    benchmark.record("q", {}, std::chrono::milliseconds{250});
    benchmark.record("q", {}, std::chrono::milliseconds{750});
    const auto rows = benchmark.buildRows();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_DOUBLE_EQ(rows.front().time, 0.25);
}

TEST_F(BenchmarkTest, ComputesTheRatesFromTheInputFiles)
{
    const Testing::TemporaryDirectory tmp;
    const auto input = writeFileWith(tmp.get(), "input.csv", "1\n2\n");
    Benchmark benchmark;
    benchmark.record("q", {input}, std::chrono::milliseconds{500});
    const auto rows = benchmark.buildRows();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_DOUBLE_EQ(rows.front().bytesPerSecond, 8.0);
    EXPECT_DOUBLE_EQ(rows.front().tuplesPerSecond, 4.0);
}

TEST_F(BenchmarkTest, CountsAFileOncePerReference)
{
    const Testing::TemporaryDirectory tmp;
    const auto input = writeFileWith(tmp.get(), "input.csv", "1\n2\n");
    Benchmark benchmark;
    benchmark.record("q", {input, input}, std::chrono::milliseconds{500});
    const auto rows = benchmark.buildRows();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_DOUBLE_EQ(rows.front().bytesPerSecond, 16.0);
    EXPECT_DOUBLE_EQ(rows.front().tuplesPerSecond, 8.0);
}

TEST_F(BenchmarkTest, AMissingInputFileContributesNothing)
{
    const Testing::TemporaryDirectory tmp;
    Benchmark benchmark;
    benchmark.record("q", {tmp.get() / "absent.csv"}, std::chrono::milliseconds{500});
    const auto rows = benchmark.buildRows();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_DOUBLE_EQ(rows.front().bytesPerSecond, 0.0);
    EXPECT_DOUBLE_EQ(rows.front().tuplesPerSecond, 0.0);
}

/// Stable order keeps reports diffable across runs.
TEST_F(BenchmarkTest, KeepsTheOrderInWhichQueriesWereFirstMeasured)
{
    Benchmark benchmark;
    benchmark.record("b", {}, std::chrono::milliseconds{100});
    benchmark.record("a", {}, std::chrono::milliseconds{100});
    benchmark.record("b", {}, std::chrono::milliseconds{50});
    const auto rows = benchmark.buildRows();
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(rows.at(0).queryName.value(), "b");
    EXPECT_EQ(rows.at(1).queryName.value(), "a");
}

/// Readers expect the key `query name`, with a space.
TEST_F(BenchmarkTest, WritesTheHistoricalJsonKey)
{
    const Testing::TemporaryDirectory tmp;
    Benchmark benchmark;
    benchmark.record("q", {}, std::chrono::milliseconds{100});
    std::ignore = benchmark.writeTo(tmp.get() / "report.json");
    std::ifstream contents{tmp.get() / "report.json"};
    const std::string json{std::istreambuf_iterator<char>{contents}, std::istreambuf_iterator<char>{}};
    EXPECT_TRUE(json.contains("\"query name\"")) << json;
}

TEST_F(BenchmarkTest, CreatesTheReportDirectoryAndRejectsAnUnwritablePath)
{
    const Testing::TemporaryDirectory tmp;
    const Benchmark benchmark;
    std::ignore = benchmark.writeTo(tmp.get() / "nested" / "deeper" / "report.json");
    EXPECT_TRUE(std::filesystem::exists(tmp.get() / "nested" / "deeper" / "report.json"));
    EXPECT_THROW(std::ignore = benchmark.writeTo(tmp.get()), Exception);
}

/// The report uses the name that the progress line prints.
TEST_F(BenchmarkTest, RecordsAPassingQueryUnderItsReportName)
{
    Benchmark benchmark;
    benchmark.record(createEntryWith(Verdict{Success{}}), createQueryCase(ExpectedRows{}), createOneTiming(std::chrono::milliseconds{250}));
    const auto rows = benchmark.buildRows();
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(rows.front().queryName.value(), "errors/Measured:1");
    EXPECT_DOUBLE_EQ(rows.front().time, 0.25);
}

TEST_F(BenchmarkTest, DoesNotRecordAFailedQuery)
{
    Benchmark benchmark;
    benchmark.record(
        createEntryWith(Verdict{std::unexpected{Mismatch{"boom"}}}),
        createQueryCase(ExpectedRows{}),
        createOneTiming(std::chrono::milliseconds{250}));
    EXPECT_TRUE(benchmark.buildRows().empty());
}

TEST_F(BenchmarkTest, DoesNotRecordAQueryThatExpectsAnError)
{
    Benchmark benchmark;
    benchmark.record(
        createEntryWith(Verdict{Success{}}),
        createQueryCase(ExpectedError{.code = ErrorCode::InvalidQuerySyntax, .message = std::nullopt}),
        createOneTiming(std::chrono::milliseconds{250}));
    EXPECT_TRUE(benchmark.buildRows().empty());
}

TEST_F(BenchmarkTest, DoesNotRecordADifferentialBlockOrAnExplain)
{
    Benchmark benchmark;
    const RewrittenTestCase block{
        .action = RewrittenDifferential{
            .firstSql = "",
            .firstId = SystestQueryId{1},
            .firstResultFile = "first.csv",
            .secondSql = "",
            .secondId = SystestQueryId{2},
            .secondResultFile = "second.csv"}};
    const RewrittenTestCase explain{.action = RewrittenExplain{.sql = "", .id = SystestQueryId{3}, .expected = ExpectedPlan{.lines = {}}}};
    benchmark.record(createEntryWith(Verdict{Success{}}), block, createOneTiming(std::chrono::milliseconds{250}));
    benchmark.record(createEntryWith(Verdict{Success{}}), explain, createOneTiming(std::chrono::milliseconds{250}));
    EXPECT_TRUE(benchmark.buildRows().empty());
}

TEST_F(BenchmarkTest, DoesNotRecordACaseWithoutTimings)
{
    Benchmark benchmark;
    benchmark.record(createEntryWith(Verdict{Success{}}), createQueryCase(ExpectedRows{}), {});
    EXPECT_TRUE(benchmark.buildRows().empty());
}

}
