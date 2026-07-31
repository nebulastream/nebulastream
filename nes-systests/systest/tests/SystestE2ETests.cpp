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
#include <chrono>
#include <cstddef>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <SystestConfiguration.hpp>
#include <SystestExecutor.hpp>
#include <WorkerConfig.hpp>

namespace
{
size_t countFailedTests(const std::string_view failedTestString, const std::string_view systestE2EExtension)
{
    return std::ranges::count_if(
        std::views::iota(0UZ, failedTestString.length() - systestE2EExtension.length() + 1),
        [&](const size_t idx) { return failedTestString.substr(idx, systestE2EExtension.length()) == systestE2EExtension; });
};
}

namespace NES::Systest
{

struct E2ETestParameters
{
    std::string directory;
    std::string testFile;
};

/// Tests if SLT Parser rejects invalid .test files correctly
class SystestE2ETest : public Testing::BaseUnitTest, public testing::WithParamInterface<E2ETestParameters>
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestE2ETest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestE2ETest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestE2ETest test class."); }

    static constexpr std::string_view EXTENSION = ".dummy";
    static constexpr size_t DEFAULT_WORKER_CAPACITY = 1000;
};

/// Given a file with some correct and some incorrect queries, make sure that only the incorrect queries fail
TEST_F(SystestE2ETest, CheckThatOnlyWrongQueriesFailInFileWithManyQueries)
{
    SystestConfiguration config{};
    config.testsDiscoverDir.setValue(SYSTEST_DATA_DIR);
    const auto testFileName = fmt::format("MultipleCorrectAndIncorrect{}", EXTENSION);
    config.directlySpecifiedTestFiles.setValue(fmt::format("{}/errors/{}", SYSTEST_DATA_DIR, testFileName));
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/MultipleCorrectAndIncorrect", PATH_TO_BINARY_DIR));
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = Host("localhost:8080"),
            .dataAddress = "localhost:9090",
            .maxOperators = Capacity(CapacityKind::Limited{DEFAULT_WORKER_CAPACITY}),
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {Host("localhost:8080")},
        .allowSinkPlacement = {Host("localhost:8080")}};

    ::NES::SystestExecutor executor(config);
    const auto systestResult = executor.executeSystests();
    ASSERT_TRUE(systestResult.returnType == SystestExecutorResult::ReturnType::FAILED) << " Return type not as expected.";
    ASSERT_FALSE(systestResult.outputMessage.contains(fmt::format("{}:1", testFileName))) << "Correct query found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(fmt::format("{}:2", testFileName))) << "Query not found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(fmt::format("{}:3", testFileName))) << "Query not found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(fmt::format("{}:4", testFileName))) << "Query not found in failed queries.";
    ASSERT_FALSE(systestResult.outputMessage.contains(fmt::format("{}:5", testFileName))) << "Correct query found in failed queries.";
    ASSERT_FALSE(systestResult.outputMessage.contains(fmt::format("{}:6", testFileName))) << "Correct query found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(fmt::format("{}:7", testFileName))) << "Query not found in failed queries.";
    ASSERT_FALSE(systestResult.outputMessage.contains(fmt::format("{}:8", testFileName))) << "Correct query found in failed queries.";
    ASSERT_EQ(countFailedTests(systestResult.outputMessage, SystestE2ETest::EXTENSION), 4) << "Number of failed queries is unexpected.";
}

/// Each test file contains one correct and one similar, but incorrect query. We check that the correct query, which is always the
/// first query, passes and the second query, which is always the incorrect query, fails.
TEST_F(SystestE2ETest, DirectQueryNumberSelectionRunsOnlyTheSelectedCase)
{
    const auto root = std::filesystem::temp_directory_path()
        / fmt::format("systest-direct-selection-{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(root);
    const auto testFile = root / "selection.dummy";
    std::ofstream(testFile) << R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1

SEQUENTIAL_EXECUTION

SELECT missing FROM input INTO File();
----
1

SELECT value FROM input INTO File();
----
1
)";

    SystestConfiguration config{};
    config.testsDiscoverDir.setValue(root.string());
    config.directlySpecifiedTestFiles.setValue(testFile.string());
    config.testQueryNumberRanges = {{.first = SystestQueryId{2}, .last = SystestQueryId{2}}};
    config.workingDir.setValue((root / "working").string());
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = Host("localhost:8080"),
            .dataAddress = "localhost:9090",
            .maxOperators = Capacity(CapacityKind::Limited{DEFAULT_WORKER_CAPACITY}),
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {Host("localhost:8080")},
        .allowSinkPlacement = {Host("localhost:8080")}};

    const auto result = ::NES::SystestExecutor(config).executeSystests();

    EXPECT_EQ(result.returnType, SystestExecutorResult::ReturnType::SUCCESS) << result.outputMessage;
    EXPECT_FALSE(std::filesystem::exists(root / "working/results/selection%2Edummy/query-1-variant-0-primary.csv"));
    EXPECT_TRUE(std::filesystem::exists(root / "working/results/selection%2Edummy/query-2-variant-0-primary.csv"));
    std::error_code error;
    std::filesystem::remove_all(root, error);
}

TEST_F(SystestE2ETest, RemoteConfigurationOverrideFailureSurvivesExecutorOutput)
{
    const auto root = std::filesystem::temp_directory_path()
        / fmt::format("systest-remote-capability-{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(root);
    const auto testFile = root / "override.dummy";
    std::ofstream(testFile) << R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1

CONFIGURATION worker.query_engine.number_of_worker_threads: 1
SELECT value FROM input INTO File();
----
1
)";

    SystestConfiguration config{};
    config.testsDiscoverDir.setValue(root.string());
    config.directlySpecifiedTestFiles.setValue(testFile.string());
    config.workingDir.setValue((root / "working").string());
    config.remoteWorker.setValue(true);
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = Host("unreachable:8080"),
            .dataAddress = "unreachable:9090",
            .maxOperators = Capacity(CapacityKind::Limited{DEFAULT_WORKER_CAPACITY}),
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {Host("unreachable:8080")},
        .allowSinkPlacement = {Host("unreachable:8080")}};

    const auto result = ::NES::SystestExecutor(config).executeSystests();

    EXPECT_EQ(result.returnType, SystestExecutorResult::ReturnType::FAILED);
    EXPECT_TRUE(result.outputMessage.contains("override.dummy")) << result.outputMessage;
    EXPECT_TRUE(result.outputMessage.contains("environment")) << result.outputMessage;
    EXPECT_TRUE(result.outputMessage.contains("supportsConfigurationOverrides")) << result.outputMessage;
    EXPECT_FALSE(result.outputMessage.contains("All queries passed"));
    std::error_code error;
    std::filesystem::remove_all(root, error);
}

TEST_F(SystestE2ETest, ExplainOnlyBenchmarkFailsWithoutClaimingSuccess)
{
    const auto root = std::filesystem::temp_directory_path()
        / fmt::format("systest-benchmark-explain-{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(root);
    const auto testFile = root / "explain.dummy";
    std::ofstream(testFile) << R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1

EXPLAIN (LOGICAL) FORMAT TEXT SELECT value FROM input INTO File();
----
<REGEX>SOURCE\(INPUT\)</REGEX>
==END==
)";

    SystestConfiguration config{};
    config.testsDiscoverDir.setValue(root.string());
    config.directlySpecifiedTestFiles.setValue(testFile.string());
    config.workingDir.setValue((root / "working").string());
    config.benchmark.setValue(true);
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = Host("localhost:8080"),
            .dataAddress = "localhost:9090",
            .maxOperators = Capacity(CapacityKind::Limited{DEFAULT_WORKER_CAPACITY}),
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {Host("localhost:8080")},
        .allowSinkPlacement = {Host("localhost:8080")}};

    const auto result = ::NES::SystestExecutor(config).executeSystests();

    EXPECT_EQ(result.returnType, SystestExecutorResult::ReturnType::FAILED);
    EXPECT_EQ(result.outputMessage, "No benchmarkable queries were run.");
    EXPECT_FALSE(result.outputMessage.contains("All queries passed"));
    std::ifstream benchmark(root / "working/BenchmarkResults.json");
    ASSERT_TRUE(benchmark);
    const auto contents = std::string{std::istreambuf_iterator<char>{benchmark}, std::istreambuf_iterator<char>{}};
    EXPECT_EQ(contents, "[]");
    std::error_code error;
    std::filesystem::remove_all(root, error);
}

TEST_F(SystestE2ETest, DifferentialAndErrorOnlyBenchmarkFailsWithoutMeasurements)
{
    const auto root = std::filesystem::temp_directory_path()
        / fmt::format("systest-benchmark-ineligible-{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(root);
    const auto testFile = root / "ineligible.dummy";
    std::ofstream(testFile) << R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1

SELECT value FROM input INTO File();
====
SELECT value + UINT64(0) AS value FROM input INTO File();

SELECT missing FROM input INTO File();
----
ERROR CannotInferSchema
)";

    SystestConfiguration config{};
    config.testsDiscoverDir.setValue(root.string());
    config.directlySpecifiedTestFiles.setValue(testFile.string());
    config.workingDir.setValue((root / "working").string());
    config.benchmark.setValue(true);
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = Host("localhost:8080"),
            .dataAddress = "localhost:9090",
            .maxOperators = Capacity(CapacityKind::Limited{DEFAULT_WORKER_CAPACITY}),
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {Host("localhost:8080")},
        .allowSinkPlacement = {Host("localhost:8080")}};

    const auto result = ::NES::SystestExecutor(config).executeSystests();

    EXPECT_EQ(result.returnType, SystestExecutorResult::ReturnType::FAILED);
    EXPECT_EQ(result.outputMessage, "No benchmarkable queries were run.");
    std::ifstream benchmark(root / "working/BenchmarkResults.json");
    ASSERT_TRUE(benchmark);
    const auto contents = std::string{std::istreambuf_iterator<char>{benchmark}, std::istreambuf_iterator<char>{}};
    EXPECT_EQ(contents, "[]");
    std::error_code error;
    std::filesystem::remove_all(root, error);
}

TEST_F(SystestE2ETest, MixedBenchmarkRunsEligibleSuccessorAfterIneligiblePredecessor)
{
    const auto root = std::filesystem::temp_directory_path()
        / fmt::format("systest-benchmark-mixed-{}", std::chrono::steady_clock::now().time_since_epoch().count());
    std::filesystem::create_directories(root);
    const auto testFile = root / "mixed.dummy";
    std::ofstream(testFile) << R"(CREATE LOGICAL SOURCE input(value UINT64 NOT NULL);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH INLINE
1

SEQUENTIAL_EXECUTION
EXPLAIN (LOGICAL) FORMAT TEXT SELECT value FROM input INTO File();
----
<REGEX>SOURCE\(INPUT\)</REGEX>
==END==

SELECT value FROM input INTO File();
----
1
)";

    SystestConfiguration config{};
    config.testsDiscoverDir.setValue(root.string());
    config.directlySpecifiedTestFiles.setValue(testFile.string());
    config.workingDir.setValue((root / "working").string());
    config.benchmark.setValue(true);
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = Host("localhost:8080"),
            .dataAddress = "localhost:9090",
            .maxOperators = Capacity(CapacityKind::Limited{DEFAULT_WORKER_CAPACITY}),
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {Host("localhost:8080")},
        .allowSinkPlacement = {Host("localhost:8080")}};

    const auto result = ::NES::SystestExecutor(config).executeSystests();

    EXPECT_EQ(result.returnType, SystestExecutorResult::ReturnType::SUCCESS) << result.outputMessage;
    EXPECT_TRUE(std::filesystem::is_regular_file(root / "working/results/mixed%2Edummy/query-2-variant-0-primary.csv"));
    std::ifstream benchmark(root / "working/BenchmarkResults.json");
    ASSERT_TRUE(benchmark);
    const auto contents = std::string{std::istreambuf_iterator<char>{benchmark}, std::istreambuf_iterator<char>{}};
    const auto queryName = contents.find("\"query name\"");
    EXPECT_NE(queryName, std::string::npos) << contents;
    EXPECT_EQ(queryName, contents.rfind("\"query name\"")) << contents;
    EXPECT_NE(contents.find("mixed.dummy:2:variant=0"), std::string::npos) << contents;
    EXPECT_EQ(contents.find("mixed.dummy:1:variant=0"), std::string::npos) << contents;
    std::error_code error;
    std::filesystem::remove_all(root, error);
}

TEST_P(SystestE2ETest, correctAndIncorrectSchemaTestFile)
{
    const auto& [directory, testFile] = GetParam();
    const auto testFileName = testFile + std::string(".dummy");
    SystestConfiguration config{};
    config.testsDiscoverDir.setValue(SYSTEST_DATA_DIR);
    config.directlySpecifiedTestFiles.setValue(fmt::format("{}/errors/{}/{}", SYSTEST_DATA_DIR, directory, testFileName));
    config.testFileExtension.setValue(std::string(EXTENSION));
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/{}", PATH_TO_BINARY_DIR, testFile));
    config.clusterConfig = SystestClusterConfiguration{
        .workers = {WorkerConfig{
            .host = Host("localhost:8080"),
            .dataAddress = "localhost:9090",
            .maxOperators = Capacity(CapacityKind::Limited{DEFAULT_WORKER_CAPACITY}),
            .downstream = {},
            .config = {}}},
        .allowSourcePlacement = {Host("localhost:8080")},
        .allowSinkPlacement = {Host("localhost:8080")}};

    ::NES::SystestExecutor executor(config);
    const auto systestResult = executor.executeSystests();
    ASSERT_TRUE(systestResult.returnType == SystestExecutorResult::ReturnType::FAILED) << " Return type not as expected.";
    ASSERT_EQ(countFailedTests(systestResult.outputMessage, SystestE2ETest::EXTENSION), 1) << "Too many failed queries.";
    ASSERT_FALSE(systestResult.outputMessage.contains(testFileName + std::string(":1"))) << "Correct query found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(testFileName + std::string(":2"))) << "Incorrect query not found in failed queries.";
}

INSTANTIATE_TEST_CASE_P(
    QueryTests,
    SystestE2ETest,
    testing::Values(
        E2ETestParameters{"schema", "FieldNameDifference"},
        E2ETestParameters{"schema", "TypeDifference"},
        E2ETestParameters{"schema", "ResultsEmptyButSchemasDifferent"},
        E2ETestParameters{"result", "SingleValueIsDifferent"},
        E2ETestParameters{"result", "LessResultsThanExpected"},
        E2ETestParameters{"result", "MoreResultsThanExpected"},
        E2ETestParameters{"result", "ResultEmptyButExpectedIsNot"},
        E2ETestParameters{"result", "ExpectedEmptyButResultIsNot"},
        E2ETestParameters{"result", "ResultIsSubsetOfExpected"},
        E2ETestParameters{"result", "ExpectedIsColumnSubsetOfResult"},
        E2ETestParameters{"result", "ExpectedIsSubsetOfResult"}));
}
