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

#include <cstddef>
#include <string>
#include <string_view>
#include <variant>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Config/Config.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <Executor.hpp>
#include <WorkerConfig.hpp>

namespace
{
/// The report lists one line per failed case, each starting with this marker.
constexpr std::string_view FailMarker = "  FAIL  ";

size_t countFailedCases(const std::string_view report)
{
    size_t count = 0;
    for (auto at = report.find(FailMarker); at != std::string_view::npos; at = report.find(FailMarker, at + FailMarker.size()))
    {
        ++count;
    }
    return count;
}

/// The start of a failed case's report line. The label is the test file relative to the discovery root, without its
/// extension, followed by the query number.
std::string failLine(const std::string_view directory, const std::string_view testFile, const int queryNumber)
{
    return fmt::format("{}{}/{}:{}: ", FailMarker, directory, testFile, queryNumber);
}
}

namespace NES
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
    config.testDiscoverRoot = SYSTEST_DATA_DIR;
    constexpr std::string_view testFile = "MultipleCorrectAndIncorrect";
    config.directlySpecifiedTestFiles.setValue(fmt::format("{}/errors/{}{}", SYSTEST_DATA_DIR, testFile, EXTENSION));
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

    const Executor executor{config};
    const auto result = executor.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr) << " Return type not as expected.";
    ASSERT_FALSE(failed->report.contains(failLine("errors", testFile, 1))) << "Correct query found in failed queries.";
    ASSERT_TRUE(failed->report.contains(failLine("errors", testFile, 2))) << "Query not found in failed queries.";
    ASSERT_TRUE(failed->report.contains(failLine("errors", testFile, 3))) << "Query not found in failed queries.";
    ASSERT_TRUE(failed->report.contains(failLine("errors", testFile, 4))) << "Query not found in failed queries.";
    ASSERT_FALSE(failed->report.contains(failLine("errors", testFile, 5))) << "Correct query found in failed queries.";
    ASSERT_FALSE(failed->report.contains(failLine("errors", testFile, 6))) << "Correct query found in failed queries.";
    ASSERT_TRUE(failed->report.contains(failLine("errors", testFile, 7))) << "Query not found in failed queries.";
    ASSERT_FALSE(failed->report.contains(failLine("errors", testFile, 8))) << "Correct query found in failed queries.";
    ASSERT_EQ(countFailedCases(failed->report), 4) << "Number of failed queries is unexpected.";
}

/// Each test file contains one correct and one similar, but incorrect query. We check that the correct query, which is always the
/// first query, passes and the second query, which is always the incorrect query, fails.
TEST_P(SystestE2ETest, correctAndIncorrectSchemaTestFile)
{
    const auto& [directory, testFile] = GetParam();
    const auto testFileName = testFile + std::string(".dummy");
    SystestConfiguration config{};
    config.testDiscoverRoot = SYSTEST_DATA_DIR;
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

    const Executor executor{config};
    const auto result = executor.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr) << " Return type not as expected.";
    ASSERT_EQ(countFailedCases(failed->report), 1) << "Too many failed queries.";
    const auto testDirectory = fmt::format("errors/{}", directory);
    ASSERT_FALSE(failed->report.contains(failLine(testDirectory, testFile, 1))) << "Correct query found in failed queries.";
    ASSERT_TRUE(failed->report.contains(failLine(testDirectory, testFile, 2))) << "Incorrect query not found in failed queries.";
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
