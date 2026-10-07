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
#include <cstddef>
#include <fstream>
#include <iterator>
#include <ranges>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Config/Config.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <Executor.hpp>
#include <TemporaryDirectory.hpp>
#include <WorkerConfig.hpp>

namespace
{
constexpr std::string_view FailMarker = "  FAIL  ";

size_t countFailedCases(const std::string_view report)
{
    return static_cast<size_t>(std::ranges::distance(report | std::views::split(FailMarker))) - 1;
}

/// The start of a failed case's report line, such as `  FAIL  errors/Unparseable:3: ` for query 3.
std::string formatFailLine(const std::string_view directory, const std::string_view testFile, const int queryNumber)
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

/// Runs the executor end to end on the files under `testdata/errors`.
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

    /// A working directory per file, so runs do not share result files.
    static SystestConfiguration createConfigFor(const std::string_view testFile)
    {
        SystestConfiguration config{};
        config.testDiscoverRoot = SYSTEST_DATA_DIR;
        config.directlySpecifiedTestFiles.setValue(fmt::format("{}/errors/{}{}", SYSTEST_DATA_DIR, testFile, EXTENSION));
        config.workingDir.setValue(fmt::format("{}/nes-systests/systest/{}", PATH_TO_BINARY_DIR, testFile));
        config.clusterConfig = SystestClusterConfiguration{
            .workers = {WorkerConfig{
                .host = Host{"localhost:8080"},
                .dataAddress = "localhost:9090",
                .maxOperators = Capacity{CapacityKind::Limited{DEFAULT_WORKER_CAPACITY}},
                .downstream = {},
                .config = {}}},
            .allowSourcePlacement = {Host{"localhost:8080"}},
            .allowSinkPlacement = {Host{"localhost:8080"}}};
        return config;
    }
};

TEST_F(SystestE2ETest, CheckThatOnlyWrongQueriesFailInFileWithManyQueries)
{
    constexpr std::string_view testFile = "MultipleCorrectAndIncorrect";
    const auto result = Executor{createConfigFor(testFile)}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr) << " Return type not as expected.";
    ASSERT_FALSE(failed->report.contains(formatFailLine("errors", testFile, 1))) << "Correct query found in failed queries.";
    ASSERT_TRUE(failed->report.contains(formatFailLine("errors", testFile, 2))) << "Query not found in failed queries.";
    ASSERT_TRUE(failed->report.contains(formatFailLine("errors", testFile, 3))) << "Query not found in failed queries.";
    ASSERT_TRUE(failed->report.contains(formatFailLine("errors", testFile, 4))) << "Query not found in failed queries.";
    ASSERT_FALSE(failed->report.contains(formatFailLine("errors", testFile, 5))) << "Correct query found in failed queries.";
    ASSERT_FALSE(failed->report.contains(formatFailLine("errors", testFile, 6))) << "Correct query found in failed queries.";
    ASSERT_TRUE(failed->report.contains(formatFailLine("errors", testFile, 7))) << "Query not found in failed queries.";
    ASSERT_FALSE(failed->report.contains(formatFailLine("errors", testFile, 8))) << "Correct query found in failed queries.";
    ASSERT_EQ(countFailedCases(failed->report), 4) << "Number of failed queries is unexpected.";
}

TEST_F(SystestE2ETest, AFileThatDoesNotParseFailsAsAWhole)
{
    const auto result = Executor{createConfigFor("Unparseable")}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("0 queries passed, 1 failed\n")) << failed->report;
    EXPECT_TRUE(failed->report.contains("  FAIL  errors/Unparseable: could not prepare: ")) << failed->report;
}

TEST_F(SystestE2ETest, ARejectedSetupSkipsEveryTestCaseOfTheFile)
{
    const auto result = Executor{createConfigFor("SetupRejected")}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("0 queries passed, 1 failed, 2 skipped\n")) << failed->report;
    EXPECT_TRUE(failed->report.contains("  FAIL  errors/SetupRejected: could not run: ")) << failed->report;
    EXPECT_TRUE(failed->report.contains("  SKIP  errors/SetupRejected: 2 test cases: the file's setup failed\n")) << failed->report;
}

TEST_F(SystestE2ETest, ASelectionThatMatchesNothingFails)
{
    auto config = createConfigFor("NothingSelected");
    config.testQueryNumbers.add(7);
    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("no query ran")) << failed->report;
}

TEST_F(SystestE2ETest, ASelectionRunsOnlyTheSelectedQueriesAndCanSucceed)
{
    auto config = createConfigFor("MultipleCorrectAndIncorrect");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/MultipleCorrectAndIncorrectSelected", PATH_TO_BINARY_DIR));
    config.testQueryNumbers.add(1);
    config.testQueryNumbers.add(5);
    const auto result = Executor{config}.execute();
    const auto* succeeded = std::get_if<RunSucceeded>(&result);
    ASSERT_NE(succeeded, nullptr) << std::get<RunFailed>(result).report;
    EXPECT_TRUE(succeeded->report.starts_with("2 queries passed, 0 failed\n")) << succeeded->report;
}

TEST_F(SystestE2ETest, ASelectionWithAnAbsentNumberStillRunsTheRest)
{
    auto config = createConfigFor("NothingSelected");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/NothingSelectedPartial", PATH_TO_BINARY_DIR));
    config.testQueryNumbers.add(1);
    config.testQueryNumbers.add(7);
    const auto result = Executor{config}.execute();
    const auto* succeeded = std::get_if<RunSucceeded>(&result);
    ASSERT_NE(succeeded, nullptr) << std::get<RunFailed>(result).report;
    EXPECT_TRUE(succeeded->report.starts_with("1 queries passed, 0 failed\n")) << succeeded->report;
}

TEST_F(SystestE2ETest, AMeasuringRunSurvivesADifferentialBlock)
{
    auto config = createConfigFor("Measured");
    config.benchmark = true;
    const auto result = Executor{config}.execute();
    const auto* succeeded = std::get_if<RunSucceeded>(&result);
    ASSERT_NE(succeeded, nullptr) << std::get<RunFailed>(result).report;
    EXPECT_TRUE(succeeded->report.contains("queries measured, written to")) << succeeded->report;
}

TEST_F(SystestE2ETest, ADifferentialBlockWhoseHalvesDisagreeFails)
{
    const auto result = Executor{createConfigFor("DifferentialDisagrees")}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(countFailedCases(failed->report), 1) << failed->report;
    EXPECT_TRUE(failed->report.contains(formatFailLine("errors", "DifferentialDisagrees", 1))) << failed->report;
}

TEST_F(SystestE2ETest, ADifferentialBlockWhoseHalfDoesNotBindFailsAlone)
{
    const auto result = Executor{createConfigFor("DifferentialHalfUnbound")}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("1 queries passed, 1 failed\n")) << failed->report;
    EXPECT_EQ(countFailedCases(failed->report), 1) << failed->report;
    EXPECT_TRUE(failed->report.contains(formatFailLine("errors", "DifferentialHalfUnbound", 1))) << failed->report;
}

/// Without a round or time limit, a failed round is a repeating run's only exit.
TEST_F(SystestE2ETest, ARepeatingRunStopsAtTheFirstFailedRound)
{
    auto config = createConfigFor("MultipleCorrectAndIncorrect");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/MultipleCorrectAndIncorrectEndless", PATH_TO_BINARY_DIR));
    config.endlessMode = true;
    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(countFailedCases(failed->report), 4) << failed->report;
}

TEST_F(SystestE2ETest, ARepeatingRunThatKeepsPassingEndsAfterItsRounds)
{
    auto config = createConfigFor("NothingSelected");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/NothingSelectedRounds", PATH_TO_BINARY_DIR));
    config.endlessMode = true;
    config.endlessRounds = 2;
    const auto result = Executor{config}.execute();
    const auto* succeeded = std::get_if<RunSucceeded>(&result);
    ASSERT_NE(succeeded, nullptr) << std::get<RunFailed>(result).report;
    EXPECT_TRUE(succeeded->report.starts_with("1 queries passed, 0 failed\n")) << succeeded->report;
}

TEST_F(SystestE2ETest, AMeasuringRunOverSeveralRoundsWritesOneReport)
{
    auto config = createConfigFor("Measured");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/MeasuredRounds", PATH_TO_BINARY_DIR));
    config.benchmark = true;
    config.benchmarkRounds = 2;
    const auto result = Executor{config}.execute();
    const auto* succeeded = std::get_if<RunSucceeded>(&result);
    ASSERT_NE(succeeded, nullptr) << std::get<RunFailed>(result).report;
    EXPECT_TRUE(succeeded->report.contains("1 queries measured, written to")) << succeeded->report;
}

TEST_F(SystestE2ETest, ARepeatingRunWithNothingSelectedFailsInsteadOfLooping)
{
    auto config = createConfigFor("NothingSelected");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/NothingSelectedEndless", PATH_TO_BINARY_DIR));
    config.testQueryNumbers.add(7);
    config.endlessMode = true;
    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("no query ran")) << failed->report;
}

TEST_F(SystestE2ETest, ARepeatingRunWithABrokenFileFailsInsteadOfLooping)
{
    auto config = createConfigFor("Unparseable");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/UnparseableEndless", PATH_TO_BINARY_DIR));
    config.endlessMode = true;
    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("0 queries passed, 1 failed\n")) << failed->report;
    EXPECT_TRUE(failed->report.contains("  FAIL  errors/Unparseable: could not prepare: ")) << failed->report;
}

TEST_F(SystestE2ETest, ARepeatingRunWithARejectedSetupFailsInsteadOfLooping)
{
    auto config = createConfigFor("SetupRejected");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/SetupRejectedEndless", PATH_TO_BINARY_DIR));
    config.endlessMode = true;
    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("0 queries passed, 1 failed, 2 skipped\n")) << failed->report;
}

TEST_F(SystestE2ETest, AQueryUnderAnOverrideRunsInItsOwnGroupAndReportsTheOverride)
{
    const auto result = Executor{createConfigFor("OverridePartition")}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("1 queries passed, 1 failed\n")) << failed->report;
    EXPECT_TRUE(failed->report.contains("  FAIL  errors/OverridePartition:2 [worker.default_query_execution.operator_buffer_size=4096]: "))
        << failed->report;
}

/// The runner clamps a zero window to one.
TEST_F(SystestE2ETest, AConcurrencyOfZeroStillRunsTheQueries)
{
    auto config = createConfigFor("NothingSelected");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/ZeroConcurrency", PATH_TO_BINARY_DIR));
    config.numberConcurrentQueries = 0;
    const auto result = Executor{config}.execute();
    const auto* succeeded = std::get_if<RunSucceeded>(&result);
    ASSERT_NE(succeeded, nullptr) << std::get<RunFailed>(result).report;
    EXPECT_TRUE(succeeded->report.starts_with("1 queries passed, 0 failed\n")) << succeeded->report;
}

TEST_F(SystestE2ETest, ARemoteRunSkipsAFileAskingForSettingsAndStillSucceeds)
{
    auto config = createConfigFor("AllSkipped");
    config.remoteWorker = true;
    const auto result = Executor{config}.execute();
    const auto* succeeded = std::get_if<RunSucceeded>(&result);
    ASSERT_NE(succeeded, nullptr) << std::get<RunFailed>(result).report;
    EXPECT_TRUE(succeeded->report.starts_with("0 queries passed, 0 failed, 1 skipped\n")) << succeeded->report;
    EXPECT_TRUE(
        succeeded->report.contains("  SKIP  errors/AllSkipped [worker.default_query_execution.operator_buffer_size=4096]: 1 test cases: "))
        << succeeded->report;
}

/// With every file skipped, no round has anything to submit.
TEST_F(SystestE2ETest, ARepeatingRemoteRunWhoseFilesAllSkipEndsInsteadOfLooping)
{
    auto config = createConfigFor("AllSkipped");
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/AllSkippedEndless", PATH_TO_BINARY_DIR));
    config.remoteWorker = true;
    config.endlessMode = true;
    const auto result = Executor{config}.execute();
    const auto* succeeded = std::get_if<RunSucceeded>(&result);
    ASSERT_NE(succeeded, nullptr) << std::get<RunFailed>(result).report;
    EXPECT_TRUE(succeeded->report.starts_with("0 queries passed, 0 failed, 1 skipped\n")) << succeeded->report;
}

TEST_F(SystestE2ETest, AShuffledRunKeepsEachExpectationWithItsQuery)
{
    constexpr std::string_view testFile = "MultipleCorrectAndIncorrect";
    auto config = createConfigFor(testFile);
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/MultipleCorrectAndIncorrectShuffled", PATH_TO_BINARY_DIR));
    config.randomQueryOrder = true;
    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_EQ(countFailedCases(failed->report), 4) << failed->report;
    EXPECT_TRUE(failed->report.contains(formatFailLine("errors", testFile, 2))) << failed->report;
    EXPECT_TRUE(failed->report.contains(formatFailLine("errors", testFile, 3))) << failed->report;
    EXPECT_TRUE(failed->report.contains(formatFailLine("errors", testFile, 4))) << failed->report;
    EXPECT_TRUE(failed->report.contains(formatFailLine("errors", testFile, 7))) << failed->report;
}

/// All files declare the same names into the shared catalogs.
TEST_F(SystestE2ETest, ABrokenFileFailsAloneAndTheOtherFilesStillRun)
{
    const Testing::TemporaryDirectory tmp;
    const auto writeFile = [&](const std::string_view name, const std::string_view content)
    {
        std::ofstream out{tmp.get() / name};
        out << content;
    };
    constexpr std::string_view passing = "CREATE LOGICAL SOURCE s(id UINT64 NOT NULL);\n"
                                         "CREATE PHYSICAL SOURCE FOR s TYPE File;\n"
                                         "ATTACH INLINE\n"
                                         "1\n"
                                         "\n"
                                         "CREATE SINK sinkA(id UINT64 NOT NULL) TYPE File;\n"
                                         "\n"
                                         "SELECT id FROM s INTO sinkA;\n"
                                         "----\n"
                                         "1\n";
    writeFile("First.dummy", passing);
    writeFile("Second.dummy", passing);
    writeFile(
        "Broken.dummy",
        "CREATE LOGICAL SOURCE s(id UINT64 NOT NULL);\n"
        "THIS IS NOT A STATEMENT AT ALL ;;;\n"
        "SELECT id FROM s INTO sink;\n"
        "----\n"
        "1\n");

    auto config = createConfigFor("MultipleFiles");
    config.directlySpecifiedTestFiles.setValue("");
    config.testDiscoverRoot.setValue(tmp.get().string());
    config.testFileExtension.setValue(std::string{EXTENSION});
    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    EXPECT_TRUE(failed->report.starts_with("2 queries passed, 1 failed\n")) << failed->report;
    EXPECT_TRUE(failed->report.contains("Broken: could not prepare: ")) << failed->report;
    EXPECT_EQ(countFailedCases(failed->report), 1) << failed->report;
}

/// Same report regardless of which concurrent query finishes first.
TEST_F(SystestE2ETest, TheReportListsFailuresInFileOrder)
{
    constexpr std::string_view testFile = "MultipleCorrectAndIncorrect";
    auto config = createConfigFor(testFile);
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/MultipleCorrectAndIncorrectOrdered", PATH_TO_BINARY_DIR));
    config.numberConcurrentQueries.setValue(8);
    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    std::vector<size_t> positions;
    for (const auto number : {2, 3, 4, 7})
    {
        const auto position = failed->report.find(formatFailLine("errors", testFile, number));
        ASSERT_NE(position, std::string::npos) << failed->report;
        positions.push_back(position);
    }
    EXPECT_TRUE(std::ranges::is_sorted(positions)) << failed->report;
}

/// Settings groups reorder execution.
TEST_F(SystestE2ETest, TheReportKeepsFileOrderAcrossSettingsGroups)
{
    const Testing::TemporaryDirectory tmp;
    const auto writeFile = [&](const std::string_view name, const std::string_view content)
    {
        std::ofstream out{tmp.get() / name};
        out << content;
    };
    constexpr std::string_view setup = "CREATE LOGICAL SOURCE s(id UINT64 NOT NULL);\n"
                                       "CREATE PHYSICAL SOURCE FOR s TYPE File;\n"
                                       "ATTACH INLINE\n"
                                       "1\n"
                                       "\n"
                                       "CREATE SINK sinkA(id UINT64 NOT NULL) TYPE File;\n"
                                       "\n";
    constexpr std::string_view settings = "Configuration worker.default_query_execution.operator_buffer_size: [4096]\n";
    constexpr std::string_view failing = "SELECT id FROM s INTO sinkA;\n"
                                         "----\n"
                                         "2\n"
                                         "\n";
    writeFile("First.dummy", fmt::format("{}{}{}{}", setup, settings, failing, failing));
    writeFile("Second.dummy", fmt::format("{}{}{}{}", setup, failing, settings, failing));

    auto config = createConfigFor("SettingsGroups");
    config.directlySpecifiedTestFiles.setValue("");
    config.testDiscoverRoot.setValue(tmp.get().string());
    config.testFileExtension.setValue(std::string{EXTENSION});
    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr);
    ASSERT_TRUE(failed->report.starts_with("0 queries passed, 4 failed\n")) << failed->report;

    const auto findPositionOf = [&](const std::string_view file, const int query)
    { return failed->report.find(fmt::format("{}{}:{}", FailMarker, file, query)); };
    const auto first1 = findPositionOf("First", 1);
    const auto first2 = findPositionOf("First", 2);
    const auto second1 = findPositionOf("Second", 1);
    const auto second2 = findPositionOf("Second", 2);
    for (const auto position : {first1, first2, second1, second2})
    {
        ASSERT_NE(position, std::string::npos) << failed->report;
    }
    EXPECT_LT(first1, first2) << failed->report;
    EXPECT_LT(second1, second2) << failed->report;
    EXPECT_TRUE(std::max(first1, first2) < std::min(second1, second2) or std::max(second1, second2) < std::min(first1, first2))
        << failed->report;
}

/// Each file has a correct first query and a wrong second one, and only the second should fail.
TEST_P(SystestE2ETest, correctAndIncorrectSchemaTestFile)
{
    const auto& [directory, testFile] = GetParam();
    auto config = createConfigFor(fmt::format("{}/{}", directory, testFile));
    config.testFileExtension.setValue(std::string{EXTENSION});
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/{}", PATH_TO_BINARY_DIR, testFile));

    const auto result = Executor{config}.execute();
    const auto* failed = std::get_if<RunFailed>(&result);
    ASSERT_NE(failed, nullptr) << " Return type not as expected.";
    ASSERT_EQ(countFailedCases(failed->report), 1) << "Too many failed queries.";
    const auto testDirectory = fmt::format("errors/{}", directory);
    ASSERT_FALSE(failed->report.contains(formatFailLine(testDirectory, testFile, 1))) << "Correct query found in failed queries.";
    ASSERT_TRUE(failed->report.contains(formatFailLine(testDirectory, testFile, 2))) << "Incorrect query not found in failed queries.";
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
