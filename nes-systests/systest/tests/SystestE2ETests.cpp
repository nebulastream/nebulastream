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
#include <ranges>
#include <string>
#include <string_view>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>

#include <Util/ExecutionMode.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestConfiguration.hpp>
#include <SystestExecutor.hpp>

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
};

/// Given a file with some correct and some incorrect queries, make sure that only the incorrect queries fail
TEST_F(SystestE2ETest, CheckThatOnlyWrongQueriesFailInFileWithManyQueries)
{
    Configuration::SystestConfiguration config{};
#ifndef USE_MLIR
    auto snwConf = Configuration::SingleNodeWorkerConfiguration{};
    snwConf.workerConfiguration.queryOptimizer.executionMode = Nautilus::Configurations::ExecutionMode::INTERPRETER;
    config.singleNodeWorkerConfig = snwConf;
#endif
    config.testsDiscoverDir.setValue(SYSTEST_DATA_DIR);
    const auto testFileName = fmt::format("MultipleCorrectAndIncorrect{}", EXTENSION);
    config.directlySpecifiedTestFiles.setValue(fmt::format("{}/errors/{}", SYSTEST_DATA_DIR, testFileName));
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/MultipleCorrectAndIncorrect", PATH_TO_BINARY_DIR));

    const auto systestResult = NES::executeSystests(config);
    ASSERT_TRUE(systestResult.returnType == SystestExecutorResult::ReturnType::FAILED) << " Return type not as expected.";
    ASSERT_EQ(countFailedTests(systestResult.outputMessage, SystestE2ETest::EXTENSION), 4) << "Number of failed queries is unexpected.";
    ASSERT_FALSE(systestResult.outputMessage.contains(fmt::format("{}:1", testFileName))) << "Correct query found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(fmt::format("{}:2", testFileName))) << "Query not found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(fmt::format("{}:3", testFileName))) << "Query not found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(fmt::format("{}:4", testFileName))) << "Query not found in failed queries.";
    ASSERT_FALSE(systestResult.outputMessage.contains(fmt::format("{}:5", testFileName))) << "Correct query found in failed queries.";
    ASSERT_FALSE(systestResult.outputMessage.contains(fmt::format("{}:6", testFileName))) << "Correct query found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(fmt::format("{}:7", testFileName))) << "Query not found in failed queries.";
    ASSERT_FALSE(systestResult.outputMessage.contains(fmt::format("{}:8", testFileName))) << "Correct query found in failed queries.";
}


/// Each test file contains one correct and one similar, but incorrect query. We check that the correct query, which is always the
/// first query, passes and the second query, which is always the incorrect query, fails.
TEST_P(SystestE2ETest, correctAndIncorrectSchemaTestFile)
{
    const auto& [directory, testFile] = GetParam();
    const auto testFileName = testFile + std::string(".dummy");
    Configuration::SystestConfiguration config{};
#ifndef USE_MLIR
    auto snwConf = Configuration::SingleNodeWorkerConfiguration{};
    snwConf.workerConfiguration.queryOptimizer.executionMode = Nautilus::Configurations::ExecutionMode::INTERPRETER;
    config.singleNodeWorkerConfig = snwConf;
#endif
    config.testsDiscoverDir.setValue(SYSTEST_DATA_DIR);
    config.directlySpecifiedTestFiles.setValue(fmt::format("{}/errors/{}/{}", SYSTEST_DATA_DIR, directory, testFileName));
    config.testFileExtension.setValue(std::string(EXTENSION));
    config.workingDir.setValue(fmt::format("{}/nes-systests/systest/{}", PATH_TO_BINARY_DIR, testFile));

    const auto systestResult = NES::executeSystests(config);
    ASSERT_TRUE(systestResult.returnType == SystestExecutorResult::ReturnType::FAILED) << " Return type not as expected.";
    ASSERT_EQ(countFailedTests(systestResult.outputMessage, SystestE2ETest::EXTENSION), 1) << "Too many failed queries.";
    ASSERT_FALSE(systestResult.outputMessage.contains(testFileName + std::string(":1"))) << "Correct query found in failed queries.";
    ASSERT_TRUE(systestResult.outputMessage.contains(testFileName + std::string(":2"))) << "Incorrect query not found in failed queries.";
}

INSTANTIATE_TEST_CASE_P(
    QueryTests,
    SystestE2ETest,
    testing::Values(
        E2ETestParameters{"schema", "SourceNameDifference"},
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
