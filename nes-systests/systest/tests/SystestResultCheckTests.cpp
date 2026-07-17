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

#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestResultCheck.hpp>
#include <SystestState.hpp>

namespace
{
NES::Systest::RunningQuery makeExplainRunningQuery(std::vector<std::string> expectedResultLines, std::string actualExplainOutput)
{
    NES::Systest::SystestQuery query{
        .testName = "regex_explain",
        .queryIdInFile = NES::Systest::SystestQueryId(1),
        .testFilePath = SYSTEST_DATA_DIR "regex_explain.dummy",
        .workingDir = {},
        .queryDefinition = "EXPLAIN (OPTIMIZED, FORMAT TEXT) SELECT id FROM stream INTO sink;",
        .planInfoOrException = std::unexpected<NES::Exception>{NES::TestException("unused")},
        .expectedResultsOrExpectedError = std::move(expectedResultLines),
        .additionalSourceThreads = std::make_shared<std::vector<std::jthread>>(),
        .configurationOverride = NES::Systest::ConfigurationOverride{},
        .differentialQueryPlan = std::nullopt,
        .runAfter = std::nullopt,
        .actualExplainOutput = std::move(actualExplainOutput)};
    return NES::Systest::RunningQuery{
        .systestQuery = std::move(query),
        .queryId = NES::DistributedQueryId(NES::DistributedQueryId::INVALID),
        .differentialQueryPair = std::nullopt,
        .queryStatus = std::nullopt,
        .bytesProcessed = 0,
        .tuplesProcessed = 0,
        .passed = false,
        .exception = std::nullopt};
}
}

namespace NES::Systest
{
class SystestResultCheckTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestResultCheckTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestResultCheckTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestResultCheckTest test class."); }
};

TEST_F(SystestResultCheckTest, ExplainExactResultLinesKeepVanillaMatching)
{
    auto matchingQuery = makeExplainRunningQuery({"== Optimized Plan ==", "SINK(FILE)"}, "== Optimized Plan ==\nSINK(FILE)\n");
    EXPECT_EQ(checkExplainResult(matchingQuery), std::nullopt);

    auto mismatchingQuery = makeExplainRunningQuery({"== Optimized Plan ==", "SINK(FILE)"}, "== Optimized Plan ==\nSINK(OTHER)\n");
    const auto error = checkExplainResult(mismatchingQuery);
    ASSERT_TRUE(error.has_value());
    EXPECT_TRUE(error->contains("first difference at line 2"));
}

TEST_F(SystestResultCheckTest, ExplainRegexAssertionsFromDummyMatchActualOutput)
{
    SystestParser parser{};

    bool explainCallbackCalled = false;
    std::vector<std::string> expectedResultLines;

    parser.registerOnExplainQueryCallback([&](const std::string&, SystestQueryId) { explainCallbackCalled = true; });
    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, SystestQueryId)
                                          { expectedResultLines = std::move(resultTuples); });

    static constexpr std::string_view Filename = SYSTEST_DATA_DIR "regex_explain.dummy";
    ASSERT_TRUE(parser.loadFile(Filename));
    ASSERT_NO_THROW(parser.parse());

    ASSERT_TRUE(explainCallbackCalled);
    ASSERT_EQ(
        expectedResultLines,
        (std::vector<std::string>{
            "<REGEX>== Optimized Plan ==</REGEX>",
            "<REGEX>",
            R"(SINK\(SINK[0-9]+\))",
            R"(  SOURCE\(stream_[0-9]+\))",
            "</REGEX>",
            "<!REGEX>SELECTION</!REGEX>"}));

    auto runningQuery
        = makeExplainRunningQuery(std::move(expectedResultLines), "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n");
    EXPECT_EQ(checkExplainResult(runningQuery), std::nullopt);
}

TEST_F(SystestResultCheckTest, ExplainRegexAssertionsReportMissingPositiveMatch)
{
    auto runningQuery = makeExplainRunningQuery(
        {R"(<REGEX>SELECTION\(VALUE > 2\)</REGEX>)"}, "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n");

    const auto error = checkExplainResult(runningQuery);
    ASSERT_TRUE(error.has_value());
    EXPECT_TRUE(error->contains("expected pattern \"SELECTION\\(VALUE > 2\\)\" to match"));
}

TEST_F(SystestResultCheckTest, ExplainRegexAssertionsReportUnexpectedNegativeMatch)
{
    auto runningQuery = makeExplainRunningQuery(
        {"<!REGEX>SELECTION</!REGEX>"}, "== Optimized Plan ==\nSINK(SINK42)\n  SELECTION(VALUE > 2)\n    SOURCE(stream_17)\n");

    const auto error = checkExplainResult(runningQuery);
    ASSERT_TRUE(error.has_value());
    EXPECT_TRUE(error->contains("expected pattern \"SELECTION\" not to match"));
}

TEST_F(SystestResultCheckTest, ExplainRegexAssertionsRejectMixedMatchingModes)
{
    auto runningQuery = makeExplainRunningQuery({"<REGEX>SINK</REGEX>", "SOURCE(stream)"}, "SINK\nSOURCE(stream)");

    const auto error = checkExplainResult(runningQuery);
    ASSERT_TRUE(error.has_value());
    EXPECT_TRUE(error->contains("tagged and untagged expected output must not be mixed"));
}

TEST_F(SystestResultCheckTest, ExplainRegexAssertionsRejectMalformedTags)
{
    const std::vector<std::vector<std::string>> invalidExpectedResults{
        {"<REGEX>", "SINK"},
        {"<REGEX>", "SINK", "</!REGEX>"},
        {"<REGEX><!REGEX>SINK</!REGEX></REGEX>"},
        {"<REGEX></REGEX>"},
        {"<!REGEX>", "</!REGEX>"}};

    for (const auto& expected : invalidExpectedResults)
    {
        auto runningQuery = makeExplainRunningQuery(expected, "SINK");
        const auto error = checkExplainResult(runningQuery);
        ASSERT_TRUE(error.has_value());
        EXPECT_TRUE(error->contains("Invalid Explain Regex Assertion"));
    }
}
}
