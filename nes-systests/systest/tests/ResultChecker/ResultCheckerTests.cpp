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

#include <filesystem>
#include <fstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include <Discovery/TestFileReader.hpp>
#include <Model/SystestQueryId.hpp>
#include <Parser/SystestParser.hpp>
#include <ResultChecker/ExplainChecker.hpp>
#include <ResultChecker/ResultChecker.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace
{
NES::Systest::ExplainCheck makeExplainCheck(std::vector<std::string> expected, std::string actualOutput)
{
    return NES::Systest::ExplainCheck{.actualOutput = std::move(actualOutput), .expected = std::move(expected)};
}
}

namespace NES
{
class ResultCheckerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ResultCheckerTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(ResultCheckerTest, ChecksResultRowsAgainstTheHeaderSchema)
{
    /// The sink writes the resolved output schema as the header line; the checker recovers it from there rather than being told.
    const std::filesystem::path resultFile = std::filesystem::temp_directory_path() / "resultchecker_rows.csv";
    {
        std::ofstream file{resultFile};
        file << "id:UINT64:NOT_NULLABLE\n1\n2\n3\n";
    }

    EXPECT_TRUE(Systest::checkResult({.resultFile = resultFile, .expected = {"1", "2", "3"}}).has_value());

    const auto verdict = Systest::checkResult({.resultFile = resultFile, .expected = {"1", "2", "4"}});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("Result Mismatch"));
}

/// A result line the sink wrote in a form the check cannot read is what the check exists to catch.
/// It reports a mismatch, which leaves the other queries of this file and the files after it to run.
TEST_F(ResultCheckerTest, ReportsAResultValueThatIsNoBoolean)
{
    const std::filesystem::path resultFile = std::filesystem::temp_directory_path() / "resultchecker_not_a_bool.csv";
    {
        std::ofstream file{resultFile};
        file << "flag:BOOLEAN:NOT_NULLABLE\n" << R"("FLAG":true})" << "\n";
    }

    const auto verdict = Systest::checkResult({.resultFile = resultFile, .expected = {"true"}});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("Result Mismatch"));
}

TEST_F(ResultCheckerTest, ReportsAMissingResultFile)
{
    const auto verdict = Systest::checkResult({.resultFile = "/does/not/exist.csv", .expected = {"1"}});
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("result file was not written"));
}

TEST_F(ResultCheckerTest, ExplainExactResultLinesKeepVanillaMatching)
{
    EXPECT_TRUE(
        Systest::checkExplain(makeExplainCheck({"== Optimized Plan ==", "SINK(FILE)"}, "== Optimized Plan ==\nSINK(FILE)\n")).has_value());

    const auto verdict
        = Systest::checkExplain(makeExplainCheck({"== Optimized Plan ==", "SINK(FILE)"}, "== Optimized Plan ==\nSINK(OTHER)\n"));
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("first difference at line 2"));
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsFromDummyMatchActualOutput)
{
    SystestParser parser{};

    bool explainCallbackCalled = false;
    std::vector<std::string> expectedResultLines;

    parser.registerOnExplainQueryCallback([&](const std::string&, SystestQueryId) { explainCallbackCalled = true; });
    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, SystestQueryId)
                                          { expectedResultLines = std::move(resultTuples); });

    static constexpr std::string_view Filename = SYSTEST_DATA_DIR "regex_explain.dummy";
    parser.loadString(readTestFile(Filename));
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

    EXPECT_TRUE(
        Systest::checkExplain(makeExplainCheck(std::move(expectedResultLines), "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n"))
            .has_value());
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsReportMissingPositiveMatch)
{
    const auto verdict = Systest::checkExplain(
        makeExplainCheck({R"(<REGEX>SELECTION\(VALUE > 2\)</REGEX>)"}, "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n"));
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("expected pattern \"SELECTION\\(VALUE > 2\\)\" to match"));
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsReportUnexpectedNegativeMatch)
{
    const auto verdict = Systest::checkExplain(makeExplainCheck(
        {"<!REGEX>SELECTION</!REGEX>"}, "== Optimized Plan ==\nSINK(SINK42)\n  SELECTION(VALUE > 2)\n    SOURCE(stream_17)\n"));
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("expected pattern \"SELECTION\" not to match"));
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsRejectMixedMatchingModes)
{
    const auto verdict = Systest::checkExplain(makeExplainCheck({"<REGEX>SINK</REGEX>", "SOURCE(stream)"}, "SINK\nSOURCE(stream)"));
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("tagged and untagged expected output must not be mixed"));
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsRejectMalformedTags)
{
    const std::vector<std::vector<std::string>> invalidExpectedResults{
        {"<REGEX>", "SINK"},
        {"<REGEX>", "SINK", "</!REGEX>"},
        {"<REGEX><!REGEX>SINK</!REGEX></REGEX>"},
        {"<REGEX></REGEX>"},
        {"<!REGEX>", "</!REGEX>"}};

    for (const auto& expected : invalidExpectedResults)
    {
        const auto verdict = Systest::checkExplain(makeExplainCheck(expected, "SINK"));
        ASSERT_FALSE(verdict.has_value());
        EXPECT_TRUE(verdict.error().detail.contains("Invalid Explain Regex Assertion"));
    }
}
}
