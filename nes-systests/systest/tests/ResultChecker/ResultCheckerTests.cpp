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

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Discovery/TestFileReader.hpp>
#include <Identifiers/Identifier.hpp>
#include <Model/SystestQueryId.hpp>
#include <Parser/SystestParser.hpp>
#include <ResultChecker/Check.hpp>
#include <ResultChecker/DifferentialChecker.hpp>
#include <ResultChecker/ExplainChecker.hpp>
#include <ResultChecker/ResultChecker.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
ExplainCheck makeExplainCheck(std::vector<std::string> expected, std::string actualOutput)
{
    return ExplainCheck{.actualOutput = std::move(actualOutput), .expected = std::move(expected)};
}

Schema<UnqualifiedUnboundField, Ordered> schemaOf(const std::string& fieldName)
{
    return Schema<UnqualifiedUnboundField, Ordered>{
        std::vector{UnqualifiedUnboundField{Identifier::parse(fieldName), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};
}

std::filesystem::path writeResultFile(const std::string& name, const std::string& content)
{
    const auto path = std::filesystem::temp_directory_path() / name;
    std::ofstream file{path};
    file << content;
    return path;
}
}

class ResultCheckerTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("ResultCheckerTest.log", LogLevel::LOG_DEBUG); }
};

TEST_F(ResultCheckerTest, ChecksResultRowsAgainstTheDeclaredSchema)
{
    const auto resultFile = writeResultFile("resultchecker_rows.csv", "id:UINT64:NOT_NULLABLE\n1\n2\n3\n");

    EXPECT_TRUE((ResultCheck{.resultFile = resultFile, .expectedSchema = schemaOf("id"), .expected = {"1", "2", "3"}}.check().has_value()));

    const auto verdict = ResultCheck{.resultFile = resultFile, .expectedSchema = schemaOf("id"), .expected = {"1", "2", "4"}}.check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("Result Mismatch"));
}

/// The declared schema is what the test asked the sink to write, and the header is what the sink wrote.
/// A test whose rows happen to match still fails when the two disagree, because the query then answered about other fields.
TEST_F(ResultCheckerTest, ReportsASchemaTheSinkDidNotWrite)
{
    const auto resultFile = writeResultFile("resultchecker_schema.csv", "id:UINT64:NOT_NULLABLE\n1\n");

    const auto verdict = ResultCheck{.resultFile = resultFile, .expectedSchema = schemaOf("value"), .expected = {"1"}}.check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("Schema Mismatch"));
}

TEST_F(ResultCheckerTest, ReportsAMissingResultFile)
{
    const auto verdict = ResultCheck{.resultFile = "/does/not/exist.csv", .expectedSchema = schemaOf("id"), .expected = {"1"}}.check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("result file was not written"));
}

TEST_F(ResultCheckerTest, ComparesTheTwoResultFilesOfADifferentialBlock)
{
    const auto first = writeResultFile("resultchecker_differential_first.csv", "id:UINT64:NOT_NULLABLE\n1\n2\n");
    const auto agreeing = writeResultFile("resultchecker_differential_agreeing.csv", "id:UINT64:NOT_NULLABLE\n2\n1\n");
    const auto differing = writeResultFile("resultchecker_differential_differing.csv", "id:UINT64:NOT_NULLABLE\n1\n3\n");

    EXPECT_TRUE((DifferentialCheck{.resultFile = first, .differentialResultFile = agreeing}.check().has_value()));

    const auto verdict = DifferentialCheck{.resultFile = first, .differentialResultFile = differing}.check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("differential query execution"));
}

TEST_F(ResultCheckerTest, ExplainExactResultLinesKeepVanillaMatching)
{
    EXPECT_TRUE(makeExplainCheck({"== Optimized Plan ==", "SINK(FILE)"}, "== Optimized Plan ==\nSINK(FILE)\n").check().has_value());

    const auto verdict = makeExplainCheck({"== Optimized Plan ==", "SINK(FILE)"}, "== Optimized Plan ==\nSINK(OTHER)\n").check();
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
        makeExplainCheck(std::move(expectedResultLines), "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n").check().has_value());
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsReportMissingPositiveMatch)
{
    const auto verdict
        = makeExplainCheck({R"(<REGEX>SELECTION\(VALUE > 2\)</REGEX>)"}, "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n")
              .check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("expected pattern \"SELECTION\\(VALUE > 2\\)\" to match"));
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsReportUnexpectedNegativeMatch)
{
    const auto verdict
        = makeExplainCheck(
              {"<!REGEX>SELECTION</!REGEX>"}, "== Optimized Plan ==\nSINK(SINK42)\n  SELECTION(VALUE > 2)\n    SOURCE(stream_17)\n")
              .check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("expected pattern \"SELECTION\" not to match"));
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsRejectMixedMatchingModes)
{
    const auto verdict = makeExplainCheck({"<REGEX>SINK</REGEX>", "SOURCE(stream)"}, "SINK\nSOURCE(stream)").check();
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
        const auto verdict = makeExplainCheck(expected, "SINK").check();
        ASSERT_FALSE(verdict.has_value());
        EXPECT_TRUE(verdict.error().detail.contains("Invalid Explain Regex Assertion"));
    }
}

/// runCheck is the runner's only path from a check to a Verdict, so the dispatch over every alternative has its own test.
TEST_F(ResultCheckerTest, RunCheckDispatchesToEveryAlternative)
{
    const auto resultFile = writeResultFile("resultchecker_dispatch.csv", "id:UINT64:NOT_NULLABLE\n1\n");

    EXPECT_TRUE(runCheck(ResultCheck{.resultFile = resultFile, .expectedSchema = schemaOf("id"), .expected = {"1"}}).has_value());
    EXPECT_TRUE(runCheck(DifferentialCheck{.resultFile = resultFile, .differentialResultFile = resultFile}).has_value());
    EXPECT_TRUE(runCheck(makeExplainCheck({"SINK"}, "SINK\n")).has_value());
}
}
