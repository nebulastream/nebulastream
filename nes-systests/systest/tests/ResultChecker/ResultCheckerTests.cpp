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
#include <optional>
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
#include <Identifiers/Identifiers.hpp>
#include <Parser/SystestParser.hpp>
#include <ResultChecker/Check.hpp>
#include <ResultChecker/DifferentialChecker.hpp>
#include <ResultChecker/ExplainChecker.hpp>
#include <ResultChecker/QueryResultChecker.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>

namespace NES
{
namespace
{
/// Picks the explain check the way the runner does: by whether the expected lines carry regex tags.
AnyCheck makeExplainCheck(std::vector<std::string> expected, std::string actualOutput)
{
    if (hasExplainRegexTags(expected))
    {
        return ExplainRegexCheck{.expected = std::move(expected), .actual = std::move(actualOutput)};
    }
    return ExplainLinesCheck{.expected = std::move(expected), .actual = std::move(actualOutput)};
}

Schema<UnqualifiedUnboundField, Ordered> createSchemaOf(const std::string& fieldName)
{
    return Schema<UnqualifiedUnboundField, Ordered>{
        std::vector{UnqualifiedUnboundField{Identifier::parse(fieldName), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};
}

/// The header a CSV sink writes for createSchemaOf("id").
constexpr auto IdHeader = "id:UINT64:NOT_NULLABLE\n";

DifferentialSide createIdSide(const std::filesystem::path& resultFile)
{
    return DifferentialSide{.resultFile = resultFile, .schema = createSchemaOf("id"), .expectedHeader = IdHeader};
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

TEST_F(ResultCheckerTest, ChecksResultRowsWithTheDeclaredSchema)
{
    const auto resultFile = writeResultFile("resultchecker_rows.csv", "id:UINT64:NOT_NULLABLE\n1\n2\n3\n");

    EXPECT_TRUE((QueryResultCheck{
        .resultFile = resultFile, .expectedSchema = createSchemaOf("id"), .expectedHeader = IdHeader, .expectedTuples = {"1", "2", "3"}}
                     .check()
                     .has_value()));

    const auto verdict
        = QueryResultCheck{.resultFile = resultFile, .expectedSchema = createSchemaOf("id"), .expectedHeader = IdHeader, .expectedTuples = {"1", "2", "4"}}
              .check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("Result Mismatch"));
}

/// The expected header is what the bound plan's sink writes, and the file holds what the sink wrote. A test whose rows happen to
/// match still fails when the two disagree, because the query then answered about other fields. The checker does not parse the
/// header, so it reports any difference the same way, whatever the output format.
TEST_F(ResultCheckerTest, ReportsAHeaderTheSinkDidNotWrite)
{
    const auto resultFile = writeResultFile("resultchecker_header.csv", "id:UINT64:NOT_NULLABLE\n1\n");

    const auto verdict
        = QueryResultCheck{.resultFile = resultFile, .expectedSchema = createSchemaOf("value"), .expectedHeader = "value:UINT64:NOT_NULLABLE\n", .expectedTuples = {"1"}}
              .check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("does not start with the header"));

    const auto differential
        = DifferentialCheck{.first = {.resultFile = resultFile, .schema = createSchemaOf("id"), .expectedHeader = IdHeader}, .second = {.resultFile = resultFile, .schema = createSchemaOf("id"), .expectedHeader = "ID\n"}}
              .check();
    ASSERT_FALSE(differential.has_value());
    EXPECT_TRUE(differential.error().detail.contains("the second query of the differential block"));
    EXPECT_TRUE(differential.error().detail.contains("does not start with the header"));
}

TEST_F(ResultCheckerTest, ReportsAMissingResultFile)
{
    const auto verdict
        = QueryResultCheck{.resultFile = "/does/not/exist.csv", .expectedSchema = createSchemaOf("id"), .expectedHeader = std::nullopt, .expectedTuples = {"1"}}
              .check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("result file was not written"));
}

TEST_F(ResultCheckerTest, ComparesTheTwoResultFilesOfADifferentialBlock)
{
    const auto first = writeResultFile("resultchecker_differential_first.csv", "id:UINT64:NOT_NULLABLE\n1\n2\n");
    const auto agreeing = writeResultFile("resultchecker_differential_agreeing.csv", "id:UINT64:NOT_NULLABLE\n2\n1\n");
    const auto differing = writeResultFile("resultchecker_differential_differing.csv", "id:UINT64:NOT_NULLABLE\n1\n3\n");

    EXPECT_TRUE((DifferentialCheck{.first = createIdSide(first), .second = createIdSide(agreeing)}.check().has_value()));

    const auto verdict = DifferentialCheck{.first = createIdSide(first), .second = createIdSide(differing)}.check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("differential query execution"));
}

TEST_F(ResultCheckerTest, ReportsWhichSideOfADifferentialBlockHasNoOutput)
{
    const auto written = writeResultFile("resultchecker_differential_written.csv", "id:UINT64:NOT_NULLABLE\n1\n");

    const auto firstMissing = DifferentialCheck{.first = createIdSide("/does/not/exist.csv"), .second = createIdSide(written)}.check();
    ASSERT_FALSE(firstMissing.has_value());
    EXPECT_TRUE(firstMissing.error().detail.contains("the first query of the differential block"));
    EXPECT_TRUE(firstMissing.error().detail.contains("result file was not written"));

    const auto secondMissing = DifferentialCheck{.first = createIdSide(written), .second = createIdSide("/does/not/exist.csv")}.check();
    ASSERT_FALSE(secondMissing.has_value());
    EXPECT_TRUE(secondMissing.error().detail.contains("the second query of the differential block"));
    EXPECT_TRUE(secondMissing.error().detail.contains("result file was not written"));
}

/// A header and no rows is an empty result, which is a valid answer. No header at all is not, if the sink writes one.
TEST_F(ResultCheckerTest, DistinguishesAnEmptyResultFromAMissingHeader)
{
    const auto headerOnly = writeResultFile("resultchecker_header_only.csv", "id:UINT64:NOT_NULLABLE\n");
    EXPECT_TRUE((
        QueryResultCheck{.resultFile = headerOnly, .expectedSchema = createSchemaOf("id"), .expectedHeader = IdHeader, .expectedTuples = {}}
            .check()
            .has_value()));

    const auto empty = writeResultFile("resultchecker_empty.csv", "");
    const auto emptyVerdict = QueryResultCheck{
        .resultFile = empty,
        .expectedSchema = createSchemaOf("id"),
        .expectedHeader = IdHeader,
        .expectedTuples = {}}.check();
    ASSERT_FALSE(emptyVerdict.has_value());
    EXPECT_TRUE(emptyVerdict.error().detail.contains("does not start with the header"));
}

/// A header need not end with a newline: it ends with the tuple delimiter of its output format.
TEST_F(ResultCheckerTest, ComparesAHeaderThatEndsWithAnotherTupleDelimiter)
{
    const auto resultFile = writeResultFile("resultchecker_tuple_delimiter.csv", "ID;1\n2\n");

    EXPECT_TRUE((QueryResultCheck{
        .resultFile = resultFile, .expectedSchema = createSchemaOf("id"), .expectedHeader = "ID;", .expectedTuples = {"1", "2"}}
                     .check()
                     .has_value()));
}

/// Without a header, e.g., for JSON output, the rows are read with the bound schema alone, and an empty file is an empty result.
TEST_F(ResultCheckerTest, ChecksRowsOfAFormatWithoutAHeader)
{
    const auto resultFile = writeResultFile("resultchecker_json.json", "{\"ID\":1}\n{\"ID\":2}\n");

    EXPECT_TRUE((QueryResultCheck{
        .resultFile = resultFile,
        .expectedSchema = createSchemaOf("id"),
        .expectedHeader = std::nullopt,
        .expectedTuples = {"{\"ID\":1}", "{\"ID\":2}"}}
                     .check()
                     .has_value()));

    const auto verdict
        = QueryResultCheck{.resultFile = resultFile, .expectedSchema = createSchemaOf("id"), .expectedHeader = std::nullopt, .expectedTuples = {"{\"ID\":1}", "{\"ID\":3}"}}
              .check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("Result Mismatch"));
    EXPECT_FALSE(verdict.error().detail.contains("Schema Mismatch"));

    const auto empty = writeResultFile("resultchecker_json_empty.json", "");
    EXPECT_TRUE(
        (QueryResultCheck{.resultFile = empty, .expectedSchema = createSchemaOf("id"), .expectedHeader = std::nullopt, .expectedTuples = {}}
             .check()
             .has_value()));
}

/// Each half of a differential block is read with the schema of its own bound plan, so two halves without a header compare as well.
TEST_F(ResultCheckerTest, ComparesADifferentialBlockOfFormatsWithoutAHeader)
{
    const auto first = writeResultFile("resultchecker_differential_json_first.json", "{\"ID\":1}\n{\"ID\":2}\n");
    const auto agreeing = writeResultFile("resultchecker_differential_json_agreeing.json", "{\"ID\":2}\n{\"ID\":1}\n");
    const auto differing = writeResultFile("resultchecker_differential_json_differing.json", "{\"ID\":1}\n{\"ID\":3}\n");

    const auto sideOf = [](const std::filesystem::path& file)
    { return DifferentialSide{.resultFile = file, .schema = createSchemaOf("id"), .expectedHeader = std::nullopt}; };
    EXPECT_TRUE((DifferentialCheck{.first = sideOf(first), .second = sideOf(agreeing)}.check().has_value()));

    const auto verdict = DifferentialCheck{.first = sideOf(first), .second = sideOf(differing)}.check();
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("Result Mismatch"));
}

TEST_F(ResultCheckerTest, ExplainExactResultLinesKeepVanillaMatching)
{
    EXPECT_TRUE(runCheck(makeExplainCheck({"== Optimized Plan ==", "SINK(FILE)"}, "== Optimized Plan ==\nSINK(FILE)\n")).has_value());

    const auto verdict = runCheck(makeExplainCheck({"== Optimized Plan ==", "SINK(FILE)"}, "== Optimized Plan ==\nSINK(OTHER)\n"));
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

    EXPECT_TRUE(runCheck(makeExplainCheck(std::move(expectedResultLines), "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n"))
                    .has_value());
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsReportMissingPositiveMatch)
{
    const auto verdict = runCheck(
        makeExplainCheck({R"(<REGEX>SELECTION\(VALUE > 2\)</REGEX>)"}, "== Optimized Plan ==\nSINK(SINK42)\n  SOURCE(stream_17)\n"));
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("expected pattern \"SELECTION\\(VALUE > 2\\)\" to match"));
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsReportUnexpectedNegativeMatch)
{
    const auto verdict = runCheck(makeExplainCheck(
        {"<!REGEX>SELECTION</!REGEX>"}, "== Optimized Plan ==\nSINK(SINK42)\n  SELECTION(VALUE > 2)\n    SOURCE(stream_17)\n"));
    ASSERT_FALSE(verdict.has_value());
    EXPECT_TRUE(verdict.error().detail.contains("expected pattern \"SELECTION\" not to match"));
}

TEST_F(ResultCheckerTest, ExplainRegexAssertionsRejectMixedMatchingModes)
{
    const auto verdict = runCheck(makeExplainCheck({"<REGEX>SINK</REGEX>", "SOURCE(stream)"}, "SINK\nSOURCE(stream)"));
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
        const auto verdict = runCheck(makeExplainCheck(expected, "SINK"));
        ASSERT_FALSE(verdict.has_value());
        EXPECT_TRUE(verdict.error().detail.contains("Invalid Explain Regex Assertion"));
    }
}

/// The report is what a developer reads when a test fails, so its shape is pinned: the schema lines for a field that is
/// missing and one that is unexpected. The schemas come from the bound plans, so only the two halves of a differential block can differ.
TEST_F(ResultCheckerTest, ReportsASchemaMismatchAsBefore)
{
    const auto resultFile = writeResultFile("resultchecker_golden_schema.csv", "1,2\n");
    const Schema<UnqualifiedUnboundField, Ordered> expectedSchema{std::vector{
        UnqualifiedUnboundField{Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        UnqualifiedUnboundField{Identifier::parse("value"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};
    const Schema<UnqualifiedUnboundField, Ordered> actualSchema{std::vector{
        UnqualifiedUnboundField{Identifier::parse("id"), DataTypeProvider::provideDataType(DataType::Type::UINT64)},
        UnqualifiedUnboundField{Identifier::parse("extra"), DataTypeProvider::provideDataType(DataType::Type::UINT64)}}};

    const auto verdict = DifferentialCheck{
        .first = {.resultFile = resultFile, .schema = expectedSchema, .expectedHeader = std::nullopt},
        .second
        = {.resultFile = resultFile,
           .schema = actualSchema,
           .expectedHeader
           = std::nullopt}}.check();

    ASSERT_FALSE(verdict.has_value());
    EXPECT_EQ(verdict.error().detail, R"(Schema Mismatch
---------------
QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false)), QualifiedUnboundField: (name: VALUE, type: DataType(type: UINT64 nullable: false)) != QualifiedUnboundField: (name: ID, type: DataType(type: UINT64 nullable: false)), QualifiedUnboundField: (name: EXTRA, type: DataType(type: UINT64 nullable: false))
- 'QualifiedUnboundField: (name: VALUE, type: DataType(type: UINT64 nullable: false))' is missing from actual result schema.
+ 'QualifiedUnboundField: (name: EXTRA, type: DataType(type: UINT64 nullable: false))' is unexpected field in actual result schema.

All Results match

This error happened during differential query execution.)");
}

/// The row table of a result mismatch: one row that differs and one row the sink wrote that the test did not expect.
TEST_F(ResultCheckerTest, ReportsAResultMismatchAsBefore)
{
    const auto resultFile = writeResultFile("resultchecker_golden_rows.csv", "id:UINT64:NOT_NULLABLE\n1\n3\n4\n");

    const auto verdict
        = QueryResultCheck{.resultFile = resultFile, .expectedSchema = createSchemaOf("id"), .expectedHeader = IdHeader, .expectedTuples = {"1", "2"}}
              .check();

    ASSERT_FALSE(verdict.has_value());
    EXPECT_EQ(verdict.error().detail, R"(Result Mismatch
Expected Results(Sorted) | Actual Results(Sorted)
-------------------------------------------------
1 | 1
2 | _
_ | 3
_ | 4)");
}

/// runCheck is the runner's only path from a check to a Verdict, so the dispatch over every alternative has its own test.
TEST_F(ResultCheckerTest, RunCheckDispatchesToEveryAlternative)
{
    const auto resultFile = writeResultFile("resultchecker_dispatch.csv", "id:UINT64:NOT_NULLABLE\n1\n");

    EXPECT_TRUE(
        runCheck(
            QueryResultCheck{
                .resultFile = resultFile, .expectedSchema = createSchemaOf("id"), .expectedHeader = IdHeader, .expectedTuples = {"1"}})
            .has_value());
    EXPECT_TRUE(runCheck(DifferentialCheck{.first = createIdSide(resultFile), .second = createIdSide(resultFile)}).has_value());
    EXPECT_TRUE(runCheck(ExplainLinesCheck{.expected = {"SINK"}, .actual = "SINK\n"}).has_value());
    EXPECT_TRUE(runCheck(ExplainRegexCheck{.expected = {"<REGEX>SINK</REGEX>"}, .actual = "SINK\n"}).has_value());
}
}
