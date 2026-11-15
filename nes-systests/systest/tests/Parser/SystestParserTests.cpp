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

#include <cstdint>
#include <functional>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include <Discovery/TestFileReader.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Model/ConfigurationOverride.hpp>
#include <Parser/SystestParser.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

class SystestParserTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("SystestParserTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestParserTest test class.");
    }

    static void TearDownTestCase() { NES_DEBUG("Tear down SystestParserTest test class."); }
};

TEST_F(SystestParserTest, testEmptyFile)
{
    SystestParser parser{};
    const std::string str;

    parser.loadString(str);
    EXPECT_NO_THROW(parser.parse());
}

TEST_F(SystestParserTest, testEmptyLinesAndCommasFile)
{
    SystestParser parser{};
    /// Comment, new line in Unix/Linux, Windows, Older Mac systems
    const std::string str = std::string("#\n") + "\n" + "\r\n" + "\r";

    parser.loadString(str);
    EXPECT_NO_THROW(parser.parse());
}

TEST_F(SystestParserTest, testAttachSourceCallbackSource)
{
    SystestParser parser{};
    const std::string sourceIn = "CREATE LOGICAL SOURCE window(id UINT64, value UINT64, timestamp UINT64 timestamp);\n"
                                 "CREATE PHYSICAL SOURCE FOR window TYPE File";

    bool isCreateCallbackCalled = false;

    const std::string str = sourceIn + "\n";

    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>& testData)
        {
            isCreateCallbackCalled = true;
            ASSERT_FALSE(testData.has_value());
        });

    parser.loadString(str);
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(isCreateCallbackCalled);
}

TEST_F(SystestParserTest, testCallbackQuery)
{
    SystestParser parser{};

    static constexpr std::string_view TestFileName = "testCallbackQuery";
    const std::string queryIn = "SELECT id, value, timestamp FROM window WHERE value == 1 INTO SINK;";
    const std::string delimiter = "----";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    bool queryCallbackCalled = false;

    const std::string testFileString = fmt::format("{}\n{}\n{}\n{}\n", queryIn, delimiter, tpl1, tpl2);
    std::vector<std::string> receivedResultTuples;

    parser.registerOnQueryCallback(
        [&](const std::string& queryOut, SystestQueryId)
        {
            ASSERT_EQ(queryIn, queryOut);
            queryCallbackCalled = true;
        });
    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { FAIL(); });
    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, const SystestQueryId)
                                          { receivedResultTuples = std::move(resultTuples); });

    parser.loadString(testFileString);
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(queryCallbackCalled);
    /// Check that the queryResult map contains the expected two results for the query defined above
    ASSERT_EQ(receivedResultTuples.size(), 2);
    ASSERT_EQ(receivedResultTuples.at(0), tpl1);
    ASSERT_EQ(receivedResultTuples.at(1), tpl2);
}

TEST_F(SystestParserTest, testResultTuplesWithoutQuery)
{
    SystestParser parser{};
    const std::string delimiter = "----";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    const std::string str = delimiter + "\n" + tpl1 + "\n" + tpl2 + "\n";

    parser.registerOnQueryCallback([&](const std::string&, SystestQueryId) { FAIL(); });
    parser.registerOnResultTuplesCallback(
        [&](const std::vector<std::string>&, const SystestQueryId)
        {
            /// nop
        });
    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { FAIL(); });

    parser.loadString(str);
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserTest, testDifferentialQueryCallbackFromFile)
{
    SystestParser parser{};

    const std::string expectedMainQuery = "SELECT id * 10 AS id, value, timestamp FROM stream INTO streamSink;";
    const std::string expectedDifferentialQuery = "SELECT id * 2 * 5 AS id, value, timestamp FROM stream INTO streamSink;";

    bool mainQueryCallbackCalled = false;
    bool differentialQueryCallbackCalled = false;

    parser.registerOnQueryCallback(
        [&](const std::string& queryOut, SystestQueryId)
        {
            ASSERT_FALSE(differentialQueryCallbackCalled) << "Main query callback was called after the differential one.";
            ASSERT_FALSE(mainQueryCallbackCalled) << "Main query callback should only be called once.";
            ASSERT_EQ(expectedMainQuery, queryOut);
            mainQueryCallbackCalled = true;
        });

    parser.registerOnDifferentialQueryBlockCallback(
        [&](std::string leftQuery, std::string rightQuery, SystestQueryId mainId, SystestQueryId differentialId)
        {
            ASSERT_EQ(mainId, differentialId) << "Differential block should reuse the last parsed query id.";
            ASSERT_TRUE(mainQueryCallbackCalled) << "Differential callback was called before the main query callback.";
            ASSERT_FALSE(differentialQueryCallbackCalled) << "Differential query callback should only be called once.";
            ASSERT_EQ(expectedMainQuery, leftQuery);
            ASSERT_EQ(expectedDifferentialQuery, rightQuery);
            differentialQueryCallbackCalled = true;
        });

    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { });

    parser.registerOnResultTuplesCallback([](std::vector<std::string>&&, SystestQueryId)
                                          { FAIL() << "Result tuple callback should not be called for a differential query test."; });
    parser.registerOnErrorExpectationCallback(
        [](const SystestParser::ErrorExpectation&, SystestQueryId)
        { FAIL() << "Error expectation callback should not be called for a differential query test."; });

    static constexpr std::string_view Filename = SYSTEST_DATA_DIR "differential.dummy";
    parser.loadString(readTestFile(Filename));

    EXPECT_NO_THROW(parser.parse());

    ASSERT_TRUE(mainQueryCallbackCalled) << "The main query callback was never called.";
    ASSERT_TRUE(differentialQueryCallbackCalled) << "The differential query callback was never called.";
}

TEST_F(SystestParserTest, testDifferentialQueryCallbackInlineSyntax)
{
    SystestParser parser{};

    const std::string expectedMainQuery = "SELECT id * 10 AS id, value, timestamp FROM stream INTO streamSink;";
    const std::string expectedDifferentialQuery = "SELECT id * 2 * 5 AS id, value, timestamp FROM stream INTO streamSink;";

    bool mainQueryCallbackCalled = false;
    bool differentialQueryCallbackCalled = false;

    parser.registerOnQueryCallback(
        [&](const std::string& queryOut, SystestQueryId)
        {
            ASSERT_FALSE(differentialQueryCallbackCalled) << "Main query callback was called after the differential one.";
            ASSERT_FALSE(mainQueryCallbackCalled) << "Main query callback should only be called once.";
            ASSERT_EQ(expectedMainQuery, queryOut);
            mainQueryCallbackCalled = true;
        });

    parser.registerOnDifferentialQueryBlockCallback(
        [&](std::string leftQuery, std::string rightQuery, SystestQueryId mainId, SystestQueryId differentialId)
        {
            ASSERT_EQ(mainId, differentialId) << "Differential block should reuse the last parsed query id.";
            ASSERT_TRUE(mainQueryCallbackCalled) << "Differential callback was called before the main query callback.";
            ASSERT_FALSE(differentialQueryCallbackCalled) << "Differential query callback should only be called once.";
            ASSERT_EQ(expectedMainQuery, leftQuery);
            ASSERT_EQ(expectedDifferentialQuery, rightQuery);
            differentialQueryCallbackCalled = true;
        });

    parser.registerOnResultTuplesCallback([](std::vector<std::string>&&, SystestQueryId)
                                          { FAIL() << "Result tuple callback should not be called for a differential query test."; });

    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { });

    parser.registerOnErrorExpectationCallback(
        [](const SystestParser::ErrorExpectation&, SystestQueryId)
        { FAIL() << "Error expectation callback should not be called for a differential query test."; });

    static constexpr std::string_view TestContent = R"(
CREATE LOGICAL SOURCE stream(id INT64, value INT64, timestamp INT64);
CREATE PHYSICAL SOURCE FOR stream TYPE File;
ATTACH INLINE
5,1,1000

CREATE SINK streamSink(id INT64, stream.value INT64, stream.timestamp INT64) TYPE File;

SELECT id * 10 AS id, value, timestamp FROM stream INTO streamSink;
====
SELECT id * 2 * 5 AS id, value, timestamp FROM stream INTO streamSink;
)";

    parser.loadString(std::string(TestContent));

    EXPECT_NO_THROW(parser.parse());

    ASSERT_TRUE(mainQueryCallbackCalled) << "The main query callback was never called.";
    ASSERT_TRUE(differentialQueryCallbackCalled) << "The differential query callback was never called.";
}

/// NOLINTBEGIN(bugprone-unchecked-optional-access)
TEST_F(SystestParserTest, testExplainCallbackWithVerbatimResultBlock)
{
    SystestParser parser{};

    const std::string explainIn = "EXPLAIN (OPTIMIZED, FORMAT TEXT) SELECT id FROM stream WHERE value > 4 INTO sink;";
    const std::string queryIn = "SELECT id FROM stream INTO sink;";

    /// The expected explain output contains lines that look like parser tokens: a line starting with `----`,
    /// a section header, and an indented line starting with `SELECT`. All must be delivered verbatim.
    static constexpr std::string_view TestContent = R"(EXPLAIN (OPTIMIZED, FORMAT TEXT) SELECT id FROM stream WHERE value > 4 INTO sink;
----
== Global Optimized Plan ==
SINK(SINK1)
  SELECTION(predicate: value > 4)
    SOURCE(stream)
==END==

SELECT id FROM stream INTO sink;
----
1
)";

    std::optional<SystestQueryId> explainQueryId;
    std::optional<SystestQueryId> selectQueryId;
    std::string receivedExplainStatement;
    std::vector<std::vector<std::string>> receivedResultBlocks;

    parser.registerOnExplainQueryCallback(
        [&](const std::string& statement, SystestQueryId queryId)
        {
            receivedExplainStatement = statement;
            explainQueryId = queryId;
        });
    parser.registerOnQueryCallback(
        [&](const std::string& queryOut, SystestQueryId queryId)
        {
            ASSERT_EQ(queryIn, queryOut);
            selectQueryId = queryId;
        });
    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, SystestQueryId)
                                          { receivedResultBlocks.push_back(std::move(resultTuples)); });
    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { FAIL(); });

    parser.loadString(std::string(TestContent));
    EXPECT_NO_THROW(parser.parse());

    ASSERT_EQ(receivedExplainStatement, explainIn);
    ASSERT_TRUE(explainQueryId.has_value());
    ASSERT_TRUE(selectQueryId.has_value());
    EXPECT_EQ(explainQueryId->getRawValue() + 1, selectQueryId->getRawValue()) << "EXPLAIN must participate in query id accounting.";

    ASSERT_EQ(receivedResultBlocks.size(), 2);
    const std::vector<std::string> expectedExplainBlock{
        "== Global Optimized Plan ==", "SINK(SINK1)", "  SELECTION(predicate: value > 4)", "    SOURCE(stream)"};
    EXPECT_EQ(receivedResultBlocks.at(0), expectedExplainBlock);
    EXPECT_EQ(receivedResultBlocks.at(1), std::vector<std::string>{"1"});
}

TEST_F(SystestParserTest, testExplainWithErrorExpectation)
{
    SystestParser parser{};

    static constexpr std::string_view TestContent = R"(EXPLAIN (LOGICAL) SELECT id FROM unknownStream INTO sink;
----
ERROR 3000
)";

    bool explainCallbackCalled = false;
    bool errorExpectationCallbackCalled = false;

    parser.registerOnExplainQueryCallback([&](const std::string&, SystestQueryId) { explainCallbackCalled = true; });
    parser.registerOnErrorExpectationCallback(
        [&](const SystestParser::ErrorExpectation& expectation, SystestQueryId)
        {
            errorExpectationCallbackCalled = true;
            EXPECT_EQ(expectation.code, 3000);
        });
    parser.registerOnResultTuplesCallback(
        [](std::vector<std::string>&&, SystestQueryId) /// NOLINT(cppcoreguidelines-rvalue-reference-param-not-moved)
        { FAIL() << "Result tuple callback should not be called for an error expectation."; });

    parser.loadString(std::string(TestContent));
    EXPECT_NO_THROW(parser.parse());

    ASSERT_TRUE(explainCallbackCalled);
    ASSERT_TRUE(errorExpectationCallbackCalled);
}

TEST_F(SystestParserTest, testMultiLineExplainStatement)
{
    SystestParser parser{};

    static constexpr std::string_view TestContent = R"(EXPLAIN (ALL, FORMAT TEXT)
SELECT id FROM stream
INTO sink;
----
== Initial Logical Plan ==
SINK(SINK1)
)";

    std::string receivedExplainStatement;
    std::vector<std::string> receivedResultLines;

    parser.registerOnExplainQueryCallback([&](const std::string& statement, SystestQueryId) { receivedExplainStatement = statement; });
    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, SystestQueryId)
                                          { receivedResultLines = std::move(resultTuples); });

    parser.loadString(std::string(TestContent));
    EXPECT_NO_THROW(parser.parse());

    EXPECT_EQ(receivedExplainStatement, "EXPLAIN (ALL, FORMAT TEXT)\nSELECT id FROM stream\nINTO sink;");
    const std::vector<std::string> expectedResultLines{"== Initial Logical Plan ==", "SINK(SINK1)"};
    EXPECT_EQ(receivedResultLines, expectedResultLines);
}

/// NOLINTEND(bugprone-unchecked-optional-access)

/// Regression test for issue #945: substitution rules must not replace substrings inside longer identifiers.
/// For example, a substitution rule for keyword "we" must not modify "producedPower" to "producedPowe<replaced>r".
TEST_F(SystestParserTest, testSubstitutionRuleRespectsWordBoundaries)
{
    SystestParser parser{};

    /// Register a substitution rule for the short keyword "we"
    parser.registerSubstitutionRule({.keyword = "we", .ruleFunction = [](std::string& substitute) { substitute = "REPLACED"; }});

    /// The query contains "producedPower" which has "we" as a substring, and also "INTO we" which is an exact word match.
    static constexpr std::string_view TestContent = R"(
SELECT producedPower, timestamp FROM source INTO we;
----
100,1000
)";

    std::string receivedQuery;
    parser.registerOnQueryCallback([&](const std::string& queryOut, SystestQueryId) { receivedQuery = queryOut; });
    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { });
    parser.registerOnResultTuplesCallback([](std::vector<std::string>&& tuples, SystestQueryId) { (void)std::move(tuples); });

    parser.loadString(std::string(TestContent));
    EXPECT_NO_THROW(parser.parse());

    /// "producedPower" must NOT be modified, but "we" as a standalone word must be replaced
    EXPECT_NE(receivedQuery.find("producedPower"), std::string::npos)
        << "producedPower was incorrectly modified by substitution rule. Query: " << receivedQuery;
    EXPECT_NE(receivedQuery.find("INTO REPLACED"), std::string::npos) << "Standalone 'we' was not replaced. Query: " << receivedQuery;
    EXPECT_EQ(receivedQuery.find("INTO we"), std::string::npos) << "Standalone 'we' should have been replaced. Query: " << receivedQuery;
}

namespace
{

std::string quoteAll(const std::vector<std::string>& lines)
{
    std::vector<std::string> quoted;
    quoted.reserve(lines.size());
    for (const auto& line : lines)
    {
        quoted.push_back(fmt::format("'{}'", line));
    }
    return fmt::format("[{}]", fmt::join(quoted, ", "));
}

std::string formatOverrides(const std::vector<ConfigurationOverride>& overrides)
{
    std::vector<std::string> alternatives;
    alternatives.reserve(overrides.size());
    for (const auto& override : overrides)
    {
        std::vector<std::string> pairs;
        for (const auto& [key, value] : override)
        {
            pairs.push_back(fmt::format("{}={}", key, value));
        }
        alternatives.push_back(fmt::format("{{{}}}", fmt::join(pairs, ",")));
    }
    return fmt::format("[{}]", fmt::join(alternatives, ", "));
}

void recordCallbacks(SystestParser& parser, std::vector<std::string>& events)
{
    parser.registerOnCreateCallback(
        [&events](const std::string& sql, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>& attach)
        {
            if (not attach.has_value())
            {
                events.push_back(fmt::format("CREATE {}", sql));
                return;
            }
            const auto& [type, data] = attach.value();
            events.push_back(
                fmt::format("CREATE {} ATTACH {} {}", sql, type == TestDataIngestionType::INLINE ? "INLINE" : "FILE", quoteAll(data)));
        });
    parser.registerOnQueryCallback([&events](const std::string& sql, const SystestQueryId id)
                                   { events.push_back(fmt::format("QUERY {} {}", id.getRawValue(), sql)); });
    parser.registerOnExplainQueryCallback([&events](const std::string& sql, const SystestQueryId id)
                                          { events.push_back(fmt::format("EXPLAIN {} {}", id.getRawValue(), sql)); });
    parser.registerOnResultTuplesCallback(
        [&events](std::vector<std::string>&& rows, const SystestQueryId id)
        {
            const auto taken = std::move(rows);
            events.push_back(fmt::format("RESULT {} {}", id.getRawValue(), quoteAll(taken)));
        });
    parser.registerOnErrorExpectationCallback(
        [&events](const SystestParser::ErrorExpectation& error, const SystestQueryId id)
        {
            const auto code = static_cast<uint64_t>(error.code);
            events.push_back(
                error.message.has_value() ? fmt::format("ERROR {} {} '{}'", id.getRawValue(), code, error.message.value())
                                          : fmt::format("ERROR {} {}", id.getRawValue(), code));
        });
    parser.registerOnDifferentialQueryBlockCallback(
        [&events](const std::string& left, const std::string& right, const SystestQueryId leftId, const SystestQueryId rightId)
        { events.push_back(fmt::format("DIFFERENTIAL {} {} {} | {}", leftId.getRawValue(), rightId.getRawValue(), left, right)); });
    parser.registerOnConfigurationCallback([&events](const std::vector<ConfigurationOverride>& overrides)
                                           { events.push_back(fmt::format("CONFIGURATION {}", formatOverrides(overrides))); });
    parser.registerOnGlobalConfigurationCallback([&events](const std::vector<ConfigurationOverride>& overrides)
                                                 { events.push_back(fmt::format("GLOBALCONFIGURATION {}", formatOverrides(overrides))); });
}

std::vector<std::string> parseAndRecord(const std::string& content)
{
    SystestParser parser{};
    std::vector<std::string> events;
    recordCallbacks(parser, events);
    parser.loadString(content);
    parser.parse();
    return events;
}

std::vector<std::string> parseAndRecordUntilError(const std::string& content, const ErrorCode expectedCode)
{
    SystestParser parser{};
    std::vector<std::string> events;
    recordCallbacks(parser, events);
    parser.loadString(content);
    try
    {
        parser.parse();
        ADD_FAILURE() << "expected an exception with error code " << expectedCode;
    }
    catch (const Exception& exception)
    {
        EXPECT_EQ(exception.code(), expectedCode);
    }
    return events;
}

}

TEST_F(SystestParserTest, characterizeStatementLineDirectlyAfterResultRowsIsConsumedAsTerminator)
{
    const std::string query = "SELECT a FROM s INTO k;\n----\n1\n";
    const std::vector<std::string> queryAndResult{"QUERY 1 SELECT a FROM s INTO k;", "RESULT 1 ['1']"};

    EXPECT_EQ(parseAndRecord(query + "CREATE SINK x(a UINT64) TYPE File;\n"), queryAndResult);
    EXPECT_EQ(parseAndRecord(query + "ERROR 1003\n"), queryAndResult);
    EXPECT_EQ(parseAndRecord(query + "Configuration k: 1\n"), queryAndResult);
    EXPECT_EQ(
        parseAndRecord(query + "\nCREATE SINK x(a UINT64) TYPE File;\n"),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;", "RESULT 1 ['1']", "CREATE CREATE SINK x(a UINT64) TYPE File;"}));
    EXPECT_EQ(parseAndRecordUntilError(query + "SELECT b FROM s INTO k;\n----\n2\n", ErrorCode::SLTUnexpectedToken), queryAndResult);
}

TEST_F(SystestParserTest, characterizeResultRowsKeepWhitespaceLinesAndStripComments)
{
    EXPECT_EQ(
        parseAndRecord("SELECT a FROM s INTO k;\n----\n1,a#b\n  # indented note\n   \n# full line comment\n2\n"),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;", "RESULT 1 ['1,a', '  ', '   ', '2']"}));
    EXPECT_EQ(
        parseAndRecord("SELECT a FROM s INTO k;\n----\n\nSELECT b FROM s INTO k;\n----\n2\n"),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;", "RESULT 1 []", "QUERY 2 SELECT b FROM s INTO k;", "RESULT 2 ['2']"}));
}

TEST_F(SystestParserTest, characterizeRowStartingWithSelectEndsResultBlockAndIsDropped)
{
    EXPECT_EQ(
        parseAndRecord("SELECT a FROM s INTO k;\n----\n1\nselected,2\n"),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;", "RESULT 1 ['1']"}));
    EXPECT_EQ(
        parseAndRecord("SELECT a FROM s INTO k;\n----\nSELECTION\n"),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;", "RESULT 1 []"}));
}

TEST_F(SystestParserTest, characterizeDelimiterWithTrailingCharactersIsNotSkipped)
{
    EXPECT_EQ(
        parseAndRecordUntilError("SELECT a FROM s INTO k;\n---- # rows\n1\n", ErrorCode::SLTUnexpectedToken),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;", "RESULT 1 []"}));
    EXPECT_EQ(
        parseAndRecordUntilError("SELECT a FROM s INTO k;\r\n----\r\n1\r\n", ErrorCode::SLTUnexpectedToken),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;\r", "RESULT 1 []"}));
    EXPECT_EQ(
        parseAndRecord("EXPLAIN SELECT a FROM s INTO k;\n---- \nplan\n==END==\n"),
        (std::vector<std::string>{"EXPLAIN 1 EXPLAIN SELECT a FROM s INTO k;", "RESULT 1 ['---- ', 'plan']"}));
    EXPECT_EQ(
        parseAndRecord("SELECT a FROM s INTO k;\n-----\n1\n"), (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;\n-----\n1"}));
}

TEST_F(SystestParserTest, characterizeQueryTextBoundaries)
{
    EXPECT_EQ(
        parseAndRecord("SELECT a FROM s INTO k; # note\n----\n1\n"),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k; ", "RESULT 1 ['1']"}));
    EXPECT_EQ(
        parseAndRecord("SELECT a FROM s INTO k;\nCREATE SINK x(a UINT64) TYPE File;\n----\n1\n"),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;\nCREATE SINK x(a UINT64) TYPE File;", "RESULT 1 ['1']"}));
    EXPECT_EQ(
        parseAndRecordUntilError("SELECT a FROM s INTO k;\nConfiguration c: 1\n----\n1\n", ErrorCode::InvalidQuerySyntax),
        std::vector<std::string>{});
    EXPECT_EQ(
        parseAndRecord("SELECT a\n\n  # note\nFROM s INTO k;\n\n----\n1\n"),
        (std::vector<std::string>{"QUERY 1 SELECT a\nFROM s INTO k;", "RESULT 1 ['1']"}));
    EXPECT_EQ(parseAndRecord("SELECT a FROM s INTO k\n"), (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k"}));
    EXPECT_EQ(parseAndRecordUntilError("SELECT a FROM s INTO k\n----\n1\n", ErrorCode::InvalidQuerySyntax), std::vector<std::string>{});
    EXPECT_EQ(
        parseAndRecordUntilError("SELECT a FROM s INTO k\n====\nSELECT b FROM s INTO k;\n", ErrorCode::InvalidQuerySyntax),
        std::vector<std::string>{});
    EXPECT_EQ(
        parseAndRecordUntilError("SELECT a FROM s INTO k;\n\nSELECT b FROM s INTO k;\n----\n1\n", ErrorCode::SLTUnexpectedToken),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;"}));
}

TEST_F(SystestParserTest, characterizeTokensAreCaseInsensitiveAndSelectIsAPrefix)
{
    EXPECT_EQ(
        parseAndRecord("select a from s into k;\n----\n1\n"),
        (std::vector<std::string>{"QUERY 1 select a from s into k;", "RESULT 1 ['1']"}));
    EXPECT_EQ(
        parseAndRecord("  SELECT a FROM s INTO k;\n----\n1\n"),
        (std::vector<std::string>{"QUERY 1   SELECT a FROM s INTO k;", "RESULT 1 ['1']"}));
    EXPECT_EQ(parseAndRecord("SELECTa;\n----\n1\n"), (std::vector<std::string>{"QUERY 1 SELECTa;", "RESULT 1 ['1']"}));
    EXPECT_EQ(parseAndRecordUntilError("  SELECTa;\n----\n1\n", ErrorCode::SLTUnexpectedToken), std::vector<std::string>{});
    EXPECT_EQ(
        parseAndRecord("create sink x(a UINT64) type File;\nexplain select a from s into k;\n----\nplan\n==END==\n"),
        (std::vector<std::string>{
            "CREATE create sink x(a UINT64) type File;", "EXPLAIN 1 explain select a from s into k;", "RESULT 1 ['plan']"}));
    EXPECT_EQ(
        parseAndRecord("configuration c: 1\nGLOBALCONFIGURATION g: 2\n"),
        (std::vector<std::string>{"CONFIGURATION [{c=1}]", "GLOBALCONFIGURATION [{g=2}]"}));
}

TEST_F(SystestParserTest, characterizeQueryIdsAcrossResultErrorAndExplain)
{
    EXPECT_EQ(
        parseAndRecord(
            "SELECT a FROM s INTO k;\n----\n1\n\n"
            "SELECT b FROM s INTO k;\n----\nERROR 1003\n\n"
            "EXPLAIN SELECT c FROM s INTO k;\n----\nplan\n==END==\n"
            "SELECT d FROM s INTO k;\n----\n4\n"),
        (std::vector<std::string>{
            "QUERY 1 SELECT a FROM s INTO k;",
            "RESULT 1 ['1']",
            "QUERY 2 SELECT b FROM s INTO k;",
            "ERROR 2 1003",
            "EXPLAIN 3 EXPLAIN SELECT c FROM s INTO k;",
            "RESULT 3 ['plan']",
            "QUERY 4 SELECT d FROM s INTO k;",
            "RESULT 4 ['4']"}));
}

TEST_F(SystestParserTest, characterizeExplainVerbatimBlock)
{
    EXPECT_EQ(
        parseAndRecord(
            "EXPLAIN SELECT a FROM s INTO k;\n----\nSELECT x\n----\n\nCREATE y\n====\n  indented\n==END== trailing\n"
            "SELECT b FROM s INTO k;\n----\n1\n"),
        (std::vector<std::string>{
            "EXPLAIN 1 EXPLAIN SELECT a FROM s INTO k;",
            "RESULT 1 ['SELECT x', '----', '', 'CREATE y', '====', '  indented']",
            "QUERY 2 SELECT b FROM s INTO k;",
            "RESULT 2 ['1']"}));
    EXPECT_EQ(
        parseAndRecord("EXPLAIN SELECT a FROM s INTO k;\n----\nplan # note\n  ==END==\n\nSELECT b FROM s INTO k;\n----\n1\n"),
        (std::vector<std::string>{
            "EXPLAIN 1 EXPLAIN SELECT a FROM s INTO k;", "RESULT 1 ['plan ', '  ==END==', '', 'SELECT b FROM s INTO k;', '----', '1']"}));
}

TEST_F(SystestParserTest, characterizeErrorExpectationForms)
{
    const std::string query = "SELECT a FROM s INTO k;\n----\n";
    const std::vector<std::pair<std::string, std::string>> accepted{
        {"ERROR SLTUnexpectedToken\n", "ERROR 1 1002"},
        {"ERROR 1003 \n", "ERROR 1 1003"},
        {"ERROR 1003 unquoted message text\n", "ERROR 1 1003 'unquoted message text'"},
        {"error 01003 \t\"quoted\"\n", "ERROR 1 1003 'quoted'"},
        {"ERROR 1003 \"\"\n", "ERROR 1 1003 ''"},
        {"ERROR 1003 \"\n", "ERROR 1 1003 ''"}};
    for (const auto& [errorLine, expectedEvent] : accepted)
    {
        SCOPED_TRACE(errorLine);
        EXPECT_EQ(parseAndRecord(query + errorLine), (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;", expectedEvent}));
    }

    const std::vector<std::string> rejected{
        "ERROR\n",
        "ERROR sltunexpectedtoken\n",
        "ERROR 1003 \"open\n",
        "ERROR 1003 close\"\n",
        "ERROR 1003 \"message\" # note\n",
        "ERROR 1003 \"issue #42\"\n"};
    for (const auto& errorLine : rejected)
    {
        SCOPED_TRACE(errorLine);
        EXPECT_EQ(
            parseAndRecordUntilError(query + errorLine, ErrorCode::SLTUnexpectedToken),
            (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;"}));
    }

    EXPECT_EQ(parseAndRecordUntilError("ERROR 1003\n", ErrorCode::TestException), std::vector<std::string>{});
}

TEST_F(SystestParserTest, characterizeDifferentialBlocks)
{
    EXPECT_EQ(
        parseAndRecord(
            "SELECT a FROM s INTO k;\n\n====\nSELECT b FROM s\nINTO k;\nCREATE SINK x(a UINT64) TYPE File;\nConfiguration c: 1\n\n"
            "SELECT c FROM s INTO k;\n----\n1\n"),
        (std::vector<std::string>{
            "QUERY 1 SELECT a FROM s INTO k;",
            "DIFFERENTIAL 1 1 SELECT a FROM s INTO k; | SELECT b FROM s\nINTO k;",
            "CREATE CREATE SINK x(a UINT64) TYPE File;",
            "CONFIGURATION [{c=1}]",
            "QUERY 2 SELECT c FROM s INTO k;",
            "RESULT 2 ['1']"}));

    const std::vector<std::string> queryAndDifferential{
        "QUERY 1 SELECT a FROM s INTO k;", "DIFFERENTIAL 1 1 SELECT a FROM s INTO k; | SELECT b FROM s INTO k;"};
    EXPECT_EQ(
        parseAndRecordUntilError(
            "SELECT a FROM s INTO k;\n====\nSELECT b FROM s INTO k;\n====\nSELECT c FROM s INTO k;\n", ErrorCode::SLTUnexpectedToken),
        queryAndDifferential);
    EXPECT_EQ(
        parseAndRecordUntilError("SELECT a FROM s INTO k;\n====\nSELECT b FROM s INTO k;\n----\n1\n", ErrorCode::SLTUnexpectedToken),
        queryAndDifferential);
    EXPECT_EQ(
        parseAndRecordUntilError("SELECT a FROM s INTO k;\n====\nSELECT b FROM s INTO k;\nERROR 1003\n", ErrorCode::TestException),
        queryAndDifferential);
    EXPECT_EQ(
        parseAndRecordUntilError("SELECT a FROM s INTO k;\n----\n1\n\n====\nSELECT b FROM s INTO k;\n", ErrorCode::SLTUnexpectedToken),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;", "RESULT 1 ['1']"}));
    EXPECT_EQ(
        parseAndRecordUntilError("SELECT a FROM s INTO k;\n====\n\n", ErrorCode::SLTUnexpectedToken),
        (std::vector<std::string>{"QUERY 1 SELECT a FROM s INTO k;"}));
}

TEST_F(SystestParserTest, characterizeCreateStatementText)
{
    EXPECT_EQ(
        parseAndRecord("CREATE LOGICAL SOURCE s(\n\n  a UINT64 # column\n);\n"),
        (std::vector<std::string>{"CREATE CREATE LOGICAL SOURCE s(\n  a UINT64 \n);"}));
    EXPECT_EQ(
        parseAndRecord("CREATE SINK x(a UINT64) TYPE File\n"), (std::vector<std::string>{"CREATE CREATE SINK x(a UINT64) TYPE File\n"}));
    EXPECT_EQ(
        parseAndRecord("CREATE SINK x(a UINT64) TYPE File; # note\nCREATE SINK y(a UINT64) TYPE File;\n"),
        (std::vector<std::string>{"CREATE CREATE SINK x(a UINT64) TYPE File; \nCREATE SINK y(a UINT64) TYPE File;"}));
}

TEST_F(SystestParserTest, characterizeAttachedData)
{
    const std::string physical = "CREATE PHYSICAL SOURCE FOR s TYPE File;\n";
    EXPECT_EQ(
        parseAndRecord(
            physical
            + "ATTACH INLINE\n1\n   \n# note\n2 # trailing\nCREATE SINK x(a UINT64) TYPE File;\n\nCREATE SINK y(a UINT64) TYPE File;\n"),
        (std::vector<std::string>{
            "CREATE CREATE PHYSICAL SOURCE FOR s TYPE File; ATTACH INLINE ['1', '   ', '2 ', 'CREATE SINK x(a UINT64) TYPE File;']",
            "CREATE CREATE SINK y(a UINT64) TYPE File;"}));
    EXPECT_EQ(
        parseAndRecord(physical + "ATTACH INLINE\n\nCREATE SINK y(a UINT64) TYPE File;\n"),
        (std::vector<std::string>{
            "CREATE CREATE PHYSICAL SOURCE FOR s TYPE File; ATTACH INLINE []", "CREATE CREATE SINK y(a UINT64) TYPE File;"}));
    EXPECT_EQ(
        parseAndRecord(physical + "ATTACH INLINE\n1\n2"),
        (std::vector<std::string>{"CREATE CREATE PHYSICAL SOURCE FOR s TYPE File; ATTACH INLINE ['1', '2']"}));
    EXPECT_EQ(
        parseAndRecord(physical + "ATTACH INLINE"),
        (std::vector<std::string>{"CREATE CREATE PHYSICAL SOURCE FOR s TYPE File; ATTACH INLINE []"}));
    EXPECT_EQ(
        parseAndRecord(physical + "ATTACH FILE  data/s.csv # note\nCREATE SINK y(a UINT64) TYPE File;\n"),
        (std::vector<std::string>{
            "CREATE CREATE PHYSICAL SOURCE FOR s TYPE File; ATTACH FILE [' data/s.csv ']", "CREATE CREATE SINK y(a UINT64) TYPE File;"}));
    EXPECT_EQ(
        parseAndRecordUntilError(physical + "attach inline\n1\n", ErrorCode::SLTUnexpectedToken),
        (std::vector<std::string>{"CREATE CREATE PHYSICAL SOURCE FOR s TYPE File;"}));

    SystestParser parser{};
    std::vector<std::string> events;
    recordCallbacks(parser, events);
    parser.loadString(physical + "ATTACH FILE\n");
    EXPECT_THROW(parser.parse(), std::out_of_range);
}

TEST_F(SystestParserTest, characterizeConfigurationValueSyntax)
{
    const std::vector<std::pair<std::string, std::string>> accepted{
        {"Configuration k: 8", "CONFIGURATION [{k=8}]"},
        {"Configuration   k:   8 # note", "CONFIGURATION [{k=8}]"},
        {"Configuration k: some value", "CONFIGURATION [{k=some value}]"},
        {"Configuration k: [8, 1024]", "CONFIGURATION [{k=8}, {k=1024}]"},
        {"Configuration k: [ 8 ]", "CONFIGURATION [{k=8}]"},
        {"Configuration k: [8,,16]", "CONFIGURATION [{k=8}, {k=16}]"},
        {"Configuration k: [,]", "CONFIGURATION []"},
        {"GlobalConfiguration g: [1, 2]", "GLOBALCONFIGURATION [{g=1}, {g=2}]"}};
    for (const auto& [line, expectedEvent] : accepted)
    {
        SCOPED_TRACE(line);
        EXPECT_EQ(parseAndRecord(line + "\n"), std::vector<std::string>{expectedEvent});
    }

    const std::vector<std::string> rejected{
        "Configuration",
        "Configuration k:8",
        "Configuration k [8]",
        "Configuration : 8",
        "Configuration k:",
        "Configuration k: 8, 16",
        "Configuration k: [8",
        "Configuration k: 8]",
        "Configuration k: []",
        "Configuration k: [8, ,16]",
        "Configuration k: [8] # note",
        "GlobalConfiguration g:1"};
    for (const auto& line : rejected)
    {
        SCOPED_TRACE(line);
        EXPECT_EQ(parseAndRecordUntilError(line + "\n", ErrorCode::SLTUnexpectedToken), std::vector<std::string>{});
    }
}

TEST_F(SystestParserTest, characterizeSubstitutionRules)
{
    SystestParser parser{};
    parser.registerSubstitutionRule({.keyword = "we", .ruleFunction = [](std::string& substitute) { substitute = "R"; }});
    parser.registerSubstitutionRule({.keyword = "CONFIG/", .ruleFunction = [](std::string& substitute) { substitute = "/cfg/"; }});
    parser.registerSubstitutionRule({.keyword = "alpha", .ruleFunction = [](std::string& substitute) { substitute = "beta alpha"; }});
    parser.registerSubstitutionRule({.keyword = "beta", .ruleFunction = [](std::string& substitute) { substitute = "gamma"; }});
    std::vector<std::string> events;
    recordCallbacks(parser, events);
    parser.loadString(
        "SELECT my_we, we_x, wex, WE, we, (we), CONFIG/a, CONFIG/ , alpha FROM s INTO k; # we\n"
        "----\n"
        "we,CONFIG/\n");
    parser.registerSubstitutionRule({.keyword = "INTO", .ruleFunction = [](std::string& substitute) { substitute = "LATE"; }});
    parser.parse();

    EXPECT_EQ(
        events,
        (std::vector<std::string>{
            "QUERY 1 SELECT my_we, we_x, wex, WE, R, (R), CONFIG/a, /cfg/ , gamma alpha FROM s INTO k; ", "RESULT 1 ['R,/cfg/']"}));
}

TEST_F(SystestParserTest, characterizeRegisteredCallbacksChangeWhatIsConsumedAndCounted)
{
    {
        SystestParser parser{};
        std::vector<std::string> queries;
        parser.registerOnQueryCallback([&queries](const std::string& sql, SystestQueryId) { queries.push_back(sql); });
        parser.loadString("SELECT a FROM s INTO k;\n----\n1\n");
        ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
        EXPECT_EQ(queries, std::vector<std::string>{"SELECT a FROM s INTO k;"});
    }
    {
        SystestParser parser{};
        parser.loadString("----\n\nSELECT a FROM s INTO k;\n====\nSELECT b FROM s INTO k;\n");
        EXPECT_NO_THROW(parser.parse());
    }
    {
        SystestParser parser{};
        parser.loadString("EXPLAIN SELECT a FROM s INTO k;\n----\nplan\n==END==\n");
        EXPECT_NO_THROW(parser.parse());
    }
    {
        SystestParser parser{};
        std::vector<std::string> queries;
        parser.registerOnQueryCallback([&queries](const std::string& sql, SystestQueryId) { queries.push_back(sql); });
        parser.registerOnResultTuplesCallback([](std::vector<std::string>&& rows, SystestQueryId) { const auto taken = std::move(rows); });
        parser.loadString("SELECT a FROM s INTO k;\n----\nERROR 1003\n\nSELECT b FROM s INTO k;\n----\n1\n");
        ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
        EXPECT_EQ(queries, std::vector<std::string>{"SELECT a FROM s INTO k;"});
    }
    {
        SystestParser parser{};
        parser.registerOnQueryCallback([](const std::string&, SystestQueryId) { });
        parser.loadString("CREATE SINK x(a UINT64) TYPE File;\n");
        EXPECT_THROW(parser.parse(), std::bad_function_call);
    }
}

}
