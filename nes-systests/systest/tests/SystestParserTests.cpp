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
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>

namespace NES::Systest
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

    ASSERT_TRUE(parser.loadString(str));
    ParsedTestFile parsed;
    ASSERT_NO_THROW(parsed = parser.parse());
    EXPECT_TRUE(parsed.file.empty());
    EXPECT_TRUE(parsed.relativeTestFile.empty());
    EXPECT_TRUE(parsed.fixtures.empty());
    EXPECT_TRUE(parsed.cases.empty());
}

TEST_F(SystestParserTest, testEmptyLinesAndCommasFile)
{
    SystestParser parser{};
    const std::string str = std::string("#\n") + "\n" + "\r\n" + "\r";

    ASSERT_TRUE(parser.loadString(str));
    ParsedTestFile parsed;
    ASSERT_NO_THROW(parsed = parser.parse());
    EXPECT_TRUE(parsed.fixtures.empty());
    EXPECT_TRUE(parsed.cases.empty());
}

TEST_F(SystestParserTest, testCreateStatementsWithoutAttachments)
{
    SystestParser parser{};
    const std::string testFileString = "CREATE LOGICAL SOURCE window(id UINT64, value UINT64, timestamp UINT64 timestamp);\n"
                                       "CREATE PHYSICAL SOURCE FOR window TYPE File\n";

    ASSERT_TRUE(parser.loadString(testFileString));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.fixtures.size(), 2);
    EXPECT_EQ(parsed.fixtures[0].sql, "CREATE LOGICAL SOURCE window(id UINT64, value UINT64, timestamp UINT64 timestamp);");
    EXPECT_FALSE(parsed.fixtures[0].attachment.has_value());
    EXPECT_EQ(parsed.fixtures[0].source, (Origin{.file = {}, .firstLine = 1, .lastLine = 1}));
    EXPECT_EQ(parsed.fixtures[1].sql, "CREATE PHYSICAL SOURCE FOR window TYPE File\n");
    EXPECT_FALSE(parsed.fixtures[1].attachment.has_value());
    EXPECT_EQ(parsed.fixtures[1].source, (Origin{.file = {}, .firstLine = 2, .lastLine = 2}));
    EXPECT_TRUE(parsed.cases.empty());
}

TEST_F(SystestParserTest, testQueryAndResultTuples)
{
    SystestParser parser{};

    const std::string query = "SELECT id, value, timestamp FROM window WHERE value == 1 INTO SINK;";
    const std::string firstTuple = "1,1,1";
    const std::string secondTuple = "2,2,2";
    const std::string testFileString = query + "\n----\n" + firstTuple + "\n" + secondTuple + "\n";

    ASSERT_TRUE(parser.loadString(testFileString));
    const auto parsed = parser.parse();

    EXPECT_TRUE(parsed.fixtures.empty());
    ASSERT_EQ(parsed.cases.size(), 1);
    const auto& parsedCase = parsed.cases.front();
    EXPECT_EQ(parsedCase.key.queryNumber, SystestQueryId{1});
    EXPECT_EQ(parsedCase.source, (Origin{.file = {}, .firstLine = 1, .lastLine = 4}));
    ASSERT_TRUE(std::holds_alternative<QueryAction>(parsedCase.action));
    EXPECT_EQ(std::get<QueryAction>(parsedCase.action), (QueryAction{.sql = query, .kind = QueryKind::Execute}));
    ASSERT_TRUE(std::holds_alternative<RowsExpectation>(parsedCase.expectation));
    EXPECT_EQ(std::get<RowsExpectation>(parsedCase.expectation).rows, (std::vector<std::string>{firstTuple, secondTuple}));
}

TEST_F(SystestParserTest, testResultTuplesWithoutQuery)
{
    SystestParser parser{};
    const std::string testFileString = "----\n1,1,1\n2,2,2\n";

    ASSERT_TRUE(parser.loadString(testFileString));
    ASSERT_EXCEPTION_ERRORCODE({ (void)parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserTest, testQueryWithoutResultDelimiter)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadString("SELECT 1 INTO File();\n"));

    ASSERT_EXCEPTION_ERRORCODE({ (void)parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserTest, testOversizedNumericErrorCode)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadString("SELECT 1 INTO File();\n----\nERROR 999999999999999999999999999999999999999999999999\n"));

    ASSERT_EXCEPTION_ERRORCODE({ (void)parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserTest, testDifferentialQueryFromFile)
{
    SystestParser parser{};
    const std::string expectedMainQuery = "SELECT id * UINT32(10) AS id, value, timestamp FROM stream INTO streamSink;";
    const std::string expectedDifferentialQuery = "SELECT id * UINT32(2) * UINT32(5) AS id, value, timestamp FROM stream INTO streamSink;";

    static constexpr std::string_view Filename = SYSTEST_DATA_DIR "differential.dummy";
    ASSERT_TRUE(parser.loadFile(Filename)) << "Failed to load file: " << Filename;
    const auto parsed = parser.parse();

    EXPECT_EQ(parsed.file, std::filesystem::weakly_canonical(Filename));
    EXPECT_EQ(parsed.relativeTestFile, "differential.dummy");
    ASSERT_EQ(parsed.fixtures.size(), 3);
    ASSERT_TRUE(parsed.fixtures[1].attachment.has_value());
    ASSERT_TRUE(std::holds_alternative<InlineSourceData>(*parsed.fixtures[1].attachment));
    EXPECT_EQ(
        std::get<InlineSourceData>(*parsed.fixtures[1].attachment).rows,
        (std::vector<std::string>{"5,1,1000", "10,1,1001", "15,1,1002", "20,2,2000", "25,19,19000", "30,20,20000", "35,21,21000"}));
    ASSERT_EQ(parsed.cases.size(), 1);
    const auto& parsedCase = parsed.cases.front();
    EXPECT_EQ(parsedCase.key, (CaseKey{.relativeTestFile = "differential.dummy", .queryNumber = SystestQueryId{1}}));
    EXPECT_EQ(parsedCase.source, (Origin{.file = std::filesystem::weakly_canonical(Filename), .firstLine = 22, .lastLine = 24}));
    ASSERT_TRUE(std::holds_alternative<DifferentialAction>(parsedCase.action));
    EXPECT_EQ(
        std::get<DifferentialAction>(parsedCase.action),
        (DifferentialAction{.leftSql = expectedMainQuery, .rightSql = expectedDifferentialQuery}));
    EXPECT_TRUE(std::holds_alternative<DifferentialExpectation>(parsedCase.expectation));
}

TEST_F(SystestParserTest, testDifferentialQueryInlineSyntax)
{
    SystestParser parser{};
    const std::string expectedMainQuery = "SELECT id * UINT32(10) AS id, value, timestamp FROM stream INTO streamSink;";
    const std::string expectedDifferentialQuery = "SELECT id * UINT32(2) * UINT32(5) AS id, value, timestamp FROM stream INTO streamSink;";

    static constexpr std::string_view TestContent = R"(
CREATE LOGICAL SOURCE stream(id INT64, value INT64, timestamp INT64);
CREATE PHYSICAL SOURCE FOR stream TYPE File;
ATTACH INLINE
5,1,1000

CREATE SINK streamSink(id INT64, stream.value INT64, stream.timestamp INT64) TYPE File;

SELECT id * UINT32(10) AS id, value, timestamp FROM stream INTO streamSink;
====
SELECT id * UINT32(2) * UINT32(5) AS id, value, timestamp FROM stream INTO streamSink;
)";

    ASSERT_TRUE(parser.loadString(std::string(TestContent)));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.fixtures.size(), 3);
    ASSERT_TRUE(parsed.fixtures[1].attachment.has_value());
    EXPECT_EQ(std::get<InlineSourceData>(*parsed.fixtures[1].attachment).rows, (std::vector<std::string>{"5,1,1000"}));
    ASSERT_EQ(parsed.cases.size(), 1);
    ASSERT_TRUE(std::holds_alternative<DifferentialAction>(parsed.cases.front().action));
    EXPECT_EQ(
        std::get<DifferentialAction>(parsed.cases.front().action),
        (DifferentialAction{.leftSql = expectedMainQuery, .rightSql = expectedDifferentialQuery}));
    EXPECT_TRUE(std::holds_alternative<DifferentialExpectation>(parsed.cases.front().expectation));
}

TEST_F(SystestParserTest, testEmptyDifferentialQueryIsRejected)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadString("SELECT 1 INTO File();\n====\n----\n"));

    ASSERT_EXCEPTION_ERRORCODE({ (void)parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserTest, testExplainWithVerbatimResultBlock)
{
    SystestParser parser{};
    const std::string explain = "EXPLAIN (OPTIMIZED, FORMAT TEXT) SELECT id FROM stream WHERE value > UINT64(4) INTO sink;";
    const std::string query = "SELECT id FROM stream INTO sink;";
    static constexpr std::string_view TestContent
        = R"(EXPLAIN (OPTIMIZED, FORMAT TEXT) SELECT id FROM stream WHERE value > UINT64(4) INTO sink;
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

    ASSERT_TRUE(parser.loadString(std::string(TestContent)));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.cases.size(), 2);
    EXPECT_EQ(parsed.cases[0].key.queryNumber, SystestQueryId{1});
    EXPECT_EQ(parsed.cases[1].key.queryNumber, SystestQueryId{2});
    EXPECT_EQ(parsed.cases[0].source, (Origin{.file = {}, .firstLine = 1, .lastLine = 7}));
    ASSERT_TRUE(std::holds_alternative<QueryAction>(parsed.cases[0].action));
    EXPECT_EQ(std::get<QueryAction>(parsed.cases[0].action), (QueryAction{.sql = explain, .kind = QueryKind::Explain}));
    ASSERT_TRUE(std::holds_alternative<TextExpectation>(parsed.cases[0].expectation));
    EXPECT_EQ(
        std::get<TextExpectation>(parsed.cases[0].expectation).lines,
        (std::vector<std::string>{
            "== Global Optimized Plan ==", "SINK(SINK1)", "  SELECTION(predicate: value > 4)", "    SOURCE(stream)"}));
    EXPECT_EQ(std::get<TextExpectation>(parsed.cases[0].expectation).matching, TextMatchPolicy::Automatic);
    EXPECT_EQ(std::get<QueryAction>(parsed.cases[1].action), (QueryAction{.sql = query, .kind = QueryKind::Execute}));
    ASSERT_TRUE(std::holds_alternative<RowsExpectation>(parsed.cases[1].expectation));
    EXPECT_EQ(std::get<RowsExpectation>(parsed.cases[1].expectation).rows, (std::vector<std::string>{"1"}));
}

TEST_F(SystestParserTest, testExplainWithErrorExpectation)
{
    SystestParser parser{};
    static constexpr std::string_view TestContent = R"(EXPLAIN (LOGICAL) SELECT id FROM unknownStream INTO sink;
----
ERROR 3000
)";

    ASSERT_TRUE(parser.loadString(std::string(TestContent)));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.cases.size(), 1);
    ASSERT_TRUE(std::holds_alternative<QueryAction>(parsed.cases.front().action));
    EXPECT_EQ(std::get<QueryAction>(parsed.cases.front().action).kind, QueryKind::Explain);
    ASSERT_TRUE(std::holds_alternative<ErrorExpectation>(parsed.cases.front().expectation));
    const auto& expectation = std::get<ErrorExpectation>(parsed.cases.front().expectation);
    EXPECT_EQ(expectation.code, ErrorCode::BufferAllocationFailure);
    EXPECT_FALSE(expectation.message.has_value());
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

    ASSERT_TRUE(parser.loadString(std::string(TestContent)));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.cases.size(), 1);
    ASSERT_TRUE(std::holds_alternative<QueryAction>(parsed.cases.front().action));
    EXPECT_EQ(
        std::get<QueryAction>(parsed.cases.front().action),
        (QueryAction{.sql = "EXPLAIN (ALL, FORMAT TEXT)\nSELECT id FROM stream\nINTO sink;", .kind = QueryKind::Explain}));
    ASSERT_TRUE(std::holds_alternative<TextExpectation>(parsed.cases.front().expectation));
    EXPECT_EQ(
        std::get<TextExpectation>(parsed.cases.front().expectation).lines,
        (std::vector<std::string>{"== Initial Logical Plan ==", "SINK(SINK1)"}));
}

TEST_F(SystestParserTest, testSubstitutionRuleRespectsWordBoundaries)
{
    SystestParser parser{};
    parser.registerSubstitutionRule({.keyword = "we", .ruleFunction = [](std::string& substitute) { substitute = "REPLACED"; }});
    static constexpr std::string_view TestContent = R"(
SELECT producedPower, timestamp FROM source INTO we;
----
100,1000
)";

    ASSERT_TRUE(parser.loadString(std::string(TestContent)));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.cases.size(), 1);
    ASSERT_TRUE(std::holds_alternative<QueryAction>(parsed.cases.front().action));
    const auto& receivedQuery = std::get<QueryAction>(parsed.cases.front().action).sql;
    EXPECT_NE(receivedQuery.find("producedPower"), std::string::npos);
    EXPECT_NE(receivedQuery.find("INTO REPLACED"), std::string::npos);
    EXPECT_EQ(receivedQuery.find("INTO we"), std::string::npos);
}

TEST_F(SystestParserTest, testPunctuationTerminatedSubstitutionRule)
{
    SystestParser parser{};
    parser.registerSubstitutionRule(
        {.keyword = "CONFIG/", .ruleFunction = [](std::string& substitute) { substitute = "/resolved/config/"; }});
    static constexpr std::string_view TestContent = R"(
CREATE MODEL model TYPE ONNX FROM 'CONFIG/models/model.onnx';
CREATE MODEL untouched TYPE ONNX FROM 'MYCONFIG/models/model.onnx';
)";

    ASSERT_TRUE(parser.loadString(std::string(TestContent)));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.fixtures.size(), 2);
    EXPECT_EQ(parsed.fixtures[0].sql, "CREATE MODEL model TYPE ONNX FROM '/resolved/config/models/model.onnx';");
    EXPECT_EQ(parsed.fixtures[1].sql, "CREATE MODEL untouched TYPE ONNX FROM 'MYCONFIG/models/model.onnx';");
}

TEST_F(SystestParserTest, testWhitespaceOnlyLineTerminatesInlineAttachment)
{
    SystestParser parser{};
    const std::string testContent = "CREATE LOGICAL SOURCE input(value UINT64);\n"
                                    "CREATE PHYSICAL SOURCE FOR input TYPE File;\n"
                                    "ATTACH INLINE\n"
                                    "\x20\x20value with spaces\x20\x20\n"
                                    "\x20\x20\x20\t\n"
                                    "SELECT value FROM input INTO File();\n"
                                    "----\n"
                                    "1\n";

    ASSERT_TRUE(parser.loadString(testContent));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.fixtures.size(), 2);
    ASSERT_TRUE(parsed.fixtures[1].attachment.has_value());
    ASSERT_TRUE(std::holds_alternative<InlineSourceData>(*parsed.fixtures[1].attachment));
    EXPECT_EQ(std::get<InlineSourceData>(*parsed.fixtures[1].attachment).rows, (std::vector<std::string>{"  value with spaces  "}));
    ASSERT_EQ(parsed.cases.size(), 1);
}

TEST_F(SystestParserTest, testAttachmentsConfigurationsDependenciesAndPhysicalLines)
{
    SystestParser parser{};
    static constexpr std::string_view TestContent = R"(# ignored
GLOBALCONFIGURATION worker.mode: [A, B]
CREATE LOGICAL SOURCE input(id UINT64);
CREATE PHYSICAL SOURCE FOR input TYPE File;
ATTACH FILE input.csv
CONFIGURATION worker.size: 8
SELECT id FROM input INTO sink;
----
1

SEQUENTIAL_EXECUTION
SELECT id FROM input INTO sink;
----
2
)";

    ASSERT_TRUE(parser.loadString(std::string(TestContent)));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.fixtures.size(), 2);
    EXPECT_EQ(parsed.fixtures[0].source, (Origin{.file = {}, .firstLine = 3, .lastLine = 3}));
    EXPECT_EQ(parsed.fixtures[1].source, (Origin{.file = {}, .firstLine = 4, .lastLine = 5}));
    ASSERT_TRUE(parsed.fixtures[1].attachment.has_value());
    ASSERT_TRUE(std::holds_alternative<FileSourceData>(*parsed.fixtures[1].attachment));
    EXPECT_EQ(std::get<FileSourceData>(*parsed.fixtures[1].attachment).file, "input.csv");

    ASSERT_EQ(parsed.cases.size(), 2);
    EXPECT_EQ(parsed.cases[0].source, (Origin{.file = {}, .firstLine = 7, .lastLine = 9}));
    EXPECT_EQ(parsed.cases[1].source, (Origin{.file = {}, .firstLine = 12, .lastLine = 14}));
    ASSERT_EQ(parsed.cases[0].configuration.size(), 2);
    const auto& globalConfiguration = parsed.cases[0].configuration[0];
    EXPECT_EQ(globalConfiguration.key, "worker.mode");
    EXPECT_EQ(globalConfiguration.values, (std::vector<std::string>{"A", "B"}));
    EXPECT_TRUE(globalConfiguration.global);
    EXPECT_EQ(globalConfiguration.source, (Origin{.file = {}, .firstLine = 2, .lastLine = 2}));
    const auto& localConfiguration = parsed.cases[0].configuration[1];
    EXPECT_EQ(localConfiguration.key, "worker.size");
    EXPECT_EQ(localConfiguration.values, (std::vector<std::string>{"8"}));
    EXPECT_FALSE(localConfiguration.global);
    EXPECT_EQ(localConfiguration.source, (Origin{.file = {}, .firstLine = 6, .lastLine = 6}));
    ASSERT_EQ(parsed.cases[1].configuration.size(), 1);
    EXPECT_EQ(parsed.cases[1].configuration.front().key, globalConfiguration.key);
    EXPECT_EQ(parsed.cases[1].configuration.front().values, globalConfiguration.values);
    EXPECT_EQ(parsed.cases[1].configuration.front().global, globalConfiguration.global);
    EXPECT_EQ(parsed.cases[1].configuration.front().source, globalConfiguration.source);
    EXPECT_FALSE(parsed.cases[0].runAfter.has_value());
    ASSERT_TRUE(parsed.cases[1].runAfter.has_value());
    EXPECT_EQ(*parsed.cases[1].runAfter, (CaseKey{.relativeTestFile = {}, .queryNumber = parsed.cases[0].key.queryNumber}));
}

}
