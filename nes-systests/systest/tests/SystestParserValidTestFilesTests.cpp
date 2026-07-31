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
#include <filesystem>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SystestConfiguration.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{
namespace
{

const std::vector<std::string>& standardInlineData()
{
    static const std::vector<std::string> data{"1,1,1000",   "12,1,1001",  "4,1,1002",   "1,2,2000",   "11,2,2001",  "16,2,2002",
                                               "1,3,3000",   "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",
                                               "1,6,6000",   "1,7,7000",   "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000",
                                               "1,12,12000", "1,13,13000", "1,14,14000", "1,15,15000", "1,16,16000", "1,17,17000",
                                               "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"};
    return data;
}

const std::vector<std::string>& standardQueries()
{
    static const std::vector<std::string> queries{
        "SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow;",
        "SELECT * FROM window WHERE id >= UINT64(10) INTO sinkWindow;",
        "SELECT * FROM window WHERE timestamp <= UINT64(10000) INTO sinkWindow;",
        "SELECT * FROM window WHERE timestamp >= UINT64(5000) AND timestamp <= UINT64(15000) INTO sinkWindow;",
        "SELECT * FROM window WHERE value != UINT64(1) INTO sinkWindow;"};
    return queries;
}

const std::vector<std::vector<std::string>>& standardResults()
{
    static const std::vector<std::vector<std::string>> results{
        {"1,1,1000", "12,1,1001", "4,1,1002"},
        {"12,1,1001", "11,2,2001", "16,2,2002", "11,3,3001"},
        {"1,1,1000",
         "12,1,1001",
         "4,1,1002",
         "1,2,2000",
         "11,2,2001",
         "16,2,2002",
         "1,3,3000",
         "11,3,3001",
         "1,3,3003",
         "1,3,3200",
         "1,4,4000",
         "1,5,5000",
         "1,6,6000",
         "1,7,7000",
         "1,8,8000",
         "1,9,9000",
         "1,10,10000"},
        {"1,5,5000",
         "1,6,6000",
         "1,7,7000",
         "1,8,8000",
         "1,9,9000",
         "1,10,10000",
         "1,11,11000",
         "1,12,12000",
         "1,13,13000",
         "1,14,14000",
         "1,15,15000"},
        {"1,2,2000",   "11,2,2001",  "16,2,2002",  "1,3,3000",   "11,3,3001",  "1,3,3003",   "1,3,3200",   "1,4,4000",   "1,5,5000",
         "1,6,6000",   "1,7,7000",   "1,8,8000",   "1,9,9000",   "1,10,10000", "1,11,11000", "1,12,12000", "1,13,13000", "1,14,14000",
         "1,15,15000", "1,16,16000", "1,17,17000", "1,18,18000", "1,19,19000", "1,20,20000", "1,21,21000"}};
    return results;
}

void expectCases(
    const ParsedTestFile& parsed,
    const std::vector<std::string>& expectedQueries,
    const std::vector<std::vector<std::string>>& expectedResults)
{
    ASSERT_EQ(parsed.cases.size(), expectedQueries.size());
    ASSERT_EQ(parsed.cases.size(), expectedResults.size());
    for (size_t index = 0; index < parsed.cases.size(); ++index)
    {
        SCOPED_TRACE(index);
        const auto& parsedCase = parsed.cases[index];
        EXPECT_EQ(parsedCase.key.queryNumber, SystestQueryId{index + 1});
        ASSERT_TRUE(std::holds_alternative<QueryAction>(parsedCase.action));
        const auto& action = std::get<QueryAction>(parsedCase.action);
        EXPECT_EQ(action.sql, expectedQueries[index]);
        EXPECT_EQ(action.kind, QueryKind::Execute);
        ASSERT_TRUE(std::holds_alternative<RowsExpectation>(parsedCase.expectation));
        const auto& expectation = std::get<RowsExpectation>(parsedCase.expectation);
        EXPECT_EQ(expectation.rows, expectedResults[index]);
        EXPECT_EQ(expectation.comparison, ComparisonPolicy::UnorderedTypedRows);
    }
}

void expectStandardFixtures(const ParsedTestFile& parsed)
{
    ASSERT_EQ(parsed.fixtures.size(), 3);
    EXPECT_TRUE(parsed.fixtures[0].sql.starts_with("CREATE LOGICAL SOURCE window"));
    EXPECT_FALSE(parsed.fixtures[0].attachment.has_value());
    EXPECT_TRUE(parsed.fixtures[1].sql.starts_with("CREATE PHYSICAL SOURCE FOR window"));
    ASSERT_TRUE(parsed.fixtures[1].attachment.has_value());
    ASSERT_TRUE(std::holds_alternative<InlineSourceData>(*parsed.fixtures[1].attachment));
    EXPECT_EQ(std::get<InlineSourceData>(*parsed.fixtures[1].attachment).rows, standardInlineData());
    EXPECT_TRUE(parsed.fixtures[2].sql.starts_with("CREATE SINK sinkWindow"));
    EXPECT_FALSE(parsed.fixtures[2].attachment.has_value());
}

}

class SystestParserValidTestFileTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestParserValidTestFileTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestParserValidTestFileTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestParserValidTestFileTest test class."); }
};

TEST_F(SystestParserValidTestFileTest, ValidTestFile)
{
    SystestParser parser{};
    static constexpr std::string_view Filename = SYSTEST_DATA_DIR "valid.dummy";
    ASSERT_TRUE(parser.loadFile(Filename)) << "Failed to load file: " << Filename;
    const auto parsed = parser.parse();

    EXPECT_EQ(parsed.file, std::filesystem::weakly_canonical(Filename));
    EXPECT_EQ(parsed.relativeTestFile, "valid.dummy");
    ASSERT_EQ(parsed.fixtures.size(), 4);
    EXPECT_TRUE(parsed.fixtures[0].sql.starts_with("CREATE LOGICAL SOURCE e123"));
    EXPECT_FALSE(parsed.fixtures[0].attachment.has_value());
    ASSERT_TRUE(parsed.fixtures[1].attachment.has_value());
    ASSERT_TRUE(std::holds_alternative<InlineSourceData>(*parsed.fixtures[1].attachment));
    EXPECT_EQ(std::get<InlineSourceData>(*parsed.fixtures[1].attachment).rows, (std::vector<std::string>{"1", "1", "1"}));
    EXPECT_TRUE(parsed.fixtures[2].sql.starts_with("CREATE LOGICAL SOURCE e124"));
    EXPECT_FALSE(parsed.fixtures[2].attachment.has_value());
    ASSERT_TRUE(parsed.fixtures[3].attachment.has_value());
    ASSERT_TRUE(std::holds_alternative<InlineSourceData>(*parsed.fixtures[3].attachment));
    EXPECT_TRUE(std::get<InlineSourceData>(*parsed.fixtures[3].attachment).rows.empty());

    expectCases(
        parsed,
        {"SELECT * FROM\ne123 WHERE id >= UINT32(10) INTO sink;", "SELECT * FROM e124 WHERE i >= INT8(10) INTO sink;"},
        {{"1,1,1", "1,1,1", "1,1,1"}, {"2,2,2", "2,2,2", "2,2,2"}});
}

TEST_F(SystestParserValidTestFileTest, Nullable1TestFile)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(SYSTEST_DATA_DIR "nullable.dummy"));
    const auto parsed = parser.parse();

    expectStandardFixtures(parsed);
    expectCases(parsed, standardQueries(), standardResults());
}

TEST_F(SystestParserValidTestFileTest, Comments1TestFile)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(SYSTEST_DATA_DIR "comments.dummy"));
    const auto parsed = parser.parse();

    expectStandardFixtures(parsed);
    expectCases(parsed, standardQueries(), standardResults());
}

TEST_F(SystestParserValidTestFileTest, FilterTestFile)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(SYSTEST_DATA_DIR "filter.dummy"));
    const auto parsed = parser.parse();

    expectStandardFixtures(parsed);
    auto expectedQueries = standardQueries();
    expectedQueries.push_back("SELECT * FROM window WHERE id = id - UINT64(1) INTO sinkWindow;");
    auto expectedResults = standardResults();
    expectedResults.push_back({"0,1,1000",   "11,1,1001",  "3,1,1002",   "0,2,2000",   "10,2,2001",  "15,2,2002",  "10,3,3000",
                               "10,3,3001",  "0,3,3003",   "0,3,3200",   "0,4,4000",   "0,5,5000",   "0,6,6000",   "0,7,7000",
                               "0,8,8000",   "0,9,9000",   "0,10,10000", "0,11,11000", "0,12,12000", "0,13,13000", "0,14,14000",
                               "0,15,15000", "0,16,16000", "0,17,17000", "0,18,18000", "0,19,19000", "0,20,20000", "0,21,21000"});
    expectCases(parsed, expectedQueries, expectedResults);
}

TEST_F(SystestParserValidTestFileTest, ErrorExpectationTest)
{
    SystestParser parser{};
    static constexpr std::string_view Filename = SYSTEST_DATA_DIR "error_expectation.dummy";
    ASSERT_TRUE(parser.loadFile(Filename));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.fixtures.size(), 3);
    ASSERT_EQ(parsed.cases.size(), 1);
    const auto& parsedCase = parsed.cases.front();
    ASSERT_TRUE(std::holds_alternative<QueryAction>(parsedCase.action));
    EXPECT_EQ(std::get<QueryAction>(parsedCase.action).sql, "SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow;");
    ASSERT_TRUE(std::holds_alternative<ErrorExpectation>(parsedCase.expectation));
    const auto& expectation = std::get<ErrorExpectation>(parsedCase.expectation);
    EXPECT_EQ(expectation.code, ErrorCode::SLTWrongSchema);
    EXPECT_EQ(expectation.message, "expected error message");
    EXPECT_EQ(parsedCase.source, (Origin{.file = std::filesystem::weakly_canonical(Filename), .firstLine = 14, .lastLine = 16}));
}

TEST_F(SystestParserValidTestFileTest, CreateStatementFormat)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(SYSTEST_DATA_DIR "create_statement_format.dummy"));
    const auto parsed = parser.parse();

    ASSERT_EQ(parsed.fixtures.size(), 7);
    EXPECT_EQ(parsed.fixtures[0].sql, "CREATE LOGICAL SOURCE input1(id UINT64);");
    EXPECT_EQ(parsed.fixtures[1].sql, "CREATE PHYSICAL SOURCE FOR input1 TYPE File;");
    EXPECT_EQ(parsed.fixtures[2].sql, "CREATE LOGICAL SOURCE input2(id UINT64);");
    EXPECT_EQ(parsed.fixtures[3].sql, "CREATE PHYSICAL SOURCE FOR input2 TYPE File;");
    EXPECT_EQ(parsed.fixtures[4].sql, "CREATE LOGICAL SOURCE\n  input3(\n    id UINT64\n  );");
    EXPECT_EQ(parsed.fixtures[5].sql, "CREATE PHYSICAL SOURCE\n FOR input3\n TYPE File\n;");
    EXPECT_EQ(parsed.fixtures[6].sql, "CREATE SINK output(id UINT64) TYPE File;");
    const std::vector<std::string> expectedData{"1", "2", "3"};
    for (const size_t index : {size_t{1}, size_t{3}, size_t{5}})
    {
        SCOPED_TRACE(index);
        ASSERT_TRUE(parsed.fixtures[index].attachment.has_value());
        ASSERT_TRUE(std::holds_alternative<InlineSourceData>(*parsed.fixtures[index].attachment));
        EXPECT_EQ(std::get<InlineSourceData>(*parsed.fixtures[index].attachment).rows, expectedData);
    }
    EXPECT_FALSE(parsed.fixtures[0].attachment.has_value());
    EXPECT_FALSE(parsed.fixtures[2].attachment.has_value());
    EXPECT_FALSE(parsed.fixtures[4].attachment.has_value());
    EXPECT_FALSE(parsed.fixtures[6].attachment.has_value());

    expectCases(
        parsed,
        {"SELECT id AS id FROM input1 INTO output;",
         "SELECT id AS id FROM input2 INTO output;",
         "SELECT id AS id FROM input3 INTO output;"},
        {expectedData, expectedData, expectedData});
}

TEST_F(SystestParserValidTestFileTest, TextAfterClosingBracketOfGroups)
{
    SystestConfiguration config{};
    config.testsDiscoverDir.setValue(SYSTEST_DATA_DIR);
    const auto testFileName = fmt::format("comment_text_bracket{}", ".dummy");
    config.directlySpecifiedTestFiles.setValue(fmt::format("{}/{}", SYSTEST_DATA_DIR, testFileName));
    const auto testMap = Systest::loadTestFileMap(config);
    ASSERT_EQ(testMap.size(), 1);
    const auto testFile = testMap.begin()->second;
    const std::vector<std::string> expectedGroups{"Aggregation", "WindowOperators", "CompilationIntensive"};
    EXPECT_EQ(testFile.groups, expectedGroups);
}

}
