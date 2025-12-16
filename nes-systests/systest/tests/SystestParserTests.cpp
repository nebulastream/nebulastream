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

#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <SystestState.hpp>

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

    ASSERT_EQ(true, parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
}

TEST_F(SystestParserTest, testEmptyLinesAndCommasFile)
{
    SystestParser parser{};
    /// Comment, new line in Unix/Linux, Windows, Older Mac systems
    const std::string str = std::string("#\n") + "\n" + "\r\n" + "\r";

    ASSERT_TRUE(parser.loadString(str));
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

    ASSERT_TRUE(parser.loadString(str));
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
        [&](const std::string& queryOut, SystestQueryId, bool)
        {
            ASSERT_EQ(queryIn, queryOut);
            queryCallbackCalled = true;
        });
    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { FAIL(); });
    parser.registerOnResultTuplesCallback([&](std::vector<std::string>&& resultTuples, const SystestQueryId)
                                          { receivedResultTuples = std::move(resultTuples); });

    ASSERT_TRUE(parser.loadString(testFileString));
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

    parser.registerOnQueryCallback([&](const std::string&, SystestQueryId, bool) { FAIL(); });
    parser.registerOnResultTuplesCallback(
        [&](const std::vector<std::string>&, const SystestQueryId)
        {
            /// nop
        });
    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(str));
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserTest, testDifferentialQueryCallbackFromFile)
{
    SystestParser parser{};

    const std::string expectedMainQuery = "SELECT id * UINT32(10) AS id, value, timestamp FROM stream INTO streamSink;";
    const std::string expectedDifferentialQuery = "SELECT id * UINT32(2) * UINT32(5) AS id, value, timestamp FROM stream INTO streamSink;";

    bool mainQueryCallbackCalled = false;
    bool differentialQueryCallbackCalled = false;

    parser.registerOnQueryCallback(
        [&](const std::string& queryOut, SystestQueryId, bool)
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
    ASSERT_TRUE(parser.loadFile(Filename)) << "Failed to load file: " << Filename;

    EXPECT_NO_THROW(parser.parse());

    ASSERT_TRUE(mainQueryCallbackCalled) << "The main query callback was never called.";
    ASSERT_TRUE(differentialQueryCallbackCalled) << "The differential query callback was never called.";
}

TEST_F(SystestParserTest, testDifferentialQueryCallbackInlineSyntax)
{
    SystestParser parser{};

    const std::string expectedMainQuery = "SELECT id * UINT32(10) AS id, value, timestamp FROM stream INTO streamSink;";
    const std::string expectedDifferentialQuery = "SELECT id * UINT32(2) * UINT32(5) AS id, value, timestamp FROM stream INTO streamSink;";

    bool mainQueryCallbackCalled = false;
    bool differentialQueryCallbackCalled = false;

    parser.registerOnQueryCallback(
        [&](const std::string& queryOut, SystestQueryId, bool)
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

SELECT id * UINT32(10) AS id, value, timestamp FROM stream INTO streamSink;
====
SELECT id * UINT32(2) * UINT32(5) AS id, value, timestamp FROM stream INTO streamSink;
)";

    ASSERT_TRUE(parser.loadString(std::string(TestContent)));

    EXPECT_NO_THROW(parser.parse());

    ASSERT_TRUE(mainQueryCallbackCalled) << "The main query callback was never called.";
    ASSERT_TRUE(differentialQueryCallbackCalled) << "The differential query callback was never called.";
}

}
