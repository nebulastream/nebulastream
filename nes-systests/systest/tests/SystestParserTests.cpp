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
#include <string>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
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
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
}

TEST_F(SystestParserTest, testEmptyLinesAndCommasFile)
{
    SystestParser parser{};
    /// Comment, new line in Unix/Linux, Windows, Older Mac systems
    const std::string str = std::string("#\n") + "\n" + "\r\n" + "\r";

    ASSERT_TRUE(parser.loadString(str));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
}

TEST_F(SystestParserTest, testAttachSourceCallbackSource)
{
    SystestParser parser{};
    const std::string sourceIn = "Source window UINT64 id UINT64 value UINT64 timestamp\n"
                                 "Attach File CSV window INLINE";

    bool isSystestLogicalSourceCallbackCalled = false;
    bool isAttachSourceCallbackCalled = false;

    const std::string str = sourceIn + "\n";

    parser.registerOnQueryCallback([](const std::string&, size_t) { FAIL(); });
    parser.registerOnSystestLogicalSourceCallback([&isSystestLogicalSourceCallbackCalled](SystestParser::SystestLogicalSource&&)
                                                  { isSystestLogicalSourceCallbackCalled = true; });
    parser.registerOnAttachSourceCallback(
        [&isAttachSourceCallbackCalled](SystestAttachSource&& attachSource)
        {
            isAttachSourceCallbackCalled = true;
            /// Not asserting on configurationPath, because we expand 'CONFIG' to the local path on the system executing the test
            ASSERT_EQ(attachSource.logicalSourceName, "window");
            ASSERT_EQ(attachSource.sourceType, "File");
            ASSERT_EQ(attachSource.testDataIngestionType, TestDataIngestionType::INLINE);
        });

    ASSERT_TRUE(parser.loadString(str));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
    ASSERT_TRUE(isSystestLogicalSourceCallbackCalled);
    ASSERT_TRUE(isAttachSourceCallbackCalled);
}

TEST_F(SystestParserTest, testCallbackQuery)
{
    SystestParser parser{};
    static constexpr std::string_view testFileName = "testCallbackQuery";
    const std::string queryIn = "SELECT id, value, timestamp FROM window WHERE value == 1 INTO SINK";
    const std::string delimiter = "----";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    bool queryCallbackCalled = false;

    const std::string testFileString = fmt::format("{}\n{}\n{}\n{}\n", queryIn, delimiter, tpl1, tpl2);

    parser.registerOnQueryCallback(
        [&](const std::string& queryOut, size_t)
        {
            ASSERT_EQ(queryIn, queryOut);
            queryCallbackCalled = true;
        });
    parser.registerOnSystestLogicalSourceCallback([&](SystestParser::SystestLogicalSource&&) { FAIL(); });
    parser.registerOnAttachSourceCallback([&](SystestAttachSource&&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(testFileString));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, testFileName));
    ASSERT_TRUE(queryCallbackCalled);
    /// Check that the queryResult map contains the expected two results for the query defined above
    ASSERT_TRUE(systestStarterGlobals.getQueryResultMap().contains("results/testCallbackQuery_1.csv"));
    ASSERT_EQ(systestStarterGlobals.getQueryResultMap().at("results/testCallbackQuery_1.csv").size(), 2);
    ASSERT_EQ(systestStarterGlobals.getQueryResultMap().at("results/testCallbackQuery_1.csv").at(0), tpl1);
    ASSERT_EQ(systestStarterGlobals.getQueryResultMap().at("results/testCallbackQuery_1.csv").at(1), tpl2);
}

TEST_F(SystestParserTest, testCallbackSystestLogicalSource)
{
    SystestParser parser{};
    const std::string sourceIn = "Source window UINT64 id UINT64 value UINT64 timestamp";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    bool callbackCalled = false;

    const std::string str = sourceIn + "\n" + tpl1 + "\n" + tpl2 + "\n";

    parser.registerOnQueryCallback([&](const std::string&, size_t) { FAIL(); });
    parser.registerOnSystestLogicalSourceCallback(
        [&](SystestParser::SystestLogicalSource&& sourceOut)
        {
            ASSERT_EQ(sourceOut.name, "window");
            ASSERT_EQ(sourceOut.fields[0].type, DataTypeProvider::provideDataType(DataType::Type::UINT64));
            ASSERT_EQ(sourceOut.fields[0].name, "id");
            ASSERT_EQ(sourceOut.fields[1].type, DataTypeProvider::provideDataType(DataType::Type::UINT64));
            ASSERT_EQ(sourceOut.fields[1].name, "value");
            ASSERT_EQ(sourceOut.fields[2].type, DataTypeProvider::provideDataType(DataType::Type::UINT64));
            ASSERT_EQ(sourceOut.fields[2].name, "timestamp");
            callbackCalled = true;
        });
    parser.registerOnAttachSourceCallback([&](SystestAttachSource&&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(str));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
    ASSERT_TRUE(callbackCalled);
}

TEST_F(SystestParserTest, testResultTuplesWithoutQuery)
{
    SystestParser parser{};
    const std::string delimiter = "----";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    const std::string str = delimiter + "\n" + tpl1 + "\n" + tpl2 + "\n";

    parser.registerOnQueryCallback([&](const std::string&, size_t) { FAIL(); });
    parser.registerOnSystestLogicalSourceCallback([&](SystestParser::SystestLogicalSource&&) { FAIL(); });
    parser.registerOnAttachSourceCallback([&](SystestAttachSource&&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(str));
    SystestStarterGlobals systestStarterGlobals{};
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(systestStarterGlobals, {}); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserTest, testSubstitutionRule)
{
    SystestParser parser{};
    const std::string queryIn = "SELECT id, value, timestamp FROM window WHERE value == 1 INTO SINK";
    const std::string delim = "----";
    const std::string result = "1 1 1";

    std::string queryExpect = "SELECT id, value, timestamp FROM window WHERE value == 1 INTO TestSink()";

    bool callbackCalled = false;

    const std::string str = queryIn + "\n" + delim + "\n" + result + "\n";

    const SystestParser::SubstitutionRule rule{.keyword = "SINK", .ruleFunction = [](std::string& input) { input = "TestSink()"; }};
    parser.registerSubstitutionRule(rule);

    const SystestParser::QueryCallback callback = [&queryExpect, &callbackCalled](const std::string& query, size_t)
    {
        ASSERT_EQ(queryExpect, query);
        callbackCalled = true;
    };
    parser.registerOnQueryCallback(callback);

    ASSERT_TRUE(parser.loadString(str));
    SystestStarterGlobals systestStarterGlobals{};
    EXPECT_NO_THROW(parser.parse(systestStarterGlobals, {}));
    ASSERT_TRUE(callbackCalled);
}

TEST_F(SystestParserTest, testRegisterSubstitutionKeywordTwoTimes)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const SystestParser::SubstitutionRule rule1{.keyword = "SINK", .ruleFunction = [](std::string& input) { input = "TestSink()"; }};
    const SystestParser::SubstitutionRule rule2{.keyword = "SINK", .ruleFunction = [](std::string& input) { input = "AnotherTestSink()"; }};

    SystestParser parser{};
    parser.registerSubstitutionRule(rule1);
    ASSERT_DEATH_DEBUG(parser.registerSubstitutionRule(rule2), "");
}

}
