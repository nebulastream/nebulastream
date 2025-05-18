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

#include <memory>
#include <string>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>
#include <Common/DataTypes/BasicTypes.hpp>
#include <Common/DataTypes/DataTypeProvider.hpp>

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
    parser.parse();
}

TEST_F(SystestParserTest, testEmptyLinesAndCommasFile)
{
    SystestParser parser{};
    /// Comment, new line in Unix/Linux, Windows, Older Mac systems
    const std::string str = std::string("#\n") + "\n" + "\r\n" + "\r";

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
}

TEST_F(SystestParserTest, testCallbackSourceCSV)
{
    SystestParser parser{};
    const std::string sourceIn = "Source window UINT64 id UINT64 value UINT64 timestamp\n"
                                 "Attach File InlineData CONFIG/sources/file_inline_default.yaml window";

    bool isSLTSourceCallbackCalled = false;
    bool isAttachSourceCallbackCalled = false;

    const std::string str = sourceIn + "\n";

    parser.registerOnQueryCallback([](SystestParser::Query&&) { FAIL(); });
    parser.registerOnResultTuplesCallback([](SystestParser::ResultTuples&&) { FAIL(); });
    parser.registerOnSLTSourceCallback([&isSLTSourceCallbackCalled](SystestParser::SLTSource&&) { isSLTSourceCallbackCalled = true; });
    parser.registerOnAttachSourceCallback(
        [&isAttachSourceCallbackCalled](SystestAttachSource&& attachSource)
        {
            ASSERT_EQ(attachSource.configurationPath, "CONFIG/sources/file_inline_default.yaml");
            ASSERT_EQ(attachSource.logicalSourceName, "window");
            ASSERT_EQ(attachSource.sourceType, "File");
            ASSERT_EQ(attachSource.testDataIngestionType, TestDataIngestionType::INLINE);
            isAttachSourceCallbackCalled = true;
        });

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(isSLTSourceCallbackCalled);
    ASSERT_TRUE(isAttachSourceCallbackCalled);
}

TEST_F(SystestParserTest, testCallbackQuery)
{
    SystestParser parser{};
    const std::string queryIn = "SELECT id, value, timestamp FROM window WHERE value == 1 INTO SINK";
    const std::string delimiter = "----";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    bool queryCallbackCalled = false;
    bool resultCallbackCalled = false;

    const std::string str = queryIn + "\n" + delimiter + "\n" + tpl1 + "\n" + tpl2 + "\n";

    parser.registerOnQueryCallback(
        [&](SystestParser::Query&& queryOut)
        {
            ASSERT_EQ(queryIn, queryOut);
            queryCallbackCalled = true;
        });
    parser.registerOnResultTuplesCallback(
        [&](SystestParser::ResultTuples&& result)
        {
            ASSERT_EQ(result[0], tpl1);
            ASSERT_EQ(result[1], tpl2);
            resultCallbackCalled = true;
        });
    parser.registerOnSLTSourceCallback([&](SystestParser::SLTSource&&) { FAIL(); });
    parser.registerOnAttachSourceCallback([&](SystestAttachSource&&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(queryCallbackCalled);
    ASSERT_TRUE(resultCallbackCalled);
}

TEST_F(SystestParserTest, testCallbackSLTSource)
{
    SystestParser parser{};
    const std::string sourceIn = "Source window UINT64 id UINT64 value UINT64 timestamp";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    bool callbackCalled = false;

    const std::string str = sourceIn + "\n" + tpl1 + "\n" + tpl2 + "\n";

    parser.registerOnQueryCallback([&](SystestParser::Query&&) { FAIL(); });
    parser.registerOnResultTuplesCallback([&](SystestParser::ResultTuples&&) { FAIL(); });
    parser.registerOnSLTSourceCallback(
        [&](SystestParser::SLTSource&& sourceOut)
        {
            ASSERT_EQ(sourceOut.name, "window");
            ASSERT_EQ(*sourceOut.fields[0].type, *DataTypeProvider::provideDataType(LogicalType::UINT64));
            ASSERT_EQ(sourceOut.fields[0].name, "id");
            ASSERT_EQ(*sourceOut.fields[1].type, *DataTypeProvider::provideDataType(LogicalType::UINT64));
            ASSERT_EQ(sourceOut.fields[1].name, "value");
            ASSERT_EQ(*sourceOut.fields[2].type, *DataTypeProvider::provideDataType(LogicalType::UINT64));
            ASSERT_EQ(sourceOut.fields[2].name, "timestamp");
            // ASSERT_EQ(sourceOut.tuples[0], tpl1);
            // ASSERT_EQ(sourceOut.tuples[1], tpl2);
            callbackCalled = true;
        });
    parser.registerOnAttachSourceCallback([&](SystestAttachSource&&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(callbackCalled);
}

TEST_F(SystestParserTest, testResultTuplesWithoutQuery)
{
    SystestParser parser{};
    const std::string delimiter = "----";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    const std::string str = delimiter + "\n" + tpl1 + "\n" + tpl2 + "\n";

    parser.registerOnQueryCallback([&](SystestParser::Query&&) { FAIL(); });
    parser.registerOnResultTuplesCallback(
        [&](SystestParser::ResultTuples&&)
        {
            /// nop
        });
    parser.registerOnSLTSourceCallback([&](SystestParser::SLTSource&&) { FAIL(); });
    parser.registerOnAttachSourceCallback([&](SystestAttachSource&&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(str));
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
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

    const SystestParser::QueryCallback callback = [&queryExpect, &callbackCalled](const std::string& query)
    {
        ASSERT_EQ(queryExpect, query);
        callbackCalled = true;
    };
    parser.registerOnQueryCallback(callback);

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
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
