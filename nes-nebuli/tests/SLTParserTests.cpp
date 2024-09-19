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
#include <Parser/SLTParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <Common/DataTypes/BasicTypes.hpp>

namespace NES::SLTParser
{

class SLTParserTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        Logger::setupLogging("SLTParserTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SLTParserTest test class.");
    }

    static void TearDownTestCase() { NES_DEBUG("Tear down SLTParserTest test class."); }
};

TEST_F(SLTParserTest, testEmptyFile)
{
    SLTParser parser{};
    const std::string str = "";

    ASSERT_EQ(true, parser.loadString(str));
    parser.parse();
}

TEST_F(SLTParserTest, testEmptyLinesAndCommasFile)
{
    SLTParser parser{};
    /// Comment, new line in Unix/Linux, Windows, Older Mac systems
    const std::string str = std::string("#\n") + "\n" + "\r\n" + "\r";

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
}

TEST_F(SLTParserTest, testCallbackSourceCSV)
{
    SLTParser parser{};
    const std::string sourceIn = "SourceCSV window UINT64 id UINT64 value UINT64 timestamp window.csv";

    bool callbackCalled = false;

    const std::string str = sourceIn + "\n";

    parser.registerOnQueryCallback([&](SLTParser::Query&&) { FAIL(); });
    parser.registerOnResultTuplesCallback([&](SLTParser::ResultTuples&&) { FAIL(); });
    parser.registerOnSLTSourceCallback([&](SLTParser::SLTSource&&) { FAIL(); });
    parser.registerOnCSVSourceCallback(
        [&](SLTParser::CSVSource&& sourceOut)
        {
            ASSERT_EQ(sourceOut.name, "window");
            ASSERT_EQ(sourceOut.fields[0].type, BasicType::UINT64);
            ASSERT_EQ(sourceOut.fields[0].name, "id");
            ASSERT_EQ(sourceOut.fields[1].type, BasicType::UINT64);
            ASSERT_EQ(sourceOut.fields[1].name, "value");
            ASSERT_EQ(sourceOut.fields[2].type, BasicType::UINT64);
            ASSERT_EQ(sourceOut.fields[2].name, "timestamp");
            ASSERT_EQ(sourceOut.csvFilePath, "window.csv");
            callbackCalled = true;
        });

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(callbackCalled);
}

TEST_F(SLTParserTest, testCallbackQuery)
{
    SLTParser parser{};
    std::string queryIn = "Query::from('window').filter(Attribute('value') == 1).SINK;";
    const std::string delimiter = "----";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    bool queryCallbackCalled = false;
    bool resultCallbackCalled = false;

    const std::string str = queryIn + "\n" + delimiter + "\n" + tpl1 + "\n" + tpl2 + "\n";

    parser.registerOnQueryCallback(
        [&](SLTParser::Query&& queryOut)
        {
            ASSERT_EQ(queryIn, queryOut);
            queryCallbackCalled = true;
        });
    parser.registerOnResultTuplesCallback(
        [&](SLTParser::ResultTuples&& result)
        {
            ASSERT_EQ(result[0], tpl1);
            ASSERT_EQ(result[1], tpl2);
            resultCallbackCalled = true;
        });
    parser.registerOnSLTSourceCallback([&](SLTParser::SLTSource&&) { FAIL(); });
    parser.registerOnCSVSourceCallback([&](SLTParser::CSVSource&&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(queryCallbackCalled);
    ASSERT_TRUE(resultCallbackCalled);
}

TEST_F(SLTParserTest, testCallbackSLTSource)
{
    SLTParser parser{};
    const std::string sourceIn = "Source window UINT64 id UINT64 value UINT64 timestamp";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    bool callbackCalled = false;

    const std::string str = sourceIn + "\n" + tpl1 + "\n" + tpl2 + "\n";

    parser.registerOnQueryCallback([&](SLTParser::Query&&) { FAIL(); });
    parser.registerOnResultTuplesCallback([&](SLTParser::ResultTuples&&) { FAIL(); });
    parser.registerOnSLTSourceCallback(
        [&](SLTParser::SLTSource&& sourceOut)
        {
            ASSERT_EQ(sourceOut.name, "window");
            ASSERT_EQ(sourceOut.fields[0].type, BasicType::UINT64);
            ASSERT_EQ(sourceOut.fields[0].name, "id");
            ASSERT_EQ(sourceOut.fields[1].type, BasicType::UINT64);
            ASSERT_EQ(sourceOut.fields[1].name, "value");
            ASSERT_EQ(sourceOut.fields[2].type, BasicType::UINT64);
            ASSERT_EQ(sourceOut.fields[2].name, "timestamp");
            ASSERT_EQ(sourceOut.tuples[0], tpl1);
            ASSERT_EQ(sourceOut.tuples[1], tpl2);
            callbackCalled = true;
        });
    parser.registerOnCSVSourceCallback([&](SLTParser::CSVSource&&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(callbackCalled);
}

TEST_F(SLTParserTest, testResultTuplesWithoutQuery)
{
    SLTParser parser{};
    const std::string delimiter = "----";
    const std::string tpl1 = "1,1,1";
    const std::string tpl2 = "2,2,2";

    const std::string str = delimiter + "\n" + tpl1 + "\n" + tpl2 + "\n";

    parser.registerOnQueryCallback([&](SLTParser::Query&&) { FAIL(); });
    parser.registerOnResultTuplesCallback(
        [&](SLTParser::ResultTuples&&)
        {
            /// nop
        });
    parser.registerOnSLTSourceCallback([&](SLTParser::SLTSource&&) { FAIL(); });
    parser.registerOnCSVSourceCallback([&](SLTParser::CSVSource&&) { FAIL(); });

    ASSERT_TRUE(parser.loadString(str));
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SLTParserTest, testSubstitutionRule)
{
    SLTParser parser{};
    std::string const queryIn = "Query::from('window').filter(Attribute('value') == 1).SINK;";
    std::string const delim = "----";
    std::string const result = "1 1 1";

    std::string queryExpect = "Query::from('window').filter(Attribute('value') == 1).TestSink();";

    bool callbackCalled = false;

    const std::string str = queryIn + "\n" + delim + "\n" + result + "\n";

    SLTParser::SubstitutionRule const rule{.keyword = "SINK", .ruleFunction = [](std::string& in) { in = "TestSink()"; }};
    parser.registerSubstitutionRule(rule);

    SLTParser::QueryCallback const callback = [&queryExpect, &callbackCalled](std::string query)
    {
        ASSERT_EQ(queryExpect, query);
        callbackCalled = true;
    };
    parser.registerOnQueryCallback(callback);

    ASSERT_TRUE(parser.loadString(str));
    EXPECT_NO_THROW(parser.parse());
    ASSERT_TRUE(callbackCalled);
}

TEST_F(SLTParserTest, testRegisterSubstitutionKeywordTwoTimes)
{
    SLTParser::SubstitutionRule const rule1{.keyword = "SINK", .ruleFunction = [](std::string& in) { in = "TestSink()"; }};
    SLTParser::SubstitutionRule const rule2{.keyword = "SINK", .ruleFunction = [](std::string& in) { in = "AnotherTestSink()"; }};

    SLTParser parser{};
    parser.registerSubstitutionRule(rule1);
    ASSERT_EXCEPTION_ERRORCODE({ parser.registerSubstitutionRule(rule2); }, ErrorCode::PreconditionViolated)
}

}
