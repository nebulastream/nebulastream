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

#include <string>
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
/// Tests if SLT Parser rejects invalid .test files correctly
class SystestParserInvalidTestFilesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SystestParserInvalidTestFilesTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SystestParserInvalidTestFilesTest test class.");
    }
    static void TearDownTestSuite() { NES_DEBUG("Tear down SystestParserInvalidTestFilesTest test class."); }
};
TEST_F(SystestParserInvalidTestFilesTest, InvalidTestFile)
{
    const std::string filename = TEST_DATA_DIR "invalid.dummy";
    SystestParser parser{};
    parser.registerOnCSVSourceCallback(
        [&](SystestParser::CSVSource&&)
        {
            /// nop, ensure parsing of CSVSource token
        });
    ASSERT_TRUE(parser.loadFile(filename));
    QueryResultMap queryResultMap{};
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(queryResultMap, {}, {}); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidErrorCodeTest)
{
    const auto* const filename = TEST_DATA_DIR "invalid_error.dummy";

    const auto* const expectQuery = R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow;)";

    SystestParser parser{};
    parser.registerOnQueryCallback([&](std::string&& query, const size_t) { ASSERT_EQ(query, expectQuery); });

    parser.registerOnErrorExpectationCallback(
        [&](SystestParser::ErrorExpectation&&)
        {
            /// nop, ensure parsing
        });

    ASSERT_TRUE(parser.loadFile(filename));
    QueryResultMap queryResultMap{};
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(queryResultMap, {}, {}); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidErrorMessageTest)
{
    const auto* const filename = TEST_DATA_DIR "invalid_error_message.dummy";

    const auto* const expectQuery = R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow;)";

    SystestParser parser{};
    parser.registerOnQueryCallback([&](std::string&& query, size_t) { ASSERT_EQ(query, expectQuery); });

    parser.registerOnErrorExpectationCallback(
        [&](SystestParser::ErrorExpectation&&)
        {
            /// nop, ensure parsing
        });

    QueryResultMap queryResultMap{};
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(queryResultMap, {}, {}); }, ErrorCode::SLTUnexpectedToken)
}

}
