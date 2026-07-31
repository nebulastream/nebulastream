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
#include <string_view>

#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <SystestParser.hpp>

namespace NES::Systest
{
namespace
{

void expectParseFailure(SystestParser& parser, const std::string_view expectedMessage)
{
    try
    {
        (void)parser.parse();
        FAIL() << "Expected parser failure";
    }
    catch (const Exception& exception)
    {
        EXPECT_EQ(exception.code(), ErrorCode::SLTUnexpectedToken);
        EXPECT_EQ(std::string_view(exception.what()), expectedMessage);
    }
}

}

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
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(SYSTEST_DATA_DIR "invalid.dummy"));
    expectParseFailure(
        parser,
        "unexpected token in sql logic test file; Should never run into the INVALID token during systest file parsing, but got line: "
        "Attach File InlineData CONFIG/sources/tcp_inline_default.yaml.\n");
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidErrorCodeTest)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(SYSTEST_DATA_DIR "invalid_error.dummy"));
    expectParseFailure(
        parser, "unexpected token in sql logic test file; invalid error code: 9999000 is not defined in ErrorDefinitions.inc\n");
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidErrorMessageTest)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(SYSTEST_DATA_DIR "invalid_error_message.dummy"));
    expectParseFailure(
        parser, "unexpected token in sql logic test file; invalid error type: InvalidErrorCode is not defined in ErrorDefinitions.inc\n");
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidTokenTest)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(SYSTEST_DATA_DIR "invalid_token.dummy"));
    expectParseFailure(
        parser,
        "unexpected token in sql logic test file; Should never run into the INVALID token during systest file parsing, but got line: "
        "THISISANINVALIDTOKEN.\n");
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidDifferentialTest)
{
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(SYSTEST_DATA_DIR "invalid_differential.dummy"));
    expectParseFailure(
        parser,
        "unexpected token in sql logic test file; Expected differential delimiter '====' but encountered legacy keyword "
        "'DIFFERENTIAL'\n");
}

}
