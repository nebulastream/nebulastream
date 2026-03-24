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
#include <utility>
#include <vector>

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
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    const std::string filename = SYSTEST_DATA_DIR "invalid.dummy";
    SystestParser parser{};
    ASSERT_TRUE(parser.loadFile(filename));
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidErrorCodeTest)
{
    const auto* const filename = SYSTEST_DATA_DIR "invalid_error.dummy";

    const auto* const expectQuery = R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow;)";

    SystestParser parser{};
    parser.registerOnQueryCallback([&](const std::string& query, const SystestQueryId, bool) { ASSERT_EQ(query, expectQuery); });

    parser.registerOnErrorExpectationCallback(
        [&](const SystestParser::ErrorExpectation&, const SystestQueryId)
        {
            /// nop, ensure parsing
        });

    ASSERT_TRUE(parser.loadFile(filename));
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidErrorMessageTest)
{
    const auto* const filename = SYSTEST_DATA_DIR "invalid_error_message.dummy";

    const auto* const expectQuery = R"(SELECT * FROM window WHERE value == UINT64(1) INTO sinkWindow;)";

    SystestParser parser{};
    parser.registerOnQueryCallback([&](const std::string& query, SystestQueryId, bool) { ASSERT_EQ(query, expectQuery); });

    parser.registerOnErrorExpectationCallback(
        [&](const SystestParser::ErrorExpectation&, const SystestQueryId)
        {
            /// nop, ensure parsing
        });

    ASSERT_TRUE(parser.loadFile(filename));
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidTokenTest)
{
    const auto* const filename = SYSTEST_DATA_DIR "invalid_token.dummy";

    SystestParser parser{};
    parser.registerOnQueryCallback([&](const std::string&, SystestQueryId, bool) { /* nop, ensure parsing*/ });
    parser.registerOnCreateCallback(
        [&](const std::string&, const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { });

    ASSERT_TRUE(parser.loadFile(filename));
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

TEST_F(SystestParserInvalidTestFilesTest, InvalidDifferentialTest)
{
    const auto* const filename = SYSTEST_DATA_DIR "invalid_differential.dummy";

    SystestParser parser{};
    parser.registerOnCreateCallback(
        [&](const std::string&,
            const std::optional<std::pair<TestDataIngestionType, std::vector<std::string>>>&) { /* nop, ensure parsing*/ });
    parser.registerOnQueryCallback([&](const std::string&, SystestQueryId, bool) { /* nop, ensure parsing*/ });
    parser.registerOnDifferentialQueryBlockCallback(
        [](std::string, std::string, SystestQueryId, SystestQueryId) { /* nop, ensure parsing*/ });

    ASSERT_TRUE(parser.loadFile(filename));
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

}
