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
#include <Parser/SLTParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES::SLTParser
{
/// Tests if SLT Parser rejects invalid .test files correctly
class SLTParserInvalidTestFilesTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SLTParserInvalidTestFilesTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SLTParserInvalidTestFilesTest test class.");
    }

    static void TearDownTestSuite() { NES_DEBUG("Tear down SLTParserInvalidTestFilesTest test class."); }
};

TEST_F(SLTParserInvalidTestFilesTest, InvalidTestFile)
{
    std::string const filename = TEST_DATA_DIR "invalid.test";

    SLTParser parser{};
    parser.registerOnCSVSourceCallback(
        [&](SLTParser::CSVSource&&)
        {
            /// nop, ensure parsing of CSVSource token
        });

    ASSERT_TRUE(parser.loadFile(filename));
    ASSERT_EXCEPTION_ERRORCODE({ parser.parse(); }, ErrorCode::SLTUnexpectedToken)
}

}
