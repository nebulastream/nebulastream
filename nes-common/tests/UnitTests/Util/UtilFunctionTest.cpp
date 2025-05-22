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

#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES
{
class UtilFunctionTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("UtilFunctionTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("UtilFunctionTest test class SetUpTestCase.");
    }

    static void TearDownTestCase() { NES_INFO("UtilFunctionTest test class TearDownTestCase."); }
};

TEST(UtilFunctionTest, replaceNothing)
{
    std::string origin = "I do not have the search string in me.";
    std::string search = "nebula";
    std::string replace = "replacing";
    std::string replacedString = Util::replaceFirst(origin, search, replace);
    EXPECT_TRUE(replacedString == origin);
}

TEST(UtilFunctionTest, trimWhiteSpaces)
{
    EXPECT_EQ(NES::Util::trimWhiteSpaces("   1234  "), "1234");
    EXPECT_EQ(NES::Util::trimWhiteSpaces("   12  34  "), "12  34");
    EXPECT_EQ(NES::Util::trimWhiteSpaces("     "), "");
    EXPECT_EQ(NES::Util::trimWhiteSpaces(""), "");
}

TEST(UtilFunctionTest, replaceOnceWithOneFinding)
{
    std::string origin = "I do  have the search string nebula in me, but only once.";
    std::string search = "nebula";
    std::string replace = "replacing";
    std::string replacedString = Util::replaceFirst(origin, search, replace);
    std::string expectedReplacedString = "I do  have the search string replacing in me, but only once.";
    EXPECT_TRUE(replacedString == expectedReplacedString);
}

TEST(UtilFunctionTest, replaceOnceWithMultipleFindings)
{
    std::string origin = "I do  have the search string nebula in me, but multiple times nebula";
    std::string search = "nebula";
    std::string replace = "replacing";
    std::string replacedString = Util::replaceFirst(origin, search, replace);
    std::string expectedReplacedString = "I do  have the search string replacing in me, but multiple times nebula";
    EXPECT_TRUE(replacedString == expectedReplacedString);
}

TEST(UtilFunctionTest, splitWithStringDelimiterNothing)
{
    std::vector<std::string> tokens;
    std::vector<std::string> test;
    test.emplace_back("This is a random test line with no delimiter.");
    std::string line = "This is a random test line with no delimiter.";
    std::string delimiter = "x";
    tokens = NES::Util::splitWithStringDelimiter<std::string>(line, delimiter);
    EXPECT_TRUE(tokens == test);
}

TEST(UtilFunctionTest, splitWithStringDelimiterOnce)
{
    std::vector<std::string> tokens;
    std::vector<std::string> test;
    test.emplace_back("This is a random test line with ");
    test.emplace_back(" delimiter.");
    std::string line = "This is a random test line with x delimiter.";
    std::string delimiter = "x";
    tokens = NES::Util::splitWithStringDelimiter<std::string>(line, delimiter);
    EXPECT_TRUE(tokens == test);
}

TEST(UtilFunctionTest, splitWithStringDelimiterTwice)
{
    std::vector<std::string> tokens;
    std::vector<std::string> test;
    test.emplace_back("This is a random ");
    test.emplace_back(" line with ");
    test.emplace_back(" delimiter.");
    std::string line = "This is a random x line with x delimiter.";
    std::string delimiter = "x";
    tokens = NES::Util::splitWithStringDelimiter<std::string>(line, delimiter);
    EXPECT_TRUE(tokens == test);
}

TEST(UtilFunctionTest, splitIntegersWithWhiteSpaces)
{
    const std::string line = "123,43,123,532, 12, 432,12,43   ,2341,321";
    EXPECT_THAT(NES::Util::splitWithStringDelimiter<int>(line, ","), ::testing::ElementsAre(123, 43, 123, 532, 12, 432, 12, 43, 2341, 321));
}

TEST(UtilFunctionTest, splitWithOmittingEmptyLast)
{
    std::vector<std::string> tokens;
    std::vector<std::string> test;
    test.emplace_back("This is a random ");
    test.emplace_back(" line with ");
    test.emplace_back(" delimiter. ");
    const std::string line = "This is a random x line with x delimiter. x";
    const std::string delimiter = "x";
    tokens = NES::Util::splitWithStringDelimiter<std::string>(line, delimiter);
    EXPECT_EQ(tokens, test);
}

}
