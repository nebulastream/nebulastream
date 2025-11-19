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

#include <vector>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
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
        Logger::setupLogging("UtilFunctionTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("UtilFunctionTest test class SetUpTestCase.");
    }

    static void TearDownTestCase() { NES_INFO("UtilFunctionTest test class TearDownTestCase."); }
};

TEST(UtilFunctionTest, replaceNothing)
{
    const std::string origin = "I do not have the search string in me.";
    const std::string search = "nebula";
    const std::string replace = "replacing";
    const std::string replacedString = replaceFirst(origin, search, replace);
    EXPECT_TRUE(replacedString == origin);
}

TEST(UtilFunctionTest, trimWhiteSpaces)
{
    EXPECT_EQ(trimWhiteSpaces("   1234  "), "1234");
    EXPECT_EQ(trimWhiteSpaces("   12  34  "), "12  34");
    EXPECT_EQ(trimWhiteSpaces("     "), "");
    EXPECT_EQ(trimWhiteSpaces(""), "");
}

TEST(UtilFunctionTest, replaceOnceWithOneFinding)
{
    const std::string origin = "I do  have the search string nebula in me, but only once.";
    const std::string search = "nebula";
    const std::string replace = "replacing";
    const std::string replacedString = replaceFirst(origin, search, replace);
    const std::string expectedReplacedString = "I do  have the search string replacing in me, but only once.";
    EXPECT_TRUE(replacedString == expectedReplacedString);
}

TEST(UtilFunctionTest, replaceOnceWithMultipleFindings)
{
    const std::string origin = "I do  have the search string nebula in me, but multiple times nebula";
    const std::string search = "nebula";
    const std::string replace = "replacing";
    const std::string replacedString = replaceFirst(origin, search, replace);
    const std::string expectedReplacedString = "I do  have the search string replacing in me, but multiple times nebula";
    EXPECT_TRUE(replacedString == expectedReplacedString);
}

TEST(UtilFunctionTest, splitWithStringDelimiterNothing)
{
    std::vector<std::string> test;
    test.emplace_back("This is a random test line with no delimiter.");
    const std::string line = "This is a random test line with no delimiter.";
    const std::string delimiter = "x";
    const auto tokens = splitWithStringDelimiter<std::string>(line, delimiter);
    EXPECT_TRUE(tokens == test);
}

TEST(UtilFunctionTest, splitWithStringDelimiterOnce)
{
    std::vector<std::string> test;
    test.emplace_back("This is a random test line with ");
    test.emplace_back(" delimiter.");
    const std::string line = "This is a random test line with x delimiter.";
    const std::string delimiter = "x";
    const auto tokens = splitWithStringDelimiter<std::string>(line, delimiter);
    EXPECT_TRUE(tokens == test);
}

TEST(UtilFunctionTest, splitWithStringDelimiterTwice)
{
    std::vector<std::string> test;
    test.emplace_back("This is a random ");
    test.emplace_back(" line with ");
    test.emplace_back(" delimiter.");
    const std::string line = "This is a random x line with x delimiter.";
    const std::string delimiter = "x";
    const auto tokens = splitWithStringDelimiter<std::string>(line, delimiter);
    EXPECT_TRUE(tokens == test);
}

TEST(UtilFunctionTest, splitStringOnMultipleSpaces)
{
    std::vector<std::string> test;
    test.emplace_back("This");
    test.emplace_back("is");
    test.emplace_back("a");
    test.emplace_back("random");
    test.emplace_back("line");
    test.emplace_back("with");
    test.emplace_back("delimiter.");
    const std::string line = "This is a random   line with    delimiter.";
    const auto tokens = splitWithStringDelimiter<std::string>(line, " ");
    EXPECT_TRUE(tokens == test);
}

TEST(UtilFunctionTest, splitIntegersWithWhiteSpaces)
{
    const std::string line = "123,43,123,532, 12, 432,12,43   ,2341,321";
    EXPECT_THAT(splitWithStringDelimiter<int>(line, ","), ::testing::ElementsAre(123, 43, 123, 532, 12, 432, 12, 43, 2341, 321));
}

TEST(UtilFunctionTest, splitWithOmittingEmptyLast)
{
    std::vector<std::string> test;
    test.emplace_back("This is a random ");
    test.emplace_back(" line with ");
    test.emplace_back(" delimiter. ");
    const std::string line = "This is a random x line with x delimiter. x";
    const std::string delimiter = "x";
    const auto tokens = splitWithStringDelimiter<std::string>(line, delimiter);
    EXPECT_EQ(tokens, test);
}

}
