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
#include <cmath>
#include <limits>
#include <optional>
#include <string>
#include <Util/Strings.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>

namespace NES::Util
{

TEST(TrimWhiteSpacesTest, TrimLeadingAndTrailingSpaces)
{
    EXPECT_EQ(trimWhiteSpaces("   hello world   "), "hello world");
}

TEST(TestTrimCharRight, TrimTrailingZeros)
{
    EXPECT_EQ(trimCharsRight("", '0'), "");
    EXPECT_EQ(trimCharsRight("0", '0'), "");
    EXPECT_EQ(trimCharsRight("1.2340000", '0'), "1.234");
    EXPECT_EQ(trimCharsRight("1.230040000", '0'), "1.23004");
    EXPECT_EQ(trimCharsRight("001.230040000", '0'), "001.23004");
}

TEST(FormatFloatTests, HandlesNoDecimalPoint)
{
    EXPECT_EQ(formatFloat(12345.0F), "12345.0");
    EXPECT_EQ(formatFloat(0.0F), "0.0");
}

TEST(FormatFloatTests, HandlesEdgeCases2)
{
    EXPECT_EQ(formatFloat(0.), "0.0");
    EXPECT_EQ(formatFloat(123.45000), "123.45");
    EXPECT_EQ(formatFloat(.0000), "0.0");
    EXPECT_EQ(formatFloat(0.000100), "0.0001");
    EXPECT_EQ(formatFloat(0.000001F), "0.000001");
    EXPECT_EQ(formatFloat(0.0000001F), "0.0");
    EXPECT_EQ(formatFloat(0.0000005F), "0.0");
    EXPECT_EQ(formatFloat(0.0000006F), "0.000001");
}

TEST(FormatFloatTests, HandlesInfinity)
{
    constexpr float floatInfinity = std::numeric_limits<float>::infinity();
    constexpr double doubleInfinity = std::numeric_limits<double>::infinity();
    EXPECT_EQ(formatFloat(floatInfinity), "inf");
    EXPECT_EQ(formatFloat(doubleInfinity), "inf");
}

TEST(FormatFloatTests, HandlesNaN)
{
    constexpr float floatQuietNan = std::numeric_limits<float>::quiet_NaN();
    constexpr float floatSignalingNan = std::numeric_limits<float>::signaling_NaN();
    EXPECT_EQ(formatFloat(floatQuietNan), "nan");
    EXPECT_EQ(formatFloat(floatSignalingNan), "nan");
    constexpr double doubleQuietNan = std::numeric_limits<double>::quiet_NaN();
    constexpr double doubleSignalingNan = std::numeric_limits<double>::signaling_NaN();
    EXPECT_EQ(formatFloat(doubleQuietNan), "nan");
    EXPECT_EQ(formatFloat(doubleSignalingNan), "nan");
}

TEST(FormatFloatTests, HandlesMax)
{
    constexpr float floatMax = std::numeric_limits<float>::max();
    constexpr double doubleMax = std::numeric_limits<double>::max();
    EXPECT_EQ(formatFloat(floatMax), "340282346638528859811704183484516925440.0");
    EXPECT_EQ(
        formatFloat(doubleMax),
        "1797693134862315708145274237317043567980705675258449965989174768031572607800285387605895586327668781715404589535143824642343213268"
        "8946418276846754670353751698604991057655128207624549009038932894407586850845513394230458323690322294816580855933212334827479782620"
        "4144723168738177180919299881250404026184124858368.0");
}

TEST(FormatFloatTests, HandlesTrailingZerosAfterDecimal)
{
    EXPECT_EQ(formatFloat(0.234000), "0.234");
    EXPECT_EQ(formatFloat(34.0000), "34.0");
    EXPECT_EQ(formatFloat(45.0), "45.0");
}

TEST(FormatFloatTests, HandlesAllZerosAfterDecimal)
{
    EXPECT_EQ(formatFloat(0.0000), "0.0");
    EXPECT_EQ(formatFloat(123.000000), "123.0");
    EXPECT_EQ(formatFloat(0.0), "0.0");
}

TEST(FormatFloatTests, HandlesEdgeCases)
{
    EXPECT_EQ(formatFloat(0.), "0.0");
    EXPECT_EQ(formatFloat(123.45000), "123.45");
    EXPECT_EQ(formatFloat(.0000), "0.0");
    EXPECT_EQ(formatFloat(0.000100), "0.0001");
}

TEST(FormatFloatTests, HandlesNegativeNumbers)
{
    EXPECT_EQ(formatFloat(-123.45000), "-123.45");
    EXPECT_EQ(formatFloat(-0.0000), "-0.0");
    EXPECT_EQ(formatFloat(-0.1000), "-0.1");
}

TEST(TrimWhiteSpacesTest, TrimLeadingSpacesOnly)
{
    EXPECT_EQ(trimWhiteSpaces("   hello"), "hello");
}

TEST(TrimWhiteSpacesTest, TrimTrailingSpacesOnly)
{
    EXPECT_EQ(trimWhiteSpaces("world   "), "world");
}

TEST(TrimWhiteSpacesTest, NoSpacesToTrim)
{
    EXPECT_EQ(trimWhiteSpaces("hello world"), "hello world");
}

TEST(TrimWhiteSpacesTest, EmptyString)
{
    EXPECT_EQ(trimWhiteSpaces(""), "");
}

TEST(TrimWhiteSpacesTest, OnlySpaces)
{
    EXPECT_EQ(trimWhiteSpaces("     "), "");
}

TEST(TrimWhiteSpacesTest, MixedWhitespaceCharacters)
{
    EXPECT_EQ(trimWhiteSpaces("\t \n hello world \t \n "), "hello world");
}

TEST(TrimWhiteSpacesTest, SingleCharacterNoTrim)
{
    EXPECT_EQ(trimWhiteSpaces("a"), "a");
}

TEST(TrimWhiteSpacesTest, SingleCharacterWithSpaces)
{
    EXPECT_EQ(trimWhiteSpaces("  a  "), "a");
}

TEST(FromCharsTest, ValidIntegerInput)
{
    auto result = from_chars<int>("123");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 123);
}

TEST(FromCharsTest, ValidNegativeIntegerInput)
{
    auto result = from_chars<int>("-456");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), -456);
}

TEST(FromCharsTest, InvalidIntegerInput)
{
    auto result = from_chars<int>("abc123");
    EXPECT_FALSE(result.has_value());
}

TEST(FromCharsTest, OverflowIntegerInput)
{
    auto result = from_chars<int>("2147483648"); /// INT_MAX + 1
    EXPECT_FALSE(result.has_value());
}

TEST(FromCharsTest, UnderflowIntegerInput)
{
    auto result = from_chars<int>("-2147483649"); /// INT_MIN - 1
    EXPECT_FALSE(result.has_value());
}

TEST(FromCharsTest, ValidFloatInput)
{
    auto result = from_chars<float>("3.14159");
    ASSERT_TRUE(result.has_value());
    EXPECT_FLOAT_EQ(result.value(), 3.14159F);
}

TEST(FromCharsTest, ValidNegativeFloatInput)
{
    auto result = from_chars<float>("-2.71828");
    ASSERT_TRUE(result.has_value());
    EXPECT_FLOAT_EQ(result.value(), -2.71828F);
}

TEST(FromCharsTest, InvalidFloatInput)
{
    auto result = from_chars<float>("3.14abc");
    EXPECT_TRUE(result.has_value());
    EXPECT_FLOAT_EQ(result.value(), 3.14F);
}

TEST(FromCharsTest, InfinityInput)
{
    auto result = from_chars<float>("inf");
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(std::isinf(result.value()));
}

TEST(FromCharsTest, NaNInput)
{
    auto result = from_chars<float>("nan");
    ASSERT_TRUE(result.has_value());
    EXPECT_TRUE(std::isnan(result.value()));
}

TEST(FromCharsTest, EmptyStringInput)
{
    auto result = from_chars<int>("");
    EXPECT_FALSE(result.has_value());
}

TEST(FromCharsTest, WhitespaceOnlyInput)
{
    auto result = from_chars<int>("   ");
    EXPECT_FALSE(result.has_value());
}

TEST(FromCharsTest, LeadingWhitespaceInputShouldBeTrimmed)
{
    auto result = from_chars<int>("   42");
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value(), 42);
}

TEST(FromCharsTest, TrailingWhitespaceInput)
{
    auto result = from_chars<float>("2.71   ");
    ASSERT_TRUE(result.has_value());
    EXPECT_FLOAT_EQ(result.value(), 2.71F);
}

TEST(FromCharsTest, EdgeCaseMaxInt)
{
    auto result = from_chars<int>("2147483647"); /// INT_MAX
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 2147483647);
}

TEST(FromCharsTest, EdgeCaseMinInt)
{
    auto result = from_chars<int>("-2147483648"); /// INT_MIN
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), -2147483648);
}

TEST(FromCharsTest, TooSmallFloatInput)
{
    auto result = from_chars<float>("-1e-38");
    ASSERT_FALSE(result.has_value());
}

TEST(FromCharsTest, SmallFloatInput)
{
    auto result = from_chars<float>("-1e-37");
    ASSERT_TRUE(result.has_value());
    EXPECT_FLOAT_EQ(result.value(), -1e-37F);
}

TEST(FromCharsTest, DigitsWithCharacterInTheMiddle)
{
    auto result = from_chars<int>("12a34");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 12);
}

TEST(FromCharsTest, FromCharsWithBoolean)
{
    EXPECT_THAT(from_chars<bool>("true"), ::testing::Optional(true));
    EXPECT_THAT(from_chars<bool>("  true "), ::testing::Optional(true));
    EXPECT_THAT(from_chars<bool>("True"), ::testing::Optional(true));
    EXPECT_THAT(from_chars<bool>("TRUE"), ::testing::Optional(true));

    EXPECT_THAT(from_chars<bool>("1"), ::testing::Optional(true));
    EXPECT_THAT(from_chars<bool>("  1"), ::testing::Optional(true));
    EXPECT_THAT(from_chars<bool>("  1  "), ::testing::Optional(true));


    EXPECT_THAT(from_chars<bool>("false"), ::testing::Optional(false));
    EXPECT_THAT(from_chars<bool>("  false "), ::testing::Optional(false));
    EXPECT_THAT(from_chars<bool>("False"), ::testing::Optional(false));
    EXPECT_THAT(from_chars<bool>("FALSE"), ::testing::Optional(false));

    EXPECT_THAT(from_chars<bool>("0"), ::testing::Optional(false));
    EXPECT_THAT(from_chars<bool>("  0"), ::testing::Optional(false));
    EXPECT_THAT(from_chars<bool>("  0  "), ::testing::Optional(false));


    EXPECT_EQ(from_chars<bool>("false true"), std::nullopt);
    EXPECT_EQ(from_chars<bool>("  valse "), std::nullopt);
    EXPECT_EQ(from_chars<bool>("  11 "), std::nullopt);
    EXPECT_EQ(from_chars<bool>("10"), std::nullopt);
    EXPECT_EQ(from_chars<bool>("yes"), std::nullopt);
}

TEST(FromCharsTest, ScientificNotationInput)
{
    auto result = from_chars<float>("1.23e4");
    ASSERT_TRUE(result.has_value());
    EXPECT_FLOAT_EQ(result.value(), 12300.0F);
}

TEST(StringCaseConversionTest, ToUpperCaseBasic)
{
    EXPECT_EQ(toUpperCase("hello"), "HELLO");
    EXPECT_EQ(toUpperCase("Hello World"), "HELLO WORLD");
    EXPECT_EQ(toUpperCase("123abc"), "123ABC");
}

TEST(StringCaseConversionTest, ToUpperCaseEmpty)
{
    EXPECT_EQ(toUpperCase(""), "");
}

TEST(StringCaseConversionTest, ToUpperCaseSpecialCharacters)
{
    EXPECT_EQ(toUpperCase("hello!@#"), "HELLO!@#");
}

TEST(StringCaseConversionTest, ToLowerCaseBasic)
{
    EXPECT_EQ(toLowerCase("HELLO"), "hello");
    EXPECT_EQ(toLowerCase("Hello World"), "hello world");
    EXPECT_EQ(toLowerCase("123ABC"), "123abc");
}

TEST(StringCaseConversionTest, ToLowerCaseEmpty)
{
    EXPECT_EQ(toLowerCase(""), "");
}

TEST(StringCaseConversionTest, ToLowerCaseSpecialCharacters)
{
    EXPECT_EQ(toLowerCase("HELLO!@#"), "hello!@#");
}

TEST(StringCaseConversionTest, NoSupportForNonAsciiCharacters)
{
    SKIP_IF_TSAN();

    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH_DEBUG([]() { [[maybe_unused]] auto testString = toLowerCase("ÉÇÀÔ"); }(), "");
    EXPECT_DEATH_DEBUG([]() { [[maybe_unused]] auto testString = toLowerCase("éçàô"); }(), "");
}

TEST(StringCaseInplaceTest, ToUpperCaseInplaceBasic)
{
    std::string str = "hello";
    toUpperCaseInplace(str);
    EXPECT_EQ(str, "HELLO");
}

TEST(StringCaseInplaceTest, ToUpperCaseInplaceEmpty)
{
    std::string str;
    toUpperCaseInplace(str);
    EXPECT_EQ(str, "");
}

TEST(StringCaseInplaceTest, ToLowerCaseInplaceBasic)
{
    std::string str = "HELLO";
    toLowerCaseInplace(str);
    EXPECT_EQ(str, "hello");
}

TEST(StringCaseInplaceTest, ToLowerCaseInplaceEmpty)
{
    std::string str;
    toLowerCaseInplace(str);
    EXPECT_EQ(str, "");
}

TEST(StringCaseInplaceTest, ToLowerCaseInplaceMixed)
{
    std::string str = "HeLLo WorlD!";
    toLowerCaseInplace(str);
    EXPECT_EQ(str, "hello world!");
}

TEST(StringCaseInplaceTest, NoSupportForNonAsciiCharacters)
{
    SKIP_IF_TSAN();

    GTEST_FLAG_SET(death_test_style, "threadsafe");
    std::string lowerStr = "héllô!123";
    EXPECT_DEATH_DEBUG(toUpperCaseInplace(lowerStr), "");
    std::string str = "HÉLLÔ!123";
    EXPECT_DEATH_DEBUG(toLowerCaseInplace(str), "");
}

TEST(ReplaceAllTest, ReplaceAllOccurrencesBasic)
{
    EXPECT_EQ(replaceAll("hello world", "l", "x"), "hexxo worxd");
    EXPECT_EQ(replaceAll("hello world", "world", "there"), "hello there");
}

TEST(ReplaceAllTest, ReplaceAllEmptySearch)
{
    EXPECT_EQ(replaceAll("hello", "", "x"), "hello"); /// No change if search is empty
}

TEST(ReplaceAllTest, ReplaceAllEmptyReplace)
{
    EXPECT_EQ(replaceAll("hello world", "o", ""), "hell wrld"); /// Removes the "o"
}

TEST(ReplaceAllTest, ReplaceAllNoOccurrences)
{
    EXPECT_EQ(replaceAll("hello world", "x", "y"), "hello world"); /// No "x" found
}

TEST(ReplaceAllTest, ReplaceAllMultipleCharacterMatch)
{
    EXPECT_EQ(replaceAll("abcabcabc", "abc", "x"), "xxx"); /// Replaces each "abc" with "x"
}

TEST(ReplaceAllTest, ReplaceAllSpecialCharacters)
{
    EXPECT_EQ(replaceAll("a*b*c*", "*", "X"), "aXbXcX"); /// Replaces '*' with 'X'
}

TEST(ReplaceFirstTest, ReplaceFirstOccurrenceBasic)
{
    EXPECT_EQ(replaceFirst("hello world", "l", "x"), "hexlo world");
    EXPECT_EQ(replaceFirst("hello world", "world", "there"), "hello there");
}

TEST(ReplaceFirstTest, ReplaceFirstEmptySearch)
{
    EXPECT_EQ(replaceFirst("hello", "", "x"), "hello"); /// No change if search is empty
}

TEST(ReplaceFirstTest, EmptyOrigin)
{
    EXPECT_EQ(replaceFirst("", "3", "x"), ""); /// No change if origin is empty
}

TEST(ReplaceFirstTest, FullReplaceMent)
{
    EXPECT_EQ(replaceFirst("32", "32", "423"), "423");
}

TEST(ReplaceFirstTest, ReplaceFirstEmptyReplace)
{
    EXPECT_EQ(replaceFirst("hello world", "o", ""), "hell world"); /// Removes first "o"
}

TEST(ReplaceFirstTest, ReplaceFirstNoOccurrences)
{
    EXPECT_EQ(replaceFirst("hello world", "x", "y"), "hello world"); /// No "x" found
}

TEST(ReplaceFirstTest, ReplaceFirstMultipleCharacterMatch)
{
    EXPECT_EQ(replaceFirst("abcabcabc", "abc", "x"), "xabcabc"); /// Replaces only the first "abc"
}

TEST(ReplaceFirstTest, ReplaceFirstSpecialCharacters)
{
    EXPECT_EQ(replaceFirst("a*b*c*", "*", "X"), "aXb*c*"); /// Only replaces the first '*'
}

TEST(EscapeSpecialCharactersTest, EmptyString)
{
    EXPECT_EQ("", escapeSpecialCharacters(""));
}

TEST(EscapeSpecialCharactersTest, NoSpecialCharacters)
{
    EXPECT_EQ("Hello World", escapeSpecialCharacters("Hello World"));
    EXPECT_EQ("abc123!@#", escapeSpecialCharacters("abc123!@#"));
}

TEST(EscapeSpecialCharactersTest, SingleSpecialCharacters)
{
    EXPECT_EQ("\\a", escapeSpecialCharacters("\a")); /// Bell
    EXPECT_EQ("\\b", escapeSpecialCharacters("\b")); /// Backspace
    EXPECT_EQ("\\f", escapeSpecialCharacters("\f")); /// Form feed
    EXPECT_EQ("\\n", escapeSpecialCharacters("\n")); /// Newline
    EXPECT_EQ("\\r", escapeSpecialCharacters("\r")); /// Carriage return
    EXPECT_EQ("\\t", escapeSpecialCharacters("\t")); /// Tab
    EXPECT_EQ("\\v", escapeSpecialCharacters("\v")); /// Vertical tab
}

TEST(EscapeSpecialCharactersTest, MultipleSpecialCharacters)
{
    EXPECT_EQ("Line1\\nLine2", escapeSpecialCharacters("Line1\nLine2"));
    EXPECT_EQ("Text\\tTabbed", escapeSpecialCharacters("Text\tTabbed"));
    EXPECT_EQ("\\r\\n", escapeSpecialCharacters("\r\n")); /// Windows line ending
}

TEST(EscapeSpecialCharactersTest, MixedContent)
{
    EXPECT_EQ("Hello\\nWorld\\tTab\\r\\nNewLine", escapeSpecialCharacters("Hello\nWorld\tTab\r\nNewLine"));
    EXPECT_EQ("\\aAlert!\\bBackspace", escapeSpecialCharacters("\aAlert!\bBackspace"));
}

TEST(EscapeSpecialCharactersTest, AlreadyEscapedCharacters)
{
    /// Backslashes that are already part of the string should be preserved
    EXPECT_EQ("\\n", escapeSpecialCharacters("\\n"));
}

TEST(EscapeSpecialCharactersTest, UnicodeAndSpecialMixed)
{
    EXPECT_EQ("Unicode: 😀\\nNext line", escapeSpecialCharacters("Unicode: 😀\nNext line"));
    EXPECT_EQ("\\tÆØÅ\\r\\næøå", escapeSpecialCharacters("\tÆØÅ\r\næøå"));
}

}
