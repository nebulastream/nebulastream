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
#include <optional>
#include <string>
#include <Util/Strings.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace NES::Util
{

TEST(TrimWhiteSpacesTest, TrimLeadingAndTrailingSpaces)
{
    EXPECT_EQ(trimWhiteSpaces("   hello world   "), "hello world");
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
    EXPECT_DEATH([[maybe_unused]] auto testString = toLowerCase("ÉÇÀÔ"), "Precondition violated:.*");
    EXPECT_DEATH([[maybe_unused]] auto testString = toUpperCase("éçàô"), "Precondition violated:*");
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
    std::string lowerStr = "héllô!123";
    EXPECT_DEATH(toUpperCaseInplace(lowerStr), "Precondition violated:.*");
    std::string str = "HÉLLÔ!123";
    EXPECT_DEATH(toLowerCaseInplace(str), "Precondition violated:.*");
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

TEST(TrimCharactersTest, EmptyStringReturnsEmpty)
{
    EXPECT_TRUE(trimCharacters("", ' ').empty());
}

TEST(TrimCharactersTest, StringWithoutTargetCharacterRemainsSame)
{
    std::string_view input = "hello world";
    EXPECT_EQ(trimCharacters(input, '#'), input);
}

TEST(TrimCharactersTest, TrimLeadingSpaces)
{
    EXPECT_EQ(trimCharacters("   hello", ' '), "hello");
}

TEST(TrimCharactersTest, TrimTrailingSpaces)
{
    EXPECT_EQ(trimCharacters("hello   ", ' '), "hello");
}

TEST(TrimCharactersTest, TrimBothEndsSpaces)
{
    EXPECT_EQ(trimCharacters("   hello world   ", ' '), "hello world");
}

TEST(TrimCharactersTest, TrimMultipleOccurrencesOfChar)
{
    EXPECT_EQ(trimCharacters("###hello###", '#'), "hello");
}

TEST(TrimCharactersTest, StringWithOnlyTrimCharacter)
{
    EXPECT_TRUE(trimCharacters("     ", ' ').empty());
}

TEST(TrimCharactersTest, PreservesInnerSpaces)
{
    EXPECT_EQ(trimCharacters("  hello   world  ", ' '), "hello   world");
}

TEST(TrimCharactersTest, TrimSpecialCharacters)
{
    EXPECT_EQ(trimCharacters("\t\thello\t", '\t'), "hello");
}

TEST(TrimCharactersTest, SingleCharacterString)
{
    EXPECT_TRUE(trimCharacters("h", 'h').empty());
}

TEST(TrimCharactersTest, VerifyUnderlyingDataNotModified)
{
    std::string original = "  hello  ";
    std::string_view view(original);
    auto result = trimCharacters(view, ' ');

    EXPECT_EQ(original, "  hello  ") << "Original string should not have been modified";
    EXPECT_TRUE(result.data() >= view.data() && result.data() + result.length() <= view.data() + view.length());
}

}
