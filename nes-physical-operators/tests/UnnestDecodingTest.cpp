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


#include <UnnestDecoding.hpp>

#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include <Aggregation/Function/JsonAggEncoding.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
using Elements = std::vector<std::string>;

/// The elements as the physical operator sees them, through a packed block.
Elements roundTrip(const std::string_view text)
{
    const auto elements = decodeJsonArrayElements(text);
    std::vector<std::byte> block(packedElementsSize(elements));
    packElements(elements, block);
    Elements unpacked;
    const auto count = packedCount(block.data());
    for (uint64_t i = 0; i < count; ++i)
    {
        const auto* start = packedElement(block.data(), count, i);
        unpacked.emplace_back(reinterpret_cast<const char*>(start), packedElementSize(block.data(), i));
    }
    return unpacked;
}
}

TEST(UnnestDecodingTest, EmptyArray)
{
    EXPECT_EQ(roundTrip("[]"), Elements{});
    EXPECT_EQ(roundTrip("  [ ]  "), Elements{});
}

TEST(UnnestDecodingTest, OneAndManyElements)
{
    EXPECT_EQ(roundTrip(R"(["a"])"), Elements{"a"});
    EXPECT_EQ(roundTrip(R"(["a","","bc"])"), (Elements{"a", "", "bc"}));
    std::string many = "[";
    Elements expected;
    for (int i = 0; i < 5000; ++i)
    {
        many += (i == 0 ? "" : ",") + std::string{"\"e"} + std::to_string(i) + "\"";
        expected.push_back("e" + std::to_string(i));
    }
    many += "]";
    EXPECT_EQ(roundTrip(many), expected);
}

TEST(UnnestDecodingTest, EscapedStringsAreUnescaped)
{
    EXPECT_EQ(
        roundTrip(R"(["say \"hi\"","C:\\dir","a\nb","\u00e9","Grüße 😀","\ud83d\ude00"])"),
        (Elements{"say \"hi\"", "C:\\dir", "a\nb", "é", "Grüße 😀", "😀"}));
}

TEST(UnnestDecodingTest, NonStringElementsAreTheirJsonText)
{
    EXPECT_EQ(
        roundTrip(R"([1, 2.5, true, null, {"id": 1, "x": "y"}, [1,2]])"),
        (Elements{"1", "2.5", "true", "null", R"({"id":1,"x":"y"})", "[1,2]"}));
}

TEST(UnnestDecodingTest, RoundTripOfJsonAgg)
{
    JsonArrayBuilder builder;
    const Elements values{"apple", "say \"hi\"", "", "C:\\dir\\file", "tab\there", "Grüße 😀"};
    for (const auto& value : values)
    {
        builder.append(value);
    }
    EXPECT_EQ(roundTrip(builder.finish()), values);
}

TEST(UnnestDecodingTest, NonArrayInputIsAnError)
{
    for (const auto* text : {"", "not json", R"("a string")", "42", R"({"a": [1]})", "[1,", "null"})
    {
        SCOPED_TRACE(text);
        ASSERT_EXCEPTION_ERRORCODE(static_cast<void>(decodeJsonArrayElements(text)), ErrorCode::CannotFormatMalformedStringValue);
    }
}

}
