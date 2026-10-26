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

#include <Aggregation/Function/JsonAggEncoding.hpp>

#include <string>
#include <string_view>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

namespace NES
{

namespace
{
std::string arrayOf(std::initializer_list<std::string_view> values)
{
    JsonArrayBuilder builder;
    for (const auto value : values)
    {
        builder.append(value);
    }
    return builder.finish();
}
}

TEST(JsonAggEncodingTest, EmptyArray)
{
    EXPECT_EQ(arrayOf({}), "[]");
}

TEST(JsonAggEncodingTest, CompactArrayInAppendOrder)
{
    EXPECT_EQ(arrayOf({"b", "a", ""}), R"(["b","a",""])");
}

TEST(JsonAggEncodingTest, QuotesBackslashesAndControlCharacters)
{
    EXPECT_EQ(
        arrayOf({"say \"hi\"", "C:\\dir", "a\nb\tc\r", std::string_view{"\x01\x1f", 2}}),
        R"(["say \"hi\"","C:\\dir","a\nb\tc\r","\u0001\u001f"])");
}

TEST(JsonAggEncodingTest, Utf8IsKeptAsItIs)
{
    EXPECT_EQ(arrayOf({"ü€😀"}), "[\"ü€😀\"]");
}

TEST(JsonAggEncodingTest, FinishIsIdempotent)
{
    JsonArrayBuilder builder;
    builder.append("x");
    EXPECT_EQ(builder.finish(), R"(["x"])");
    EXPECT_EQ(builder.finish(), R"(["x"])");
}

/// Whatever goes in comes back out of a standard JSON parser unchanged.
TEST(JsonAggEncodingTest, RoundTripsThroughAJsonParser)
{
    const std::string nasty = "\"\\/\b\f\n\r\t\x7f ü \x02 end";
    const auto parsed = nlohmann::json::parse(arrayOf({nasty, "", "plain"}));
    ASSERT_TRUE(parsed.is_array());
    ASSERT_EQ(parsed.size(), 3U);
    EXPECT_EQ(parsed[0].get<std::string>(), nasty);
    EXPECT_EQ(parsed[1].get<std::string>(), "");
    EXPECT_EQ(parsed[2].get<std::string>(), "plain");
}

}
