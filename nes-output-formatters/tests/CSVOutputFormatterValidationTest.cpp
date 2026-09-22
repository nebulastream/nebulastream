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

#include <CSVOutputFormatter.hpp>

#include <string>
#include <unordered_map>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// CSVOutputFormatter::writeFormattedValue is a Nautilus-traced function and needs the full JIT/buffer
/// machinery to exercise, so it is covered end-to-end by the nes-systests formatter suite. validateAndFormat
/// is a plain function operating on strings, which makes it worth pinning down directly: it is the only
/// place that decides the defaults and rejects malformed CSV sink/source options before a query ever runs.

TEST(CSVOutputFormatterValidationTest, DefaultsWhenConfigIsEmpty)
{
    const auto config = CSVOutputFormatter::validateAndFormat({});

    EXPECT_FALSE(std::get<bool>(config.at("QUOTE_STRINGS")));
    EXPECT_EQ(std::get<std::string>(config.at("FIELD_DELIMITER")), ",");
    EXPECT_EQ(std::get<std::string>(config.at("TUPLE_DELIMITER")), "\n");
}

TEST(CSVOutputFormatterValidationTest, ParsesQuoteStringsBoolean)
{
    const auto config = CSVOutputFormatter::validateAndFormat({{"QUOTE_STRINGS", "true"}});
    EXPECT_TRUE(std::get<bool>(config.at("QUOTE_STRINGS")));
}

TEST(CSVOutputFormatterValidationTest, AcceptsCustomDelimiters)
{
    /// A multi-character tuple delimiter and a field delimiter that would itself need escaping in the
    /// output are exactly the edge cases the happy-path systests never exercise.
    const auto config = CSVOutputFormatter::validateAndFormat({{"FIELD_DELIMITER", "|"}, {"TUPLE_DELIMITER", "\r\n"}});

    EXPECT_EQ(std::get<std::string>(config.at("FIELD_DELIMITER")), "|");
    EXPECT_EQ(std::get<std::string>(config.at("TUPLE_DELIMITER")), "\r\n");
}

TEST(CSVOutputFormatterValidationTest, RejectsUnknownParameter)
{
    try
    {
        CSVOutputFormatter::validateAndFormat({{"NOT_A_REAL_PARAMETER", "1"}});
        FAIL() << "Expected an InvalidConfigParameter exception";
    }
    catch (const Exception& exception)
    {
        EXPECT_EQ(exception.code(), ErrorCode::InvalidConfigParameter);
    }
}

TEST(CSVOutputFormatterValidationTest, RejectsMalformedBoolean)
{
    try
    {
        CSVOutputFormatter::validateAndFormat({{"QUOTE_STRINGS", "not-a-bool"}});
        FAIL() << "Expected an InvalidConfigParameter exception";
    }
    catch (const Exception& exception)
    {
        EXPECT_EQ(exception.code(), ErrorCode::InvalidConfigParameter);
    }
}

}
