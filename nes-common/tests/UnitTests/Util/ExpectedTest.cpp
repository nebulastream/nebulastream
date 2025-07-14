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

#include <Util/Expected.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>

namespace NES
{
TEST(ExpectedTest, BasicUsage)
{
    {
        auto result = [] -> Expected<uint32_t> { return 3214; }();
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), 3214);
    }

    {
        auto result = [] -> Expected<std::string> { return "OkayValue"; }();
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), "OkayValue");
    }

    {
        auto result = [] -> Expected<std::string> { return unexpected("OkayValue"); }();
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), "OkayValue");
    }
}

TEST(ExpectedTest, OptionalConversion)
{
    auto testCase = []<typename T>(std::optional<T>&& opt) { return toExpected(std::move(opt), "Nullopt"); };

    EXPECT_TRUE(testCase(std::make_optional(3214)).has_value());
    EXPECT_EQ(testCase(std::make_optional(3214)).value(), 3214);

    EXPECT_FALSE(testCase.template operator()<uint32_t>({}).has_value());
    EXPECT_EQ(testCase.template operator()<uint32_t>({}).error(), "Nullopt");
}

TEST(ExpectedTest, OptionalExpectedConversion)
{
    auto testCase = []<typename T>(std::optional<Expected<T>>&& opt) { return toExpected(std::move(opt), "Nullopt"); };

    EXPECT_TRUE(testCase(std::make_optional(Expected<uint32_t>(3214))).has_value());
    EXPECT_EQ(testCase(std::make_optional(Expected<uint32_t>(3214))).value(), Expected<uint32_t>(3214));

    EXPECT_FALSE(testCase.template operator()<uint32_t>({}).has_value());
    EXPECT_EQ(testCase.template operator()<uint32_t>({}).error(), "Nullopt");
}

TEST(ExpectedTest, ValueOrThrowTest)
{
    auto value = [](bool fail) -> Expected<std::string>
    {
        if (fail)
        {
            return unexpected("failure");
        }

        return "all good";
    };

    EXPECT_ANY_THROW(valueOrThrow(value(true), CannotOpenSource));
    EXPECT_EQ(valueOrThrow(value(false), CannotOpenSource), "all good");
}
}
