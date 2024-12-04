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

#include <Util/Ranges.hpp>

#include <cstddef>
#include <list>
#include <ranges>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace NES
{
TEST(EnumerateViewTest, EnumerateVector)
{
    std::vector data = {10, 20, 30, 40};
    std::size_t index = 0;
    for (const auto& [i, value] : data | views::enumerate)
    {
        EXPECT_EQ(i, index++);
        EXPECT_EQ(value, data[i]);
    }
}

TEST(EnumerateViewTest, EnumerateEmptyContainer)
{
    std::vector<int> emptyData;
    for (const auto& [i, value] : emptyData | views::enumerate)
    {
        (void)i;
        FAIL() << "Enumeration should not enter loop for empty container.";
    }
}

TEST(EnumerateViewTest, EnumerateString)
{
    std::string data = "abcd";
    std::size_t index = 0;
    for (const auto& [i, value] : data | views::enumerate)
    {
        EXPECT_EQ(i, index++);
        EXPECT_EQ(value, data[i]);
    }
}

TEST(EnumerateViewTest, EnumerateModification)
{
    std::vector<int> data = {1, 2, 3, 4};
    for (auto [i, value] : data | views::enumerate)
    {
        data[i] += static_cast<int>(i);
    }
    EXPECT_EQ(data, (std::vector<int>{1, 3, 5, 7}));
}

TEST(EnumerateViewTest, EnumerateDifferentTypes)
{
    std::vector<std::string> data = {"alpha", "beta", "gamma"};
    std::size_t index = 0;
    for (const auto& [i, value] : data | views::enumerate)
    {
        EXPECT_EQ(i, index++);
        if (i == 0)
        {
            EXPECT_EQ(value, "alpha");
        }
        if (i == 1)
        {
            EXPECT_EQ(value, "beta");
        }
        if (i == 2)
        {
            EXPECT_EQ(value, "gamma");
        }
    }
}

TEST(RangesToTest, ConvertVectorToList)
{
    std::vector data = {1, 2, 3, 4, 5};
    auto result = data | ranges::to<std::list>();
    EXPECT_EQ(result, (std::list<int>{1, 2, 3, 4, 5}));
}

TEST(RangesToTest, ConvertVectorToSet)
{
    std::vector data = {5, 2, 5, 3, 1, 2};
    auto result = data | ranges::to<std::set>();
    EXPECT_EQ(result, (std::set{1, 2, 3, 5}));
}

TEST(RangesToTest, ConvertVectorToUnorderedSet)
{
    std::vector<std::string> data = {"Vector", "Of", "Strings", "Of"};
    auto result = data
        | std::views::transform(
                      [](std::string string) -> std::string
                      {
                          if (string == "Vector")
                          {
                              return "Set";
                          }
                          return string;
                      })
        | ranges::to<std::unordered_set>();

    EXPECT_EQ(result, (std::unordered_set<std::string>{"Set", "Of", "Strings"}));
}

TEST(RangesToTest, ConvertVectorToMap)
{
    std::vector<std::pair<size_t, size_t>> data = {{5, 2}, {2, 1}};
    auto result = data | ranges::to<std::unordered_map>();
    EXPECT_EQ(result, (std::unordered_map<size_t, size_t>{{5, 2}, {2, 1}}));
}

TEST(RangesToTest, ConvertRangeToString)
{
    std::string_view input = "hello world";
    auto result = input | ranges::to<std::string>();
    EXPECT_EQ(result, "hello world");
}

TEST(RangesToTest, TransformAndConvertToVector)
{
    std::vector data = {1, 2, 3, 4};
    auto result = data | std::views::transform([](int value) { return value * value; }) | ranges::to<std::vector>();
    EXPECT_EQ(result, (std::vector{1, 4, 9, 16}));
}

TEST(RangesToTest, FilterAndConvertToList)
{
    std::vector data = {1, 2, 3, 4, 5, 6};
    auto result = data | std::views::filter([](int value) { return value % 2 == 0; }) | ranges::to<std::list>();
    EXPECT_EQ(result, (std::list{2, 4, 6}));
}

TEST(RangesToTest, EmptyInputRangeToVector)
{
    std::vector<int> data;
    auto result = data | ranges::to<std::vector>();
    EXPECT_TRUE(result.empty());
}

TEST(RangesToTest, ConvertStringViewToString)
{
    std::string_view input = "C++23";
    auto result = input | ranges::to<std::string>();
    EXPECT_EQ(result, "C++23");
}
}
