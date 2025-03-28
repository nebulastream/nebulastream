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
}
