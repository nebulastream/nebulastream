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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <unordered_set>
#include <vector>
#include <Sequencing/FracturedNumber.hpp>
#include <gtest/gtest.h>

#include "BaseUnitTest.hpp"

using namespace NES;

TEST(FracturedNumberTest, DefaultConstructorIsInvalid)
{
    FracturedNumber fn;
    EXPECT_FALSE(fn.isValid());
    EXPECT_EQ(fn.depth(), 0);
    EXPECT_EQ(fn.toString(), "<invalid>");
}

TEST(FracturedNumberTest, SingleComponentConstructor)
{
    FracturedNumber fn(42);
    EXPECT_TRUE(fn.isValid());
    EXPECT_EQ(fn.depth(), 1);
    EXPECT_EQ(fn[0], 42);
    EXPECT_EQ(fn.toString(), "42");
}

TEST(FracturedNumberTest, VectorConstructor)
{
    FracturedNumber fn(std::vector<uint64_t>{1, 0, 3});
    EXPECT_TRUE(fn.isValid());
    EXPECT_EQ(fn.depth(), 3);
    EXPECT_EQ(fn[0], 1);
    EXPECT_EQ(fn[1], 0);
    EXPECT_EQ(fn[2], 3);
    EXPECT_EQ(fn.toString(), "1.0.3");
}

TEST(FracturedNumberTest, DataPointer)
{
    FracturedNumber fn(std::vector<uint64_t>{5, 10});
    const uint64_t* ptr = fn.data();
    EXPECT_EQ(ptr[0], 5);
    EXPECT_EQ(ptr[1], 10);
}

TEST(FracturedNumberTest, EqualitySameDepth)
{
    EXPECT_EQ(FracturedNumber(1), FracturedNumber(1));
    EXPECT_NE(FracturedNumber(1), FracturedNumber(2));

    FracturedNumber a(std::vector<uint64_t>{1, 2});
    FracturedNumber b(std::vector<uint64_t>{1, 2});
    FracturedNumber c(std::vector<uint64_t>{1, 3});
    EXPECT_EQ(a, b);
    EXPECT_NE(a, c);
}

TEST(FracturedNumberTest, ComparisonSameDepth)
{
    EXPECT_LT(FracturedNumber(1), FracturedNumber(2));
    EXPECT_GT(FracturedNumber(3), FracturedNumber(2));

    FracturedNumber a(std::vector<uint64_t>{1, 0});
    FracturedNumber b(std::vector<uint64_t>{1, 1});
    FracturedNumber c(std::vector<uint64_t>{2, 0});
    EXPECT_LT(a, b);
    EXPECT_LT(b, c);
    EXPECT_LT(a, c);
}

TEST(FracturedNumberTest, ComparisonDifferentDepthThrows)
{
    FracturedNumber a(1);
    FracturedNumber b(std::vector<uint64_t>{1, 0});
    ASSERT_DEATH_DEBUG((void)(a == b), "");
    ASSERT_DEATH_DEBUG((void)(a < b), "");
}

TEST(FracturedNumberTest, WithSubLevel)
{
    FracturedNumber fn(1);
    auto sub = fn.withSubLevel(5);
    EXPECT_EQ(sub.depth(), 2);
    EXPECT_EQ(sub[0], 1);
    EXPECT_EQ(sub[1], 5);
    EXPECT_EQ(sub.toString(), "1.5");

    auto subsub = sub.withSubLevel(3);
    EXPECT_EQ(subsub.depth(), 3);
    EXPECT_EQ(subsub.toString(), "1.5.3");
}

TEST(FracturedNumberTest, Incremented)
{
    FracturedNumber fn(std::vector<uint64_t>{1, 0});
    auto inc = fn.incremented();
    EXPECT_EQ(inc.toString(), "1.1");

    FracturedNumber fn2(5);
    auto inc2 = fn2.incremented();
    EXPECT_EQ(inc2.toString(), "6");
}

TEST(FracturedNumberTest, HashWorks)
{
    std::unordered_set<FracturedNumber> set;
    set.insert(FracturedNumber(1));
    set.insert(FracturedNumber(2));
    set.insert(FracturedNumber(1));
    EXPECT_EQ(set.size(), 2);
}

TEST(FracturedNumberTest, GetComponents)
{
    FracturedNumber fn(std::vector<uint64_t>{3, 7, 11});
    const auto& comps = fn.getComponents();
    EXPECT_EQ(comps.size(), 3);
    EXPECT_EQ(comps[0], 3);
    EXPECT_EQ(comps[1], 7);
    EXPECT_EQ(comps[2], 11);
}
