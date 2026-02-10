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

#include "Setsum.hpp"

#include <barrier>
#include <thread>
#include <vector>
#include <gtest/gtest.h>

TEST(SetsumTest, BasicFunctionality)
{
    Setsum setsum1;

    // Add some test data
    setsum1.add("apple");
    setsum1.add("banana");
    setsum1.add("cherry");

    // Verify all columns have non-zero values (highly likely with good hash)
    bool hasNonZero = false;
    for (size_t i = 0; i < setsum1.columns.size(); i++)
    {
        if (setsum1.columns.at(i).load() != 0)
        {
            hasNonZero = true;
            break;
        }
    }
    EXPECT_TRUE(hasNonZero);

    // Create another setsum with same data
    Setsum setsum2;
    setsum2.add("apple");
    setsum2.add("banana");
    setsum2.add("cherry");

    EXPECT_EQ(setsum1, setsum2);
}

TEST(SetsumTest, EqualCheck)
{
    constexpr std::string_view testData1 = "test_data_1";
    constexpr std::string_view testData2 = "test_data_2";
    constexpr std::string_view testData3 = "test_data_3";

    Setsum setsum1;
    setsum1.add(testData1);
    setsum1.add(testData2);
    setsum1.add(testData3);

    Setsum setsum2;
    setsum2.add(testData1);
    setsum2.add(testData2);
    setsum2.add(testData3);

    Setsum setsum3;
    setsum3.add(testData1);
    setsum3.add(testData2);
    setsum3.add("different_data");

    EXPECT_EQ(setsum1, setsum2);
    EXPECT_NE(setsum1, setsum3);
}

/// CRITICAL TEST: Order-agnostic property
/// This verifies that add(A); add(B) == add(B); add(A)
TEST(SetsumTest, OrderAgnostic)
{
    constexpr std::string_view item1 = "first_item";
    constexpr std::string_view item2 = "second_item";
    constexpr std::string_view item3 = "third_item";

    // Add in order: 1, 2, 3
    Setsum setsum1;
    setsum1.add(item1);
    setsum1.add(item2);
    setsum1.add(item3);

    // Add in different order: 3, 1, 2
    Setsum setsum2;
    setsum2.add(item3);
    setsum2.add(item1);
    setsum2.add(item2);

    // Add in yet another order: 2, 3, 1
    Setsum setsum3;
    setsum3.add(item2);
    setsum3.add(item3);
    setsum3.add(item1);

    // All three should be equal regardless of order
    EXPECT_EQ(setsum1, setsum2);
    EXPECT_EQ(setsum2, setsum3);
    EXPECT_EQ(setsum1, setsum3);
}

/// Test add and remove operations
TEST(SetsumTest, AddRemove)
{
    constexpr std::string_view item1 = "apple";
    constexpr std::string_view item2 = "banana";
    constexpr std::string_view item3 = "cherry";

    Setsum setsum1;
    setsum1.add(item1);
    setsum1.add(item2);
    setsum1.add(item3);
    setsum1.remove(item2); // Remove banana

    Setsum setsum2;
    setsum2.add(item1);
    setsum2.add(item3);

    EXPECT_EQ(setsum1, setsum2);

    // Test removing and re-adding
    Setsum setsum3;
    setsum3.add(item1);
    setsum3.add(item2);
    setsum3.add(item3);
    setsum3.remove(item2);
    setsum3.add(item2); // Re-add banana

    Setsum setsum4;
    setsum4.add(item1);
    setsum4.add(item2);
    setsum4.add(item3);

    EXPECT_EQ(setsum3, setsum4);
}

/// Test that removal works correctly with order independence
TEST(SetsumTest, AddRemoveOrderIndependent)
{
    constexpr std::string_view item1 = "dog";
    constexpr std::string_view item2 = "cat";

    // Add both, remove one
    Setsum setsum1;
    setsum1.add(item1);
    setsum1.add(item2);
    setsum1.remove(item1);

    // Add in reverse order, remove the same one
    Setsum setsum2;
    setsum2.add(item2);
    setsum2.add(item1);
    setsum2.remove(item1);

    EXPECT_EQ(setsum1, setsum2);
}

TEST(SetsumTest, MultiThreaded)
{
    constexpr size_t numberOfThreads = 8;
    constexpr size_t writesPerThread = 100000;

    constexpr std::string_view testData1 = "thread_data_1";
    constexpr std::string_view testData2 = "thread_data_2";

    Setsum setsum1;
    Setsum setsum2;

    std::vector<std::jthread> threads{};
    std::barrier barrier(numberOfThreads);
    threads.reserve(numberOfThreads);

    // Multi-threaded adds to setsum1
    for (size_t threadIdx = 0; threadIdx < numberOfThreads; threadIdx++)
    {
        threads.emplace_back(
            [&]()
            {
                barrier.arrive_and_wait();
                for (size_t i = 0; i < writesPerThread; i++)
                {
                    if (i % 2 == 0)
                    {
                        setsum1.add(testData1);
                    }
                    else
                    {
                        setsum1.add(testData2);
                    }
                }
            });
    }
    threads.clear();

    // Single-threaded adds to setsum2 (same total count)
    for (size_t i = 0; i < numberOfThreads * writesPerThread; i++)
    {
        if (i % 2 == 0)
        {
            setsum2.add(testData1);
        }
        else
        {
            setsum2.add(testData2);
        }
    }

    EXPECT_EQ(setsum2, setsum1);
}

/// Test empty setsum comparison
TEST(SetsumTest, EmptySetsumEqual)
{
    Setsum setsum1;
    Setsum setsum2;

    EXPECT_EQ(setsum1, setsum2);
}

/// Test that adding and removing the same item returns to original state
TEST(SetsumTest, AddRemoveSameItem)
{
    Setsum setsum1;
    Setsum setsum2;

    setsum2.add("temporary_item");
    setsum2.remove("temporary_item");

    EXPECT_EQ(setsum1, setsum2);
}

/// Test that adds setsums and compares with a setsum with equivalent elements
TEST(SetsumTest, AddSetsums)
{
    constexpr std::string_view item1 = "first_item";
    constexpr std::string_view item2 = "second_item";
    constexpr std::string_view item3 = "third_item";

    Setsum setsum1;
    setsum1.add(item1);
    setsum1.add(item2);

    Setsum setsum2;
    setsum2.add(item3);

    Setsum setsum3;
    setsum3.add(setsum1);
    setsum3.add(setsum2);

    Setsum setsum4;
    setsum4.add(item1);
    setsum4.add(item2);
    setsum4.add(item3);

    EXPECT_EQ(setsum3, setsum4);
}

/// Test that removes setsums and compares with a setsum with equivalent elements
TEST(SetsumTest, RemoveSetsums)
{
    constexpr std::string_view item1 = "first_item";
    constexpr std::string_view item2 = "second_item";
    constexpr std::string_view item3 = "third_item";


    Setsum setsum1;
    setsum1.add(item1);
    setsum1.add(item2);
    setsum1.add(item3);

    Setsum setsum2;
    setsum2.add(item3);

    setsum1.remove(setsum2);

    Setsum setsum3;
    setsum3.add(item1);
    setsum3.add(item2);

    EXPECT_EQ(setsum1, setsum3);
}

/// Test that checks adding setsums works in a multithreaded situation
TEST(SetsumTest, MultiThreadedSetsums)
{
    constexpr size_t numberOfThreads = 8;
    constexpr size_t writesPerThread = 100000;

    constexpr std::string_view testData1 = "thread_data_1";
    constexpr std::string_view testData2 = "thread_data_2";

    Setsum setsum1;
    setsum1.add(testData1);
    Setsum setsum2;
    setsum1.add(testData2);

    Setsum setsum3;
    Setsum setsum4;

    std::vector<std::jthread> threads{};
    std::barrier barrier(numberOfThreads);
    threads.reserve(numberOfThreads);

    // Multi-threaded adds to setsum3
    for (size_t threadIdx = 0; threadIdx < numberOfThreads; threadIdx++)
    {
        threads.emplace_back(
            [&]()
            {
                barrier.arrive_and_wait();
                for (size_t i = 0; i < writesPerThread; i++)
                {
                    if (i % 2 == 0)
                    {
                        setsum3.add(setsum1);
                    }
                    else
                    {
                        setsum3.add(setsum2);
                    }
                }
            });
    }
    threads.clear();

    // Single-threaded adds to setsum4 (same total count)
    for (size_t i = 0; i < numberOfThreads * writesPerThread; i++)
    {
        if (i % 2 == 0)
        {
            setsum4.add(setsum1);
        }
        else
        {
            setsum4.add(setsum2);
        }
    }

    EXPECT_EQ(setsum3, setsum4);
}

/// Testing add and remove functions with input as setsums
TEST(SetsumTest, AddRemoveSetsums)
{
    constexpr std::string_view item1 = "apple";
    constexpr std::string_view item2 = "banana";
    constexpr std::string_view item3 = "cherry";

    Setsum setsum1;
    setsum1.add(item1);
    setsum1.add(item2);
    setsum1.add(item3);

    Setsum setsum2;
    setsum2.add(item2);

    Setsum setsum3;
    setsum3.add(setsum1);
    setsum3.remove(setsum2);

    Setsum setsum4;
    setsum4.add(item1);
    setsum4.add(item3);

    EXPECT_EQ(setsum3, setsum4);

    // Test removing and re-adding
    Setsum setsum5;
    setsum5.add(setsum1);
    setsum5.remove(setsum2);
    setsum5.add(setsum2); // Re-add banana

    EXPECT_EQ(setsum5, setsum1);
}