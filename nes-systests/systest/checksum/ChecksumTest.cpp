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

#include "Checksum.hpp"

#include <barrier>
#include <cstddef>
#include <thread>
#include <vector>
#include <gtest/gtest.h>

TEST(ChecksumTest, BasicFunctionality)
{
    Checksum checksum1;

    checksum1.add(R"(1,10,100,1000,10000,100000,1000000,10000000
2,20,200,2000,20000,200000,2000000,20000000
3,30,300,3000,30000,300000,3000000,30000000
4,40,400,4000,40000,400000,4000000,40000000
5,50,500,5000,50000,500000,5000000,50000000
6,60,600,6000,60000,600000,6000000,60000000
7,70,700,7000,70000,700000,7000000,70000000
8,80,800,8000,80000,800000,8000000,80000000
9,90,900,9000,90000,900000,9000000,90000000
10,100,1000,10000,100000,1000000,10000000,100000000
11,110,1100,11000,110000,1100000,11000000,110000000
12,120,1200,12000,120000,1200000,12000000,120000000
13,130,1300,13000,130000,1300000,13000000,130000000
14,140,1400,14000,140000,1400000,14000000,140000000
15,150,1500,15000,150000,1500000,15000000,150000000
16,160,1600,16000,160000,1600000,16000000,160000000
17,170,1700,17000,170000,1700000,17000000,170000000
)");

    EXPECT_EQ(checksum1.numberOfTuples, 17);
    EXPECT_EQ(checksum1.checksum, 38502);
}

TEST(ChecksumTest, EqualCheck)
{
    constexpr std::string_view testData1 = R"(
12,3,1180
1,1,1240
5,7,1350
12,5,1475
4,6,1480
12,4,1501
3,10,1650
5,8,1750
20,102,1987
1,9,1999
20,13,2010
)";
    constexpr std::string_view testData2 = R"(
12,3,1180
1,1,1240
5,7,1350
12,5,1475
4,6,1480
12,4,1501
3,10,1650
5,8,1750
20,12,1987
1,9,3333
20,13,2010
)";
    Checksum checksum1;
    checksum1.add(testData2);
    checksum1.add(testData2);
    checksum1.add(testData2);

    Checksum checksum2;
    checksum2.add(testData2);
    checksum2.add(testData2);
    checksum2.add(testData2);

    Checksum checksum3;
    checksum3.add(testData2);
    checksum3.add(testData2);
    checksum3.add(testData1);

    EXPECT_EQ(checksum1, checksum2);
    EXPECT_EQ(checksum1.numberOfTuples, checksum3.numberOfTuples);
    EXPECT_NE(checksum1, checksum3);
}

TEST(ChecksumTest, MultiThreaded)
{
    constexpr size_t numberOfThreads = 8;
    constexpr size_t writesPerThread = 1000000;

    constexpr std::string_view testData1 = "12,3,1180\n";
    constexpr std::string_view testData2 = "12,3123123,1180\n";

    Checksum checksum1;
    Checksum checksum2;

    std::vector<std::jthread> threads{};
    std::barrier barrier(numberOfThreads);
    threads.reserve(numberOfThreads);

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
                        checksum1.add(testData1);
                    }
                    else
                    {
                        checksum1.add(testData2);
                    }
                }
            });
    }
    threads.clear();

    for (size_t i = 0; i < numberOfThreads * writesPerThread; i++)
    {
        if (i % 2 == 0)
        {
            checksum2.add(testData1);
        }
        else
        {
            checksum2.add(testData2);
        }
    }

    EXPECT_EQ(checksum2, checksum1);
}
