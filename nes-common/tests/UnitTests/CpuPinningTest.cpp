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

#include <thread>
#include <vector>
#include <sched.h>
#include <gtest/gtest.h>
#include <CpuPinning.hpp>

namespace NES
{
namespace
{
std::vector<int> affinity()
{
    cpu_set_t set;
    sched_getaffinity(0, sizeof(set), &set);
    std::vector<int> cpus;
    for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
    {
        if (CPU_ISSET(cpu, &set))
        {
            cpus.push_back(cpu);
        }
    }
    return cpus;
}
}

TEST(CpuPinningTest, ParsesCpuLists)
{
    EXPECT_EQ(CpuPinning::parseCpuList("0-3,8,10-11"), (std::vector{0, 1, 2, 3, 8, 10, 11}));
    EXPECT_EQ(CpuPinning::parseCpuList("5,1-2,2"), (std::vector{1, 2, 5}));
    EXPECT_EQ(CpuPinning::parseCpuList(""), std::vector<int>{});
    EXPECT_FALSE(CpuPinning::parseCpuList("3-1"));
    EXPECT_FALSE(CpuPinning::parseCpuList("a"));
    EXPECT_FALSE(CpuPinning::parseCpuList("1-"));
}

TEST(CpuPinningTest, PerformanceThreadsGetACpuOfTheirOwnAndMiscThreadsShare)
{
    const auto allowed = affinity();
    ASSERT_FALSE(allowed.empty());
    CpuPinning::enable({allowed.front()}, allowed);
    std::vector<int> first;
    std::vector<int> second;
    std::vector<int> afterRelease;
    std::vector<int> misc;
    std::thread(
        [&]
        {
            CpuPinning::pinThisThread(CpuPinning::Role::Performance);
            first = affinity();
            /// The only performance CPU is taken, so a second performance thread shares all of them
            std::thread(
                [&]
                {
                    CpuPinning::pinThisThread(CpuPinning::Role::Performance);
                    second = affinity();
                })
                .join();
        })
        .join();
    /// The first thread ended and released its CPU
    std::thread(
        [&]
        {
            CpuPinning::pinThisThread(CpuPinning::Role::Performance);
            afterRelease = affinity();
            CpuPinning::pinThisThread(CpuPinning::Role::Misc);
            misc = affinity();
        })
        .join();
    EXPECT_EQ(first, std::vector{allowed.front()});
    EXPECT_EQ(second, std::vector{allowed.front()});
    EXPECT_EQ(afterRelease, std::vector{allowed.front()});
    EXPECT_EQ(misc, allowed);
}
}
