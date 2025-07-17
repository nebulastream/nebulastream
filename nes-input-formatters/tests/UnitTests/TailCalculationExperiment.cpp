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

#include <atomic>
#include <chrono>
#include <format>
#include <iostream>
#include <latch>
#include <random>
#include <thread>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>


void run_experiment(const size_t RB_WIDTH, const size_t NUM_ITERATIONS)
{
    // Create writer threads
    std::vector<std::jthread> writers;
    std::vector<uint8_t> tailVec(RB_WIDTH);

    std::latch completionLatch(RB_WIDTH);
    std::atomic<uint64_t> tailCounter(0);

    for (size_t i = 0; i < RB_WIDTH; ++i)
    {
        writers.emplace_back(
            [NUM_ITERATIONS, RB_WIDTH, &tailVec, i, &completionLatch, &tailCounter]()
            {
                size_t iterationCounter = 0;
                while (iterationCounter < NUM_ITERATIONS)
                {
                    const auto currentVecIdx = i + (iterationCounter * RB_WIDTH);
                    ++iterationCounter;

                    auto currentTail = tailCounter.load();
                    /// If the current SN is out of range, recalculate the tail and check again
                    /// -> in the actual implementation, we would check only once more on the return value
                    while ((currentVecIdx + 1) > (currentTail + RB_WIDTH))
                    {
                        size_t progressCounter = 0;
                        while (progressCounter < RB_WIDTH and tailVec[(currentTail + progressCounter) % RB_WIDTH] == (((currentTail + progressCounter) / RB_WIDTH) + 1) % 2)
                        {
                            ++progressCounter;
                        }
                        const auto newTail = currentTail + progressCounter;
                        tailCounter.compare_exchange_strong(currentTail, newTail);
                    }
                    const auto compareOddEven = ((currentVecIdx / RB_WIDTH) + 1) % 2;
                    tailVec[currentVecIdx % RB_WIDTH] = compareOddEven;
                }
                completionLatch.count_down();
            });
    }
    completionLatch.wait();
    // fmt::println("tailVec: {}", fmt::join(tailVec, ","));
    const auto finalTail = tailCounter.load();

    size_t progressCounter = 0;
    while (progressCounter < RB_WIDTH and tailVec[(finalTail + progressCounter) % RB_WIDTH] == (((finalTail + progressCounter) / RB_WIDTH) + 1) % 2)
    {
        ++progressCounter;
    }

    fmt::println("Final tail: {}", finalTail);
    fmt::println("Adjusted final tail: {}", finalTail + progressCounter);
}


int main()
{
    for (int i= 0; i < 100; ++i)
    {
        run_experiment(32, 1000);
    }
    return 0;
}