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
#include <ctime>
#include <memory>
#include <optional>
#include <random>
#include <thread>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>
#include <ErrorHandling.hpp>

namespace NES::Memory
{

namespace
{
/// Stores the needed size and the created tuple buffer, so that we can later check if all allocations have worked
struct TestData
{
    explicit TestData(const size_t neededSize) : neededSize(neededSize) { }
    size_t neededSize;
    std::optional<TupleBuffer> buffer;
};
std::vector<TestData> createRandomSizeAllocations(const size_t numberOfRandomAllocationSizes, const size_t min, const size_t max)
{
    PRECONDITION(min < max, "min {} > max {}", min, max);

    /// Getting a "random" seed and logging the seed to be able to rerun the test with the same random values
    const auto seed = static_cast<unsigned>(std::time(nullptr));

    /// Generate random numbers
    std::vector<TestData> randomSizeAllocations;
    randomSizeAllocations.reserve(numberOfRandomAllocationSizes);

    std::mt19937 generator(seed);
    std::uniform_int_distribution<size_t> distribution(min, max);
    for (size_t i = 0; i < numberOfRandomAllocationSizes; ++i)
    {
        randomSizeAllocations.emplace_back(distribution(generator));
    }

    return randomSizeAllocations;
}


void runAllocations(
    const size_t numberOfRandomAllocationSizes,
    const size_t minAllocationSize,
    const size_t maxAllocationSize,
    const size_t numberOfThreads)
{
    /// Creating component under test. We do not care about the bufferSize and the number of buffers, as we test getUnpooledBuffer()
    const auto bufferManager = BufferManager::create(1, 1);

    /// Creating random allocation sizes
    auto randomAllocations = createRandomSizeAllocations(numberOfRandomAllocationSizes, minAllocationSize, maxAllocationSize);

    /// Running all allocations concurrently for #numberOfThreads
    std::vector<std::thread> threads;
    const auto chunkSize = numberOfThreads / numberOfRandomAllocationSizes;
    for (auto& allocation : randomAllocations)
    {
        allocation.buffer = bufferManager->getUnpooledBuffer(allocation.neededSize);
    }
    for (size_t i = 0; i < numberOfThreads; ++i)
    {
        size_t start = i * chunkSize;
        size_t end = (i == numberOfThreads - 1) ? randomAllocations.size() : start + chunkSize;

        threads.emplace_back(
            [&randomAllocations, &bufferManager](const size_t start, const size_t end)
            {
                for (size_t i = start; i < end; ++i)
                {
                    auto& allocation = randomAllocations[i];
                    allocation.buffer = bufferManager->getUnpooledBuffer(allocation.neededSize);
                }
            },
            start,
            end);
    }

    /// Wait for all threads to finish
    for (auto& thread : threads)
    {
        thread.join();
    }

    /// Checking if the allocation were successful
    for (const auto& allocation : randomAllocations)
    {
        ASSERT_TRUE(allocation.buffer.has_value());
        ASSERT_EQ(allocation.buffer.value().getBufferSize(), allocation.neededSize);
    }
}


}


TEST(UnpooledBufferTests, SingleUnpooledBuffer)
{
    constexpr auto numberOfRandomAllocationSizes = 1;
    constexpr auto minAllocationSize = 10;
    constexpr auto maxAllocationSize = 100 * 1024;
    constexpr auto numberOfThreads = 1;
    runAllocations(numberOfRandomAllocationSizes, minAllocationSize, maxAllocationSize, numberOfThreads);
}

TEST(UnpooledBufferTests, MultipleUnpooledBuffer)
{
    constexpr auto numberOfRandomAllocationSizes = 1000;
    constexpr auto minAllocationSize = 10; /// 10 B
    constexpr auto maxAllocationSize = 100 * 1024; /// 100 KiB
    constexpr auto numberOfThreads = 1;
    runAllocations(numberOfRandomAllocationSizes, minAllocationSize, maxAllocationSize, numberOfThreads);
}


TEST(UnpooledBufferTests, MultipleUnpooledBufferLargeSizes)
{
    constexpr auto numberOfRandomAllocationSizes = 1000;
    constexpr auto minAllocationSize = 10 * 1024; /// 10 KiB
    constexpr auto maxAllocationSize = 100 * 1024 * 1024; /// 100 MiB
    constexpr auto numberOfThreads = 1;
    runAllocations(numberOfRandomAllocationSizes, minAllocationSize, maxAllocationSize, numberOfThreads);
}


TEST(UnpooledBufferTests, MultipleUnpooledBufferMultithreaded)
{
    constexpr auto numberOfRandomAllocationSizes = 100 * 1000;
    constexpr auto minAllocationSize = 10 * 1024; /// 1 KiB
    constexpr auto maxAllocationSize = 500 * 1024; /// 500 KiB
    constexpr auto numberOfThreads = 8;
    runAllocations(numberOfRandomAllocationSizes, minAllocationSize, maxAllocationSize, numberOfThreads);
}

}
