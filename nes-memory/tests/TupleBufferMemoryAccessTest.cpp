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
#include <limits>
#include <random>


#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>
#include <gtest/gtest.h>

namespace NES
{
/// Helper for running the tests with all the data types below
using TestTypes = ::testing::Types<uint8_t, uint16_t, uint32_t, uint64_t, int8_t, int16_t, int32_t, int64_t>;

template <typename T>
class TupleBufferMemoryAccessTest : public ::testing::Test
{
protected:
    static constexpr size_t minBufferSize = 100;
    static constexpr size_t maxBufferSize = static_cast<const size_t>(1024 * 1024);
    static size_t bufferSize;

    static void SetUpTestSuite()
    {
        std::mt19937 gen(42);
        std::uniform_int_distribution dist(minBufferSize, maxBufferSize);
        bufferSize = dist(gen);
        NES_INFO("Buffer size for this run: {} B", bufferSize);
    }
};

template <typename T>
size_t TupleBufferMemoryAccessTest<T>::bufferSize = 0;

TYPED_TEST_SUITE(TupleBufferMemoryAccessTest, TestTypes);

TYPED_TEST(TupleBufferMemoryAccessTest, FillAndRetrieveFromSpan)
{
    using T = TypeParam;
    auto bufferManager = BufferManager::create(this->bufferSize);
    auto tupleBuffer = bufferManager->getBufferBlocking();
    auto span = tupleBuffer.template getAvailableMemoryArea<T>();

    /// Randomness setup for filling values
    std::random_device rd;
    std::mt19937_64 seedGen{rd()};
    std::uniform_int_distribution<size_t> dis{0, SIZE_MAX};
    const auto randomSeed = dis(seedGen);
    NES_DEBUG("Seed: {}", randomSeed);

    /// Fill with random values
    std::mt19937 gen(randomSeed);
    std::uniform_int_distribution<T> dist(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
    for (auto& elem : span)
    {
        elem = dist(gen);
    }

    /// Check retrieval
    for (const auto& elem : span)
    {
        const T retrieved = elem;
        if constexpr (std::is_floating_point_v<T>)
        {
            EXPECT_TRUE(retrieved == elem || (std::isnan(retrieved) && std::isnan(elem)));
        }
        else
        {
            EXPECT_EQ(retrieved, elem);
        }
    }

    const auto expectedSpanSize = this->bufferSize / sizeof(T);
    EXPECT_EQ(span.size(), expectedSpanSize);
    EXPECT_GE(span.size(), 0);
    EXPECT_LE(span.size(), this->bufferSize);
    EXPECT_FALSE(span.empty());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(span.data()) % alignof(T), 0);
}
}
