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
#include <memory>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/MemoryUtils.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES
{
namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 1024;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 16;
constexpr BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.5;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 4 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

std::shared_ptr<BufferManager> createBufferManager()
{
    return BufferManager::create(
        TOTAL_MEMORY_IN_BYTES,
        UNPOOLED_MEMORY_FRACTION,
        BUFFER_ALIGNMENT,
        POOLED_BUFFER_SIZE,
        std::make_shared<NesDefaultMemoryAllocator>());
}
}

TEST(TupleBufferSequenceRangeTest, SingleSequenceNumberReplacesTheRange)
{
    const auto bufferManager = createBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    buffer.setSequenceRange(SequenceNumber(10), 7);
    buffer.setSequenceNumber(SequenceNumber(3));
    EXPECT_EQ(buffer.getSequenceNumber(), SequenceNumber(3));
    EXPECT_EQ(buffer.getSequenceRangeOffset(), 0);
}

TEST(TupleBufferSequenceRangeTest, DeepCopyKeepsTheRange)
{
    const auto bufferManager = createBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    buffer.setSequenceRange(SequenceNumber(10), 7);
    const auto copy = deepCopyBuffer(buffer, *bufferManager);
    EXPECT_EQ(copy.getSequenceNumber(), SequenceNumber(10));
    EXPECT_EQ(copy.getSequenceRangeOffset(), 7);
}

#ifndef NO_ASSERT
TEST(TupleBufferSequenceRangeTest, RangeStartingBelowTheFirstSequenceNumberIsRejected)
{
    const auto bufferManager = createBufferManager();
    auto buffer = bufferManager->getBufferBlocking();
    EXPECT_DEATH(buffer.setSequenceRange(SequenceNumber(3), 3), "");
}
#endif

}
