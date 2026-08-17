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
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <gtest/gtest.h>
#include "Runtime/UnpooledBufferManager.hpp"

namespace NES
{

TEST(MallocUnpooledBufferManagerTest, AllocatesAndFreesEachBufferIndependently)
{
    constexpr size_t totalMemoryInBytes = 1024;
    constexpr uint32_t poolBufferSize = 512;
    constexpr BufferAlignment alignment{64};
    const auto bufferManager = BufferManager::create(
        totalMemoryInBytes,
        0.5,
        alignment,
        poolBufferSize,
        std::make_shared<NesDefaultMemoryAllocator>(),
        UnpooledBufferManagerType::MALLOC);

    auto buffer = bufferManager->getUnpooledBuffer(256);
    ASSERT_TRUE(buffer.has_value());
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(buffer->getAvailableMemoryArea().data()) % alignment.getRawValue(), 0);
    EXPECT_FALSE(bufferManager->getUnpooledBuffer(256).has_value());

    EXPECT_EQ(bufferManager->getNumOfUnpooledBuffers(), 1);

    buffer.reset();
    EXPECT_EQ(bufferManager->getNumOfUnpooledBuffers(), 0);
    EXPECT_TRUE(bufferManager->getUnpooledBuffer(256).has_value());
}

}
