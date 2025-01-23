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

#include <memory>
#include <optional>
#include <vector>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES::Memory
{

/// Tests that the fixed size buffer pool is kept alive until all inflight buffers have been released.
/// All buffers are returned to the global buffer manager.
TEST(BufferTest, FixedSizeBuffers)
{
    std::vector<TupleBuffer> inFlight;
    std::weak_ptr<AbstractBufferProvider> weak;
    auto global = BufferManager::create(1024, 10);
    {
        auto fixed = global->createFixedSizeBufferPool(8).value();
        weak = fixed;
        EXPECT_EQ(fixed.use_count(), 1);
        inFlight.emplace_back(fixed->getBufferBlocking());
        EXPECT_EQ(fixed.use_count(), 2);
        fixed->destroy();
    }
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(global->getAvailableBuffers(), 9);
    inFlight.clear();
    EXPECT_EQ(global->getAvailableBuffers(), 10);
    EXPECT_TRUE(weak.expired());
}

TEST(BufferTest, MultipleFixedSizeBuffers)
{
    std::vector<TupleBuffer> inFlight;
    auto global = BufferManager::create(1024, 10);
    {
        auto fixed = global->createFixedSizeBufferPool(8).value();
        inFlight.emplace_back(fixed->getBufferBlocking());
        fixed->destroy();
    }

    EXPECT_EQ(global->createFixedSizeBufferPool(10), std::nullopt);
    EXPECT_TRUE(global->createFixedSizeBufferPool(9).has_value());
    inFlight.clear();
    EXPECT_TRUE(global->createFixedSizeBufferPool(10).has_value());
}

}
