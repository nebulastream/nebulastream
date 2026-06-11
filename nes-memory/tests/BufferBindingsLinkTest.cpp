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

/*
    Minimal link test: verifies that the CXX bridge for nes-buffer-bindings
    links correctly with the Rust umbrella. If this fails to link, the
    umbrella/bridge link ordering is broken.
*/

#include <Runtime/BufferManager.hpp>
#include <gtest/gtest.h>
#include <my-buffer-test/lib.h>

namespace NES
{

TEST(BufferBindingsLinkTest, CanCallBufferBindingsFunctions2)
{
    auto bm = BufferManager::create(8192, 4);
    auto buffer = bm->getBufferBlocking();
    auto tc = create_test_case(bm);
    auto id = allocate_buffer_and_store(tc);
    allocate_buffer_and_store(tc);
    allocate_buffer_and_store(tc);
    EXPECT_EQ(allocate_buffer_and_store(tc), -1);
    {
        auto buffer = fromRust(release_buffer(tc, id));
    }
    EXPECT_EQ(allocate_buffer_and_store(tc), 3);
    auto handle = std::jthread(
        [buffer = std::move(buffer)]()
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(400));
            std::ignore = buffer;
        });
    EXPECT_EQ(allocate_buffer_and_store(tc), 4);
}


} /// namespace NES
