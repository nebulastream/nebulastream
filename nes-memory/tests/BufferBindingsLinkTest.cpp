/*
    Minimal link test: verifies that the CXX bridge for nes-buffer-bindings
    links correctly with the Rust umbrella. If this fails to link, the
    umbrella/bridge link ordering is broken.
*/

#include <gtest/gtest.h>
#include <Runtime/BufferManager.hpp>
#include <nes-buffer-bindings/lib.h>

namespace NES {

TEST(BufferBindingsLinkTest, CanCallBufferBindingsFunctions) {
    auto bm = BufferManager::create();
    auto buf = bm->getBufferBlocking();

    // These call through the CXX bridge (C++ → generated wrapper → Rust)
    auto cap = getCapacity(buf);
    EXPECT_GT(cap, 0u);

    auto numTuples = getNumberOfTuples(buf);
    EXPECT_EQ(numTuples, 0u);
}

} // namespace NES
