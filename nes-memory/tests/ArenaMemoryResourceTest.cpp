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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <vector>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Spill/ArenaMemoryResource.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <gtest/gtest.h>

namespace NES
{

namespace
{
/// Common alignment used across the tests (one cache line); allocations are page-rounded internally anyway.
constexpr size_t TestAlignment = 64;
}

/// Tests for ArenaMemoryResource, the per-slice spill unit. The core guarantees are:
/// (1) after an evict/reload cycle the data is intact at the *same* virtual address (so live pointers survive), and
/// (2) live-byte accounting reflects allocations/deallocations. Both the Anon (vmcache-faithful) and FileBacked modes
/// are exercised.
class ArenaMemoryResourceTest : public ::testing::TestWithParam<ArenaMemoryResource::Mode>
{
protected:
    std::filesystem::path backingFile;

    void SetUp() override
    {
        /// Unique backing file per test so parallel test execution does not collide. The parameterized suite/test
        /// names contain '/' (e.g. "Modes/ArenaMemoryResourceTest"), so sanitize them into a flat file name.
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string name = std::string("nes-arena-") + info->test_suite_name() + "-" + info->name() + ".spill";
        std::ranges::replace(name, '/', '_');
        backingFile = std::filesystem::temp_directory_path() / name;
    }

    void TearDown() override
    {
        std::error_code ec;
        std::filesystem::remove(backingFile, ec);
    }
};

INSTANTIATE_TEST_SUITE_P(
    Modes,
    ArenaMemoryResourceTest,
    ::testing::Values(ArenaMemoryResource::Mode::Anon, ArenaMemoryResource::Mode::FileBacked),
    [](const auto& info) { return info.param == ArenaMemoryResource::Mode::Anon ? "Anon" : "FileBacked"; });

/// Data written into an allocation survives an evict/reload cycle and stays at the same address.
TEST_P(ArenaMemoryResourceTest, DataAndAddressSurviveEvictReload)
{
    ArenaMemoryResource arena(GetParam(), backingFile.string());

    constexpr size_t numBytes = size_t{64} * 1024;
    constexpr uint8_t patternStride = 31;
    constexpr uint8_t patternOffset = 7;
    auto* region = static_cast<uint8_t*>(arena.allocate(numBytes, TestAlignment));
    ASSERT_NE(region, nullptr);

    /// Write a deterministic pattern.
    for (size_t i = 0; i < numBytes; ++i)
    {
        region[i] = static_cast<uint8_t>((i * patternStride + patternOffset) & 0xFFU);
    }

    EXPECT_FALSE(arena.isEvicted());
    arena.evict();
    EXPECT_TRUE(arena.isEvicted());
    arena.reload();
    EXPECT_FALSE(arena.isEvicted());

    /// Same virtual address, identical contents.
    for (size_t i = 0; i < numBytes; ++i)
    {
        ASSERT_EQ(region[i], static_cast<uint8_t>((i * patternStride + patternOffset) & 0xFFU)) << "mismatch at byte " << i;
    }
    EXPECT_GT(arena.bytesWritten(), 0U);
    EXPECT_GT(arena.bytesRead(), 0U);

    arena.deallocate(region, numBytes, TestAlignment);
}

/// Multiple allocations all survive a single evict/reload, each at its own stable address.
TEST_P(ArenaMemoryResourceTest, MultipleRegionsSurvive)
{
    ArenaMemoryResource arena(GetParam(), backingFile.string());
    std::vector<uint8_t*> regions;
    constexpr size_t numRegions = 5;
    constexpr size_t regionBytes = size_t{8} * 1024;
    for (size_t region = 0; region < numRegions; ++region)
    {
        auto* ptr = static_cast<uint8_t*>(arena.allocate(regionBytes, TestAlignment));
        std::memset(ptr, static_cast<int>(region + 1), regionBytes);
        regions.push_back(ptr);
    }

    arena.evict();
    arena.reload();

    for (size_t region = 0; region < numRegions; ++region)
    {
        for (size_t i = 0; i < regionBytes; ++i)
        {
            ASSERT_EQ(regions[region][i], static_cast<uint8_t>(region + 1)) << "region " << region << " byte " << i;
        }
    }
    for (auto* ptr : regions)
    {
        arena.deallocate(ptr, regionBytes, TestAlignment);
    }
}

/// Live-byte accounting tracks allocations and deallocations (page-rounded).
TEST_P(ArenaMemoryResourceTest, LiveByteAccounting)
{
    ArenaMemoryResource arena(GetParam(), backingFile.string());
    EXPECT_EQ(arena.liveBytes(), 0U);

    constexpr size_t smallAllocBytes = 1000; /// rounds up to one page
    constexpr size_t largeAllocBytes = 9000; /// spans more than one page

    auto* first = arena.allocate(smallAllocBytes, TestAlignment);
    const auto afterFirst = arena.liveBytes();
    EXPECT_GE(afterFirst, smallAllocBytes);

    auto* second = arena.allocate(largeAllocBytes, TestAlignment);
    EXPECT_GT(arena.liveBytes(), afterFirst);

    arena.deallocate(first, smallAllocBytes, TestAlignment);
    arena.deallocate(second, largeAllocBytes, TestAlignment);
    EXPECT_EQ(arena.liveBytes(), 0U);
}

/// evict()/reload() are idempotent: redundant calls are no-ops and do not corrupt state.
TEST_P(ArenaMemoryResourceTest, EvictReloadIdempotent)
{
    ArenaMemoryResource arena(GetParam(), backingFile.string());
    constexpr size_t allocBytes = 4096;
    constexpr size_t patternLength = 256;
    auto* ptr = static_cast<uint8_t*>(arena.allocate(allocBytes, TestAlignment));
    std::iota(ptr, ptr + patternLength, uint8_t{0});

    arena.evict();
    arena.evict(); /// no-op
    arena.reload();
    arena.reload(); /// no-op

    for (size_t i = 0; i < patternLength; ++i)
    {
        ASSERT_EQ(ptr[i], static_cast<uint8_t>(i));
    }
    arena.deallocate(ptr, allocBytes, TestAlignment);
}

/// A BufferManager backed by the arena keeps its buffers' contents and addresses across an evict/reload, proving the
/// arena works as the per-slice allocation backend (the real integration path for spilling operator state).
TEST_P(ArenaMemoryResourceTest, BufferManagerBackedByArenaSurvivesEvictReload)
{
    auto arena = std::make_shared<ArenaMemoryResource>(GetParam(), backingFile.string());
    constexpr uint32_t bufferSize = 4096;
    constexpr uint32_t numBuffers = 4;
    constexpr uint8_t patternStride = 13;
    constexpr uint8_t patternOffset = 5;
    auto bm = BufferManager::create(bufferSize, numBuffers, arena, TestAlignment);

    auto buffer = bm->getBufferBlocking();
    auto* raw = buffer.getAvailableMemoryArea<uint8_t>().data();
    auto mem = buffer.getAvailableMemoryArea<uint8_t>();
    for (size_t i = 0; i < mem.size(); ++i)
    {
        mem[i] = static_cast<uint8_t>((i * patternStride + patternOffset) & 0xFFU);
    }

    arena->evict();
    arena->reload();

    /// Same backing address; contents intact.
    EXPECT_EQ(buffer.getAvailableMemoryArea<uint8_t>().data(), raw);
    auto memAfter = buffer.getAvailableMemoryArea<uint8_t>();
    for (size_t i = 0; i < memAfter.size(); ++i)
    {
        ASSERT_EQ(memAfter[i], static_cast<uint8_t>((i * patternStride + patternOffset) & 0xFFU)) << "byte " << i;
    }

    /// Release before destroying the BufferManager (its destructor asserts no outstanding buffers).
    buffer = TupleBuffer();
    bm->destroy();
}

}
