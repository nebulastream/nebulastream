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
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <Runtime/Spill/ArenaMemoryResource.hpp>
#include <Runtime/Spill/SpillManager.hpp>
#include <gtest/gtest.h>

namespace NES
{

namespace
{
constexpr size_t KiB = 1024;
constexpr size_t StateBytes = 64 * KiB;
constexpr size_t Alignment = 64;
constexpr uint64_t PatternStride = 7;
constexpr uint64_t PatternMask = 0xFF;

/// A test-double SpillableState backed by a real ArenaMemoryResource. residentBytes() reports the mapped bytes while
/// resident and 0 once evicted, modelling actual DRAM pressure. A deterministic pattern lets us assert that data
/// survives an evict/reload driven by the SpillManager.
class FakeSpillableState final : public SpillableState
{
public:
    FakeSpillableState(uint64_t coldness, size_t bytes, const std::string& backingFile)
        : coldness(coldness)
        , arena(ArenaMemoryResource::Mode::Anon, backingFile)
        , region(static_cast<uint8_t*>(arena.allocate(bytes, Alignment)))
        , sizeBytes(bytes)
        , mappedBytes(arena.liveBytes())
    {
        for (size_t i = 0; i < bytes; ++i)
        {
            region[i] = patternByte(i);
        }
    }

    [[nodiscard]] size_t residentBytes() const override { return arena.isEvicted() ? 0 : mappedBytes; }

    [[nodiscard]] bool isEvicted() const override { return arena.isEvicted(); }

    void evictState() override { arena.evict(); }

    void reloadState() override { arena.reload(); }

    [[nodiscard]] uint64_t coldnessKey() const override { return coldness; }

    [[nodiscard]] bool patternIntact() const
    {
        for (size_t i = 0; i < sizeBytes; ++i)
        {
            if (region[i] != patternByte(i))
            {
                return false;
            }
        }
        return true;
    }

private:
    [[nodiscard]] uint8_t patternByte(size_t i) const { return static_cast<uint8_t>(((i * PatternStride) + coldness) & PatternMask); }

    uint64_t coldness;
    ArenaMemoryResource arena;
    uint8_t* region{nullptr};
    size_t sizeBytes{0};
    size_t mappedBytes{0};
};
}

class SpillManagerTest : public ::testing::Test
{
protected:
    std::filesystem::path dir;

    void SetUp() override
    {
        const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
        dir = std::filesystem::temp_directory_path() / (std::string("nes-spillmgr-") + info->name());
        std::filesystem::create_directories(dir);
    }

    void TearDown() override
    {
        std::error_code ec;
        std::filesystem::remove_all(dir, ec);
    }

    std::shared_ptr<FakeSpillableState> makeState(uint64_t coldness)
    {
        return std::make_shared<FakeSpillableState>(coldness, StateBytes, (dir / ("s" + std::to_string(coldness) + ".spill")).string());
    }
};

/// residentBytes() sums the footprint of all registered units; unregister stops tracking.
TEST_F(SpillManagerTest, AccountingAndRegistration)
{
    SpillManager mgr(SpillConfiguration{});
    EXPECT_EQ(mgr.registeredCount(), 0U);
    EXPECT_EQ(mgr.residentBytes(), 0U);

    auto a = makeState(1);
    auto b = makeState(2);
    mgr.registerState(a);
    mgr.registerState(b);
    EXPECT_EQ(mgr.registeredCount(), 2U);
    EXPECT_GE(mgr.residentBytes(), 2 * StateBytes);

    mgr.unregisterState(a.get());
    EXPECT_EQ(mgr.registeredCount(), 1U);
    EXPECT_GE(mgr.residentBytes(), StateBytes);
    EXPECT_FALSE(b->isEvicted());
}

/// Proactive eviction removes the coldest units (lowest key) down to the low watermark; survivors keep their data,
/// and an evicted unit's data is restored on pin().
TEST_F(SpillManagerTest, MaybeSpillEvictsColdestToLowWatermark)
{
    SpillConfiguration cfg;
    cfg.enabled = true;
    cfg.stateMemoryBudgetBytes = 5 * StateBytes;
    cfg.highWatermark = 0.9; /// 4.5 units -> over budget at 5 units triggers
    cfg.lowWatermark = 0.5; /// evict down to ~2.5 units
    SpillManager mgr(cfg);

    std::vector<std::shared_ptr<FakeSpillableState>> states;
    for (uint64_t k = 1; k <= 5; ++k)
    {
        states.push_back(makeState(k));
        mgr.registerState(states.back());
    }

    const auto freed = mgr.maybeSpill();
    EXPECT_GT(freed, 0U);
    EXPECT_LE(mgr.residentBytes(), static_cast<size_t>(cfg.stateMemoryBudgetBytes * cfg.lowWatermark));

    /// Coldest (keys 1..3) evicted; warmest (4,5) resident with intact data.
    EXPECT_TRUE(states[0]->isEvicted());
    EXPECT_TRUE(states[1]->isEvicted());
    EXPECT_TRUE(states[2]->isEvicted());
    EXPECT_FALSE(states[3]->isEvicted());
    EXPECT_FALSE(states[4]->isEvicted());
    EXPECT_TRUE(states[3]->patternIntact());

    /// Pin reloads an evicted unit and its data is restored.
    mgr.pin(*states[0]);
    EXPECT_FALSE(states[0]->isEvicted());
    EXPECT_TRUE(states[0]->patternIntact());
    mgr.unpin(*states[0]);
}

/// A pinned unit is never evicted, even when it is the coldest victim.
TEST_F(SpillManagerTest, PinnedStateIsNotEvicted)
{
    SpillConfiguration cfg;
    cfg.enabled = true;
    cfg.stateMemoryBudgetBytes = 5 * StateBytes;
    cfg.highWatermark = 0.9;
    cfg.lowWatermark = 0.5;
    SpillManager mgr(cfg);

    std::vector<std::shared_ptr<FakeSpillableState>> states;
    for (uint64_t k = 1; k <= 5; ++k)
    {
        states.push_back(makeState(k));
        mgr.registerState(states.back());
    }

    mgr.pin(*states[0]); /// pin the coldest
    mgr.maybeSpill();
    EXPECT_FALSE(states[0]->isEvicted()) << "pinned coldest unit must not be evicted";
    mgr.unpin(*states[0]);
}

/// Reactive backstop: evictDownTo evicts the coldest until resident <= target.
TEST_F(SpillManagerTest, EvictDownToTarget)
{
    SpillManager mgr(SpillConfiguration{});
    std::vector<std::shared_ptr<FakeSpillableState>> states;
    for (uint64_t k = 1; k <= 4; ++k)
    {
        states.push_back(makeState(k));
        mgr.registerState(states.back());
    }
    const auto freed = mgr.evictDownTo(2 * StateBytes);
    EXPECT_GT(freed, 0U);
    EXPECT_LE(mgr.residentBytes(), 2 * StateBytes);
    EXPECT_TRUE(states[0]->isEvicted());
}

/// With spilling disabled, maybeSpill never evicts even when over budget.
TEST_F(SpillManagerTest, DisabledDoesNotSpill)
{
    SpillConfiguration cfg;
    cfg.enabled = false;
    cfg.stateMemoryBudgetBytes = StateBytes; /// deliberately tiny
    SpillManager mgr(cfg);

    auto a = makeState(1);
    auto b = makeState(2);
    mgr.registerState(a);
    mgr.registerState(b);

    EXPECT_EQ(mgr.maybeSpill(), 0U);
    EXPECT_FALSE(a->isEvicted());
    EXPECT_FALSE(b->isEvicted());
}

}
