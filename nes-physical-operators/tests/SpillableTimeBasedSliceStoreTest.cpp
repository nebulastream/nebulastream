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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/BufferManager.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <Aggregation/SpillableAggregationSlice.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <SliceStore/InMemorySpillBackend.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillableTimeBasedSliceStore.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES::Testing
{

using Entry = std::pair<uint64_t, std::vector<std::byte>>; /// (hash, key+value payload bytes)

/// Increment A: drives SpillableTimeBasedSliceStore directly (single thread) to prove the spill lifetime model:
/// force-spill a COLD slice, trigger it, observe unspill-on-read, and that the backend was used. True end-to-end
/// query correctness is covered out-of-gate by the systest framework.
class SpillableTimeBasedSliceStoreTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SpillableTimeBasedSliceStoreTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup SpillableTimeBasedSliceStoreTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create();
    }

protected:
    static constexpr uint64_t KEY_SIZE = 8;
    static constexpr uint64_t VALUE_SIZE = 8;
    static constexpr uint64_t NUMBER_OF_BUCKETS = 16;
    static constexpr uint64_t PAGE_SIZE = 256;
    static constexpr uint64_t NUMBER_OF_HASH_MAPS = 4;
    /// Tumbling window: one slice == one window, so a created slice's window triggers at watermark > windowEnd.
    static constexpr uint64_t WINDOW_SIZE = 100;
    static constexpr uint64_t WINDOW_SLIDE = 100;

    std::shared_ptr<BufferManager> bufferManager;

    [[nodiscard]] static uint64_t entrySize() { return sizeof(ChainedHashMapEntry) + KEY_SIZE + VALUE_SIZE; }
    [[nodiscard]] static uint64_t entriesPerPage() { return PAGE_SIZE / entrySize(); }

    static CreateNewHashMapSliceArgs makeArgs()
    {
        auto noopCleanup = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(
            std::function<void(nautilus::val<HashMap*>)>([](nautilus::val<HashMap*>) { }));
        return CreateNewHashMapSliceArgs({std::move(noopCleanup)}, KEY_SIZE, VALUE_SIZE, PAGE_SIZE, NUMBER_OF_BUCKETS);
    }

    /// createNewSlice callback that produces a spillable aggregation slice (what the spill-enabled handler emits).
    static std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> spillableSliceFactory()
    {
        return [](const SliceStart sliceStart, const SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        { return {std::make_shared<SpillableAggregationSlice>(sliceStart, sliceEnd, makeArgs(), NUMBER_OF_HASH_MAPS)}; };
    }

    std::unique_ptr<SpillableTimeBasedSliceStore> makeStore(InMemorySpillBackend** backendOut) const
    {
        auto backend = std::make_unique<InMemorySpillBackend>();
        *backendOut = backend.get();
        auto store = std::make_unique<SpillableTimeBasedSliceStore>(
            WINDOW_SIZE,
            WINDOW_SLIDE,
            SliceCacheConfiguration{},
            std::move(backend),
            entriesPerPage(),
            PAGE_SIZE,
            /*softThresholdBytes*/ 1024ULL * 1024ULL,
            /*hardThresholdBytes*/ 2ULL * 1024ULL * 1024ULL);
        store->setBufferProvider(bufferManager);
        return store;
    }

    void populate(const std::shared_ptr<Slice>& slice, const WorkerThreadId workerThreadId, const uint64_t count, const uint64_t offset)
        const
    {
        auto* const spillable = dynamic_cast<SpillableAggregationSlice*>(slice.get());
        auto* const map = dynamic_cast<ChainedHashMap*>(spillable->getHashMapPtrOrCreate(workerThreadId));
        for (uint64_t i = 0; i < count; ++i)
        {
            const uint64_t hash = (offset + i + 1) * 2654435761ULL;
            auto* const entry = static_cast<ChainedHashMapEntry*>(map->insertEntry(hash, bufferManager.get()));
            auto* const payload = reinterpret_cast<std::byte*>(entry) + sizeof(ChainedHashMapEntry);
            const uint64_t key = offset + i;
            const uint64_t value = (i * 7) + 1;
            std::memcpy(payload, &key, sizeof(key));
            std::memcpy(payload + sizeof(key), &value, sizeof(value));
        }
    }

    static std::vector<Entry> sortedEntries(const SpillableAggregationSlice& slice, const WorkerThreadId workerThreadId)
    {
        const auto* const map = dynamic_cast<const ChainedHashMap*>(slice.getHashMapPtr(workerThreadId));
        std::vector<Entry> out;
        if (map != nullptr && map->getNumberOfTuples() > 0)
        {
            const uint64_t payloadSize = entrySize() - sizeof(ChainedHashMapEntry);
            for (uint64_t chain = 0; chain < map->getNumberOfChains(); ++chain)
            {
                for (const auto* entry = map->getStartOfChain(chain); entry != nullptr; entry = entry->next)
                {
                    const auto* const payload = reinterpret_cast<const std::byte*>(entry) + sizeof(ChainedHashMapEntry);
                    out.emplace_back(entry->hash, std::vector<std::byte>(payload, payload + payloadSize));
                }
            }
        }
        std::ranges::sort(out, [](const Entry& a, const Entry& b) { return a.first != b.first ? a.first < b.first : a.second < b.second; });
        return out;
    }

    /// True if any window returned by the trigger map carries a slice ending at `sliceEnd`.
    static bool
    triggeredContains(const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& triggered, const SliceEnd sliceEnd)
    {
        for (const auto& [windowInfo, slices] : triggered)
        {
            for (const auto& slice : slices)
            {
                if (slice->getSliceEnd() == sliceEnd)
                {
                    return true;
                }
            }
        }
        return false;
    }
};

TEST_F(SpillableTimeBasedSliceStoreTest, forcedSpillFreesAndBackendGetsRecord)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    const auto slices = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory());
    ASSERT_EQ(slices.size(), 1U);
    const auto slice = slices[0];
    const auto sliceEnd = slice->getSliceEnd();
    populate(slice, WorkerThreadId(0), 10, 0);

    EXPECT_GT(store->residentBytesOf(*slice), 0U);
    EXPECT_TRUE(store->isSliceResident(sliceEnd));

    store->forceSpill(sliceEnd);

    EXPECT_FALSE(store->isSliceResident(sliceEnd));
    EXPECT_TRUE(backend->contains(sliceEnd, WorkerThreadId(0)));
    EXPECT_EQ(backend->sliceCount(), 1U);
    EXPECT_EQ(store->residentBytesOf(*slice), 0U);
}

TEST_F(SpillableTimeBasedSliceStoreTest, unspillOnTriggerableReadRestoresEntries)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceEnd = slice->getSliceEnd();
    populate(slice, WorkerThreadId(0), 30, 0);
    const auto expected = sortedEntries(*dynamic_cast<SpillableAggregationSlice*>(slice.get()), WorkerThreadId(0));
    ASSERT_FALSE(expected.empty());

    store->forceSpill(sliceEnd);
    ASSERT_FALSE(store->isSliceResident(sliceEnd));

    /// The window [0,100) triggers at a watermark beyond its end; the store must unspill the slice before returning it.
    const auto triggered = store->getTriggerableWindowSlices(Timestamp(WINDOW_SIZE * 2));
    EXPECT_TRUE(triggeredContains(triggered, sliceEnd));
    EXPECT_TRUE(store->isSliceResident(sliceEnd));
    EXPECT_EQ(sortedEntries(*dynamic_cast<SpillableAggregationSlice*>(slice.get()), WorkerThreadId(0)), expected);
}

TEST_F(SpillableTimeBasedSliceStoreTest, residentBytesExactness)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    /// entriesPerPage == PAGE_SIZE/entrySize == 256/32 == 8. Each fill lands in a distinct tumbling window.
    const std::vector<std::pair<uint64_t, uint64_t>> fills = {{50, 0}, {150, 1}, {250, 8}, {350, 9}};
    for (const auto& [ts, count] : fills)
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        if (count > 0)
        {
            populate(slice, WorkerThreadId(0), count, ts);
        }
        const uint64_t pages = (count + entriesPerPage() - 1) / entriesPerPage();
        EXPECT_EQ(store->residentBytesOf(*slice), pages * PAGE_SIZE) << "count=" << count;
    }
}

TEST_F(SpillableTimeBasedSliceStoreTest, emittedSliceIsNotSpillable)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceEnd = slice->getSliceEnd();
    populate(slice, WorkerThreadId(0), 5, 0);

    /// Triggering emits the slice's raw HashMap* to the async probe → it must never be spilled afterwards.
    const auto triggered = store->getTriggerableWindowSlices(Timestamp(WINDOW_SIZE * 2));
    ASSERT_TRUE(triggeredContains(triggered, sliceEnd));

    ASSERT_EXCEPTION_ERRORCODE(store->forceSpill(sliceEnd), ErrorCode::SpillStoreFailure);
}

TEST_F(SpillableTimeBasedSliceStoreTest, doubleForceSpillIsIdempotent)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceEnd = slice->getSliceEnd();
    populate(slice, WorkerThreadId(0), 12, 0);

    store->forceSpill(sliceEnd);
    EXPECT_NO_THROW(store->forceSpill(sliceEnd)); /// already spilled → no-op
    EXPECT_FALSE(store->isSliceResident(sliceEnd));
    EXPECT_EQ(backend->sliceCount(), 1U);
}

TEST_F(SpillableTimeBasedSliceStoreTest, forceSpillUnknownSliceIsNoOp)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    EXPECT_NO_THROW(store->forceSpill(Timestamp(999)));
    EXPECT_EQ(backend->sliceCount(), 0U);
}

/// PR-1 (Increment B): store-wide residentBytes() = Σ over self-tracked createdSlices of residentBytesOf
/// (0 for spilled). Spilling one tracked slice drops the sum by exactly that slice's prior footprint.
TEST_F(SpillableTimeBasedSliceStoreTest, residentBytesIsStoreWideSum)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);

    /// Three tumbling slices in distinct windows, distinct tuple counts → distinct footprints.
    const auto sliceA = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceB = store->getSlicesOrCreate(Timestamp(150), spillableSliceFactory())[0];
    const auto sliceC = store->getSlicesOrCreate(Timestamp(250), spillableSliceFactory())[0];
    populate(sliceA, WorkerThreadId(0), 3, 0);
    populate(sliceB, WorkerThreadId(0), 7, 100);
    populate(sliceC, WorkerThreadId(0), 11, 200);

    const uint64_t bytesA = store->residentBytesOf(*sliceA);
    const uint64_t bytesB = store->residentBytesOf(*sliceB);
    const uint64_t bytesC = store->residentBytesOf(*sliceC);
    EXPECT_GT(bytesA, 0U);
    EXPECT_GT(bytesB, 0U);
    EXPECT_GT(bytesC, 0U);

    EXPECT_EQ(store->residentBytes(), bytesA + bytesB + bytesC);

    /// Spilling the middle slice frees its maps → its footprint becomes 0, the store-wide sum drops by exactly bytesB.
    store->forceSpill(sliceB->getSliceEnd());
    EXPECT_FALSE(store->isSliceResident(sliceB->getSliceEnd()));
    EXPECT_EQ(store->residentBytes(), bytesA + bytesC);
}

/// PR-1 (Increment B): garbageCollectSlicesAndWindows prunes createdSlices entries whose weak_ptr expired
/// because the base GC dropped the slice's last shared_ptr.
TEST_F(SpillableTimeBasedSliceStoreTest, gcPrunesCreatedSlices)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);

    /// Create three slices but DO NOT retain the returned shared_ptrs beyond this scope, so once the base GC
    /// erases them from its `slices` map the only remaining handle is createdSlices' weak_ptr.
    for (const uint64_t ts : {50ULL, 150ULL, 250ULL})
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        populate(slice, WorkerThreadId(0), 4, ts);
    }
    EXPECT_EQ(store->createdSlicesSizeForTest(), 3U);
    EXPECT_GT(store->residentBytes(), 0U);

    /// Trigger the windows so they are EMITTED_TO_PROBE, then GC past every (sliceEnd + windowSize). The base
    /// drops the slices; the override must then prune the expired weak_ptr entries from createdSlices.
    store->getTriggerableWindowSlices(Timestamp(WINDOW_SIZE * 5));
    store->garbageCollectSlicesAndWindows(Timestamp(WINDOW_SIZE * 10));

    EXPECT_EQ(store->createdSlicesSizeForTest(), 0U);
    EXPECT_EQ(store->residentBytes(), 0U);
}

}
