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
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
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
#include <SliceStore/SpillBackend.hpp>
#include <SliceStore/SpillableTimeBasedSliceStore.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES::Testing
{

using Entry = std::pair<uint64_t, std::vector<std::byte>>; /// (hash, key+value payload bytes)

/// Throw-injection backend (H1/H2 regression): every put() throws while `failPut` is set, simulating an I/O failure
/// (RocksDB error, bad_alloc) mid-spill. Clearing `failPut` lets a subsequent spill of the same slice succeed, which is
/// how the test proves the reservation was RELEASED (not leaked) after the throw — a leaked reservation would make the
/// retry a no-op ("already has an in-flight spill").
class ThrowingSpillBackend final : public SpillBackend
{
public:
    std::atomic<bool> failPut{true};
    std::atomic<std::size_t> putCount{0};

    void put(const SliceEnd /*sliceEnd*/, const WorkerThreadId /*workerThreadId*/, std::span<const std::byte> /*record*/) override
    {
        ++putCount;
        if (failPut.load())
        {
            throw std::runtime_error("injected spill I/O failure");
        }
    }

    std::optional<SpillRecord> get(const SliceEnd /*sliceEnd*/, const WorkerThreadId /*workerThreadId*/) override
    {
        return std::nullopt;
    }

    void erase(const SliceEnd /*sliceEnd*/) override { }
};

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
        /// Default thresholds are large enough that automatic eviction never fires, so the existing single-spill
        /// tests are unaffected.
        return makeStoreWithThresholds(1024ULL * 1024ULL, 2ULL * 1024ULL * 1024ULL, backendOut);
    }

    /// PR-2 (Increment B): construct a store with caller-chosen soft/hard thresholds so the synchronous hard-eviction
    /// loop is deterministically drivable from the unit test (the eviction tests need tiny thresholds).
    std::unique_ptr<SpillableTimeBasedSliceStore>
    makeStoreWithThresholds(const uint64_t softThresholdBytes, const uint64_t hardThresholdBytes, InMemorySpillBackend** backendOut) const
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
            softThresholdBytes,
            hardThresholdBytes);
        store->setBufferProvider(bufferManager);
        return store;
    }

    /// H1/H2 regression: construct a store with a caller-supplied backend (e.g. ThrowingSpillBackend) and tiny
    /// thresholds, so the spill exception paths are drivable. Mirrors makeStoreWithThresholds but does not assume the
    /// InMemorySpillBackend type.
    std::unique_ptr<SpillableTimeBasedSliceStore> makeStoreWithBackend(
        std::unique_ptr<SpillBackend> backend, const uint64_t softThresholdBytes, const uint64_t hardThresholdBytes) const
    {
        auto store = std::make_unique<SpillableTimeBasedSliceStore>(
            WINDOW_SIZE,
            WINDOW_SLIDE,
            SliceCacheConfiguration{},
            std::move(backend),
            entriesPerPage(),
            PAGE_SIZE,
            softThresholdBytes,
            hardThresholdBytes);
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

/// PR-2 (Increment B): the synchronous hard-eviction loop spills COLD slices oldest-first until resident memory is
/// back under the hard threshold, stops as soon as it is, writes exactly the spilled slices to the backend, and
/// leaves the still-resident slices untouched.
TEST_F(SpillableTimeBasedSliceStoreTest, hardEvictsOldestColdFirst)
{
    InMemorySpillBackend* backend = nullptr;
    /// One populated slice (3 tuples → one page == PAGE_SIZE) is exactly one threshold "unit". Hard = 2 pages: once
    /// three populated slices exist (3 pages) we are over hard and must spill back down to <= 2 pages (i.e. spill 1).
    auto store = makeStoreWithThresholds(/*soft*/ PAGE_SIZE, /*hard*/ 2ULL * PAGE_SIZE, &backend);

    const auto sliceA = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceB = store->getSlicesOrCreate(Timestamp(150), spillableSliceFactory())[0];
    const auto sliceC = store->getSlicesOrCreate(Timestamp(250), spillableSliceFactory())[0];
    populate(sliceA, WorkerThreadId(0), 3, 0);
    populate(sliceB, WorkerThreadId(0), 3, 100);
    populate(sliceC, WorkerThreadId(0), 3, 200);
    const uint64_t perSlice = store->residentBytesOf(*sliceA);
    ASSERT_EQ(perSlice, PAGE_SIZE);
    ASSERT_EQ(store->residentBytes(), 3U * PAGE_SIZE);

    /// Make all three slices COLD (sliceEnd < watermark) WITHOUT emitting them (the base feeders couple
    /// watermark-advance to emit; seeding the watermark is the only way to a cold-but-unemitted slice).
    store->setLastSeenWatermarkForTest(Timestamp(WINDOW_SIZE * 10));
    store->spillColdSlicesUnderHardForTest();

    /// Exactly one slice was spilled (the oldest, sliceA) — the loop stops once resident <= hard.
    EXPECT_EQ(store->residentBytes(), 2ULL * PAGE_SIZE);
    EXPECT_EQ(store->spillCountForTest(), 1U);
    EXPECT_FALSE(store->isSliceResident(sliceA->getSliceEnd())); /// oldest spilled FIRST
    EXPECT_TRUE(store->isSliceResident(sliceB->getSliceEnd()));
    EXPECT_TRUE(store->isSliceResident(sliceC->getSliceEnd()));
    EXPECT_TRUE(backend->contains(sliceA->getSliceEnd(), WorkerThreadId(0)));
    EXPECT_FALSE(backend->contains(sliceB->getSliceEnd(), WorkerThreadId(0)));
    EXPECT_FALSE(backend->contains(sliceC->getSliceEnd(), WorkerThreadId(0)));
    EXPECT_EQ(backend->sliceCount(), 1U);
}

/// PR-2 (Increment B, O-B5 hard): over the hard budget but no COLD candidate (every slice is still filling — none is
/// past the watermark) → proceed over-budget, no throw, no spill, resident unchanged.
TEST_F(SpillableTimeBasedSliceStoreTest, hardNoColdCandidateProceeds)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithThresholds(/*soft*/ PAGE_SIZE, /*hard*/ 2ULL * PAGE_SIZE, &backend);

    const auto sliceA = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceB = store->getSlicesOrCreate(Timestamp(150), spillableSliceFactory())[0];
    const auto sliceC = store->getSlicesOrCreate(Timestamp(250), spillableSliceFactory())[0];
    populate(sliceA, WorkerThreadId(0), 3, 0);
    populate(sliceB, WorkerThreadId(0), 3, 100);
    populate(sliceC, WorkerThreadId(0), 3, 200);
    const uint64_t before = store->residentBytes();
    ASSERT_EQ(before, 3U * PAGE_SIZE);

    /// Watermark left at 0 → no slice is cold → nothing is a spill candidate even though we are over hard.
    EXPECT_NO_THROW(store->spillColdSlicesUnderHardForTest());

    EXPECT_EQ(store->spillCountForTest(), 0U);
    EXPECT_EQ(store->residentBytes(), before);
    EXPECT_EQ(backend->sliceCount(), 0U);
}

/// PR-2 (Increment B, O-B5 terminal): every slice is COLD and we are STILL over the hard budget after spilling them
/// all → proceed, no throw, resident minimized to 0.
TEST_F(SpillableTimeBasedSliceStoreTest, hardStillOverAfterAllColdSpilled)
{
    InMemorySpillBackend* backend = nullptr;
    /// Hard = 0 bytes: even after every cold slice is spilled (resident == 0) the loop's "resident > hard" check
    /// can only ever be satisfied while resident > 0; once all cold slices are gone it must break, not throw.
    auto store = makeStoreWithThresholds(/*soft*/ 0, /*hard*/ 0, &backend);

    const auto sliceA = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceB = store->getSlicesOrCreate(Timestamp(150), spillableSliceFactory())[0];
    populate(sliceA, WorkerThreadId(0), 3, 0);
    populate(sliceB, WorkerThreadId(0), 3, 100);
    ASSERT_GT(store->residentBytes(), 0U);

    store->setLastSeenWatermarkForTest(Timestamp(WINDOW_SIZE * 10));
    EXPECT_NO_THROW(store->spillColdSlicesUnderHardForTest());

    /// Both cold slices spilled; resident minimized; the loop terminated (no infinite spin, no throw).
    EXPECT_EQ(store->spillCountForTest(), 2U);
    EXPECT_EQ(store->residentBytes(), 0U);
    EXPECT_FALSE(store->isSliceResident(sliceA->getSliceEnd()));
    EXPECT_FALSE(store->isSliceResident(sliceB->getSliceEnd()));
}

/// PR-2 (Increment B): an EMITTED slice is never chosen by the eviction scan even when it is cold and over budget
/// (the !emittedKeys filter holds), so its raw HashMap* stays valid for the async probe.
TEST_F(SpillableTimeBasedSliceStoreTest, emittedSliceNeverSpilledUnderEviction)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithThresholds(/*soft*/ 0, /*hard*/ 0, &backend);

    const auto sliceEmitted = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto emittedEnd = sliceEmitted->getSliceEnd();
    populate(sliceEmitted, WorkerThreadId(0), 3, 0);
    const auto sliceCold = store->getSlicesOrCreate(Timestamp(150), spillableSliceFactory())[0];
    const auto coldEnd = sliceCold->getSliceEnd();
    populate(sliceCold, WorkerThreadId(0), 3, 100);

    /// Emit the first slice via a trigger feeder (marks it emitted). The window [0,100) triggers at watermark > 100.
    const auto triggered = store->getTriggerableWindowSlices(Timestamp(WINDOW_SIZE + 1));
    ASSERT_TRUE(triggeredContains(triggered, emittedEnd));

    /// Now both slices are cold (watermark past both); the eviction scan must skip the emitted one and spill only the
    /// non-emitted cold one.
    store->setLastSeenWatermarkForTest(Timestamp(WINDOW_SIZE * 10));
    store->spillColdSlicesUnderHardForTest();

    EXPECT_TRUE(store->isSliceResident(emittedEnd)); /// emitted slice never spilled
    EXPECT_FALSE(store->isSliceResident(coldEnd)); /// the non-emitted cold slice was spilled
    EXPECT_FALSE(backend->contains(emittedEnd, WorkerThreadId(0)));
    EXPECT_TRUE(backend->contains(coldEnd, WorkerThreadId(0)));
}

/// PR-2 (Increment B): forceSpill twice is idempotent through the spill counter — a real spill bumps the counter
/// exactly once; the already-spilled second call is a no-op (no second backend write, no second count).
TEST_F(SpillableTimeBasedSliceStoreTest, doubleForceSpillCountsOnce)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceEnd = slice->getSliceEnd();
    populate(slice, WorkerThreadId(0), 12, 0);

    store->forceSpill(sliceEnd);
    EXPECT_EQ(store->spillCountForTest(), 1U);
    EXPECT_NO_THROW(store->forceSpill(sliceEnd)); /// already spilled → no-op, no second count
    EXPECT_EQ(store->spillCountForTest(), 1U);
    EXPECT_EQ(backend->sliceCount(), 1U);
}

/// PR-1 review coverage gap: deleteState() clears createdSlices (direct coverage of the createdSlices.wlock()->clear()
/// line, which other tests only exercise implicitly through destruction).
TEST_F(SpillableTimeBasedSliceStoreTest, deleteStateClearsCreatedSlices)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    for (const uint64_t ts : {50ULL, 150ULL, 250ULL})
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        populate(slice, WorkerThreadId(0), 4, ts);
    }
    ASSERT_EQ(store->createdSlicesSizeForTest(), 3U);

    store->deleteState();

    EXPECT_EQ(store->createdSlicesSizeForTest(), 0U);
    EXPECT_EQ(store->residentBytes(), 0U);
}

/// PR-1 review coverage gap (and M1 evidence): two getSlicesOrCreate at the SAME timestamp record the slice only ONCE
/// (the base returns the existing slice on the second call; recordCreatedSlice must not double-track it).
TEST_F(SpillableTimeBasedSliceStoreTest, recordCreatedSliceIdempotent)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    const auto first = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto second = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    EXPECT_EQ(first->getSliceEnd(), second->getSliceEnd());
    EXPECT_EQ(store->createdSlicesSizeForTest(), 1U);
}

/// PR-1 review coverage gap: residentBytes() on a freshly constructed store (no slices) is 0.
TEST_F(SpillableTimeBasedSliceStoreTest, residentBytesEmptyStoreIsZero)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    EXPECT_EQ(store->residentBytes(), 0U);
}

/// PR-2 review H2: when spill() throws during forceSpill, the exception is re-thrown (throwing contract preserved) AND
/// the spill reservation is RELEASED — so a subsequent forceSpill of the SAME slice actually retries the spill instead
/// of no-op'ing as "already in-flight". A leaked reservation (the pre-fix bug) would make the retry a silent no-op.
TEST_F(SpillableTimeBasedSliceStoreTest, forceSpillReleasesReservationWhenSpillThrows)
{
    auto backendOwned = std::make_unique<ThrowingSpillBackend>();
    auto* const backend = backendOwned.get();
    auto store = makeStoreWithBackend(std::move(backendOwned), /*soft*/ PAGE_SIZE, /*hard*/ 2ULL * PAGE_SIZE);

    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    populate(slice, WorkerThreadId(0), 3, 0);
    const auto sliceEnd = slice->getSliceEnd();

    /// First forceSpill: put() throws → the exception propagates (throwing contract preserved).
    EXPECT_ANY_THROW(store->forceSpill(sliceEnd));
    EXPECT_EQ(store->spillCountForTest(), 0U); /// spillCount only bumps AFTER a successful spill()
    EXPECT_TRUE(store->isSliceResident(sliceEnd)); /// the spill never completed → still resident

    /// The reservation must have been released: a second forceSpill retries (NOT a no-op). With put() now succeeding the
    /// spill completes; if the reservation had leaked, this call would have returned early as "already in-flight".
    backend->failPut.store(false);
    EXPECT_NO_THROW(store->forceSpill(sliceEnd));
    EXPECT_EQ(store->spillCountForTest(), 1U);
    EXPECT_FALSE(store->isSliceResident(sliceEnd));
    EXPECT_EQ(backend->putCount.load(), 2U); /// once (threw) + once (succeeded)
}

/// PR-2 review H1: when spill() throws on the eviction path, O-B5 requires the eviction path to SWALLOW (never throw,
/// proceed over-budget) AND release the reservation. The synchronous hard loop must not propagate, spillCount stays 0,
/// the slice stays resident, and the reservation is released (proven by a follow-up forceSpill succeeding).
TEST_F(SpillableTimeBasedSliceStoreTest, evictionReleasesReservationAndSwallowsWhenSpillThrows)
{
    auto backendOwned = std::make_unique<ThrowingSpillBackend>();
    auto* const backend = backendOwned.get();
    /// hard=0 ⇒ any resident slice is over-budget, so the eviction loop will try (and fail) to spill it.
    auto store = makeStoreWithBackend(std::move(backendOwned), /*soft*/ 0, /*hard*/ 0);

    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    populate(slice, WorkerThreadId(0), 3, 0);
    const auto sliceEnd = slice->getSliceEnd();
    store->setLastSeenWatermarkForTest(Timestamp(WINDOW_SIZE * 10)); /// make it cold

    /// O-B5: the eviction path must NEVER throw — even though every put() fails, the loop swallows and proceeds.
    EXPECT_NO_THROW(store->spillColdSlicesUnderHardForTest());
    EXPECT_EQ(store->spillCountForTest(), 0U); /// nothing finalized
    EXPECT_TRUE(store->isSliceResident(sliceEnd)); /// spill failed → still resident
    EXPECT_GT(backend->putCount.load(), 0U); /// it really attempted the spill

    /// Reservation released despite the throw: a forceSpill (now succeeding) completes instead of no-op'ing.
    backend->failPut.store(false);
    EXPECT_NO_THROW(store->forceSpill(sliceEnd));
    EXPECT_EQ(store->spillCountForTest(), 1U);
    EXPECT_FALSE(store->isSliceResident(sliceEnd));
}

}
