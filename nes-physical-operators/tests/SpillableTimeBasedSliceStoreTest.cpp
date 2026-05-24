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
    /// L1 (PR-3): `failGet` injects a throw on the UNSPILL path (SpillableAggregationSlice::unspill calls backend.get()),
    /// mirroring `failPut` for the spill path. Lets a test drive the unspill-throws path that the unspill ReservationGuard
    /// must survive without leaking the spillInProgress reservation. Default false so existing tests' get() stays a no-op.
    std::atomic<bool> failGet{false};
    std::atomic<std::size_t> getCount{0};

    void put(
        const SliceEnd /*sliceEnd*/,
        const WorkerThreadId /*workerThreadId*/,
        std::span<const std::byte> /*record*/,
        const PartitionId /*partition*/ = 0) override
    {
        ++putCount;
        if (failPut.load())
        {
            throw std::runtime_error("injected spill I/O failure");
        }
    }

    std::optional<SpillRecord> get(const SliceEnd /*sliceEnd*/, const WorkerThreadId /*workerThreadId*/, const PartitionId /*partition*/ = 0) override
    {
        ++getCount;
        if (failGet.load())
        {
            throw std::runtime_error("injected unspill I/O failure");
        }
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
    /// Increment C: trailing `emitLag` (default 0) so the emit-decouple tests can request an event-time lag without
    /// touching existing callers (which keep the 3-arg form ⇒ emitLag 0 ⇒ pre-Increment-C behavior).
    std::unique_ptr<SpillableTimeBasedSliceStore> makeStoreWithThresholds(
        const uint64_t softThresholdBytes,
        const uint64_t hardThresholdBytes,
        InMemorySpillBackend** backendOut,
        const uint64_t emitLag = 0) const
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
            hardThresholdBytes,
            emitLag);
        store->setBufferProvider(bufferManager);
        return store;
    }

    /// 2d: construct a store with a caller-chosen partition count (and large thresholds so automatic eviction never
    /// fires) to drive the partitioned terminal-emit primitives directly.
    std::unique_ptr<SpillableTimeBasedSliceStore> makeStoreWithPartitions(const uint64_t numberOfPartitions, InMemorySpillBackend** backendOut)
        const
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
            1024ULL * 1024ULL,
            2ULL * 1024ULL * 1024ULL,
            /*emitLag=*/0,
            numberOfPartitions);
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

/// Phase 0b: getMetrics() counts + times spill and unspill, tracks peak resident, and a spill/unspill round-trip of the
/// same slice restores exactly the bytes it freed.
TEST_F(SpillableTimeBasedSliceStoreTest, metricsRecordSpillRestoreAndPeak)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceEnd = slice->getSliceEnd();
    populate(slice, WorkerThreadId(0), 30, 0);

    /// residentBytes() is the peak-sampling point; call it while the slice is resident.
    const auto residentNow = store->residentBytes();
    ASSERT_GT(residentNow, 0U);

    const auto m0 = store->getMetrics();
    EXPECT_EQ(m0.slicesSpilled, 0U);
    EXPECT_EQ(m0.slicesUnspilled, 0U);
    EXPECT_EQ(m0.bytesSpilled, 0U);
    EXPECT_EQ(m0.bytesRestored, 0U);
    EXPECT_EQ(m0.spillSampleCount, 0U);
    EXPECT_EQ(m0.restoreSampleCount, 0U);
    /// Empty-sample state: every percentile field is the percentileOfSorted empty-vector guard (0), not garbage.
    EXPECT_EQ(m0.spillLatencyP50Us, 0U);
    EXPECT_EQ(m0.spillLatencyP95Us, 0U);
    EXPECT_EQ(m0.spillLatencyP99Us, 0U);
    EXPECT_EQ(m0.spillLatencyMaxUs, 0U);
    EXPECT_EQ(m0.restoreLatencyP50Us, 0U);
    EXPECT_EQ(m0.restoreLatencyMaxUs, 0U);
    EXPECT_GE(m0.peakResidentBytes, residentNow); /// peak captured by the residentBytes() call above

    store->forceSpill(sliceEnd);
    ASSERT_FALSE(store->isSliceResident(sliceEnd));
    const auto m1 = store->getMetrics();
    EXPECT_EQ(m1.slicesSpilled, 1U);
    EXPECT_GT(m1.bytesSpilled, 0U);
    EXPECT_EQ(m1.spillSampleCount, 1U);
    EXPECT_EQ(m1.slicesUnspilled, 0U);
    /// Single sample ⇒ every spill percentile collapses to that one value (== max). Guards the nearest-rank clamp at n==1.
    EXPECT_EQ(m1.spillLatencyP50Us, m1.spillLatencyMaxUs);
    EXPECT_EQ(m1.spillLatencyP95Us, m1.spillLatencyMaxUs);
    EXPECT_EQ(m1.spillLatencyP99Us, m1.spillLatencyMaxUs);
    /// Peak is a monotone high-water mark: spilling (which lowers resident bytes) must NEVER lower the recorded peak.
    EXPECT_GE(m1.peakResidentBytes, m0.peakResidentBytes);

    /// The window [0,100) triggers beyond its end → the store unspills the slice before returning it.
    const auto triggered = store->getTriggerableWindowSlices(Timestamp(WINDOW_SIZE * 2));
    ASSERT_TRUE(triggeredContains(triggered, sliceEnd));
    const auto m2 = store->getMetrics();
    EXPECT_EQ(m2.slicesUnspilled, 1U);
    EXPECT_GT(m2.bytesRestored, 0U);
    EXPECT_EQ(m2.restoreSampleCount, 1U);
    /// A round-trip of the SAME slice re-materializes exactly the footprint it freed.
    EXPECT_EQ(m2.bytesRestored, m2.bytesSpilled);
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

/// PR-3 review L1: when unspill() throws on the random-access read path (getSliceBySliceEnd → SpillableAggregationSlice::
/// unspill → backend.get()), the spillInProgress reservation (footprint-reader fence) MUST still be released — the
/// ReservationGuard guarantees this on the throw path. A leaked reservation (the pre-fix bug) would make the slice
/// invisible to residentBytes() forever AND make any future access to this sliceEnd hang forever on spillerCv.wait. This
/// is the MORE reachable of the two L1 sites (the slice is NOT emitted). The test proves release by: (1) the unspill
/// throw propagates, (2) a SUBSEQUENT getSliceBySliceEnd of the SAME sliceEnd does not hang and, with get() now
/// succeeding, completes the unspill and restores residency. If the reservation had leaked, step (2)'s wait predicate
/// (!spillInProgress.contains(sliceEnd)) would never become true and the single-threaded test would deadlock.
TEST_F(SpillableTimeBasedSliceStoreTest, getSliceBySliceEndReleasesReservationWhenUnspillThrows)
{
    auto backendOwned = std::make_unique<ThrowingSpillBackend>();
    auto* const backend = backendOwned.get();
    /// put succeeds (so we can spill the slice first); thresholds large so no auto-eviction interferes.
    backend->failPut.store(false);
    auto store = makeStoreWithBackend(std::move(backendOwned), /*soft*/ 1024ULL * 1024ULL, /*hard*/ 2ULL * 1024ULL * 1024ULL);

    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    populate(slice, WorkerThreadId(0), 5, 0);
    const auto sliceEnd = slice->getSliceEnd();

    /// Spill it so the next read needs an unspill (slice becomes non-resident).
    store->forceSpill(sliceEnd);
    ASSERT_FALSE(store->isSliceResident(sliceEnd));

    /// Random-access read with get() throwing → unspill() throws → the exception propagates out of getSliceBySliceEnd.
    backend->failGet.store(true);
    EXPECT_ANY_THROW(store->getSliceBySliceEnd(sliceEnd));
    EXPECT_GT(backend->getCount.load(), 0U); /// it really attempted the unspill
    EXPECT_FALSE(store->isSliceResident(sliceEnd)); /// unspill did not complete → still non-resident

    /// Reservation released despite the throw: with get() now succeeding, a retry does NOT hang (no leaked spillInProgress
    /// entry to wait on) and completes the unspill, flipping the slice back to resident.
    backend->failGet.store(false);
    std::optional<std::shared_ptr<Slice>> retry;
    EXPECT_NO_THROW(retry = store->getSliceBySliceEnd(sliceEnd));
    ASSERT_TRUE(retry.has_value());
    EXPECT_TRUE(store->isSliceResident(sliceEnd));
}

/// PR-3 review L1: same defensive RAII fix on the trigger path (unspillAndMarkEmitted). If unspill() throws while
/// restoring an emitted slice on trigger, the reservation must still be released so it does not stay stale in
/// spillInProgress for the store's lifetime. The slice is already emitted here (so a re-trigger is unusual in the
/// tumbling-window model), which is why this is the less-reachable site — but the guard makes it correct regardless.
/// Proof of release: after the throwing trigger, a getSliceBySliceEnd of the same sliceEnd (with get() now succeeding)
/// does not hang and completes — which it could not do if the trigger had leaked the reservation.
TEST_F(SpillableTimeBasedSliceStoreTest, unspillAndMarkEmittedReleasesReservationWhenUnspillThrows)
{
    auto backendOwned = std::make_unique<ThrowingSpillBackend>();
    auto* const backend = backendOwned.get();
    backend->failPut.store(false); /// allow the initial spill
    auto store = makeStoreWithBackend(std::move(backendOwned), /*soft*/ 1024ULL * 1024ULL, /*hard*/ 2ULL * 1024ULL * 1024ULL);

    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    populate(slice, WorkerThreadId(0), 5, 0);
    const auto sliceEnd = slice->getSliceEnd();

    store->forceSpill(sliceEnd);
    ASSERT_FALSE(store->isSliceResident(sliceEnd));

    /// Trigger the window [0,100): unspillAndMarkEmitted runs the unspill, get() throws → exception propagates.
    backend->failGet.store(true);
    EXPECT_ANY_THROW(store->getTriggerableWindowSlices(Timestamp(WINDOW_SIZE * 2)));
    EXPECT_GT(backend->getCount.load(), 0U);

    /// Reservation released despite the throw: a subsequent access of the same sliceEnd does not hang and completes.
    backend->failGet.store(false);
    std::optional<std::shared_ptr<Slice>> retry;
    EXPECT_NO_THROW(retry = store->getSliceBySliceEnd(sliceEnd));
    ASSERT_TRUE(retry.has_value());
    EXPECT_TRUE(store->isSliceResident(sliceEnd));
}

/// ===========================================================================================================
/// Increment C — emit-decouple (lagged emit watermark) tests.
/// emitLag (L, event-time ms) shifts the EMIT cutoff to (globalWatermark - L) while quiescence/cold-pick keeps the
/// TRUE globalWatermark. Completed windows in [globalWatermark - L, globalWatermark) are RETAINED (cold but unemitted)
/// and become spillable — fixing the E0 finding that cold ≡ emitted left the spillable set empty. emitLag == 0 is
/// byte-identical to pre-Increment-C. (Base emits windows with windowEnd < watermark — DefaultTimeBasedSliceStore.cpp:119.)
/// ===========================================================================================================

/// C-T1: with emitLag == 0 the emit cutoff equals the base's (windowEnd < watermark). Identity at the boundary.
TEST_F(SpillableTimeBasedSliceStoreTest, emitLagZeroEmitsIdenticallyToBaseline)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithThresholds(1024ULL * 1024ULL, 2ULL * 1024ULL * 1024ULL, &backend, /*emitLag*/ 0);
    const auto sliceA = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];  /// window [0,100), end 100
    const auto sliceB = store->getSlicesOrCreate(Timestamp(150), spillableSliceFactory())[0]; /// window [100,200), end 200
    populate(sliceA, WorkerThreadId(0), 3, 0);
    populate(sliceB, WorkerThreadId(0), 3, 100);

    /// Boundary: at watermark == windowEnd the window does NOT emit (windowEnd >= watermark breaks).
    EXPECT_FALSE(triggeredContains(store->getTriggerableWindowSlices(Timestamp(100)), sliceA->getSliceEnd()));
    /// At watermark 150: window end 100 emits, window end 200 does not — exactly the base cutoff.
    const auto triggered = store->getTriggerableWindowSlices(Timestamp(150));
    EXPECT_TRUE(triggeredContains(triggered, sliceA->getSliceEnd()));
    EXPECT_FALSE(triggeredContains(triggered, sliceB->getSliceEnd()));
}

/// C-T2: emitLag > 0 DEFERS emission by the lag band. A window in [watermark - L, watermark) is retained, then emits
/// once the frontier advances L past its end.
TEST_F(SpillableTimeBasedSliceStoreTest, emitLagDefersEmissionByLagBand)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithThresholds(1024ULL * 1024ULL, 2ULL * 1024ULL * 1024ULL, &backend, /*emitLag*/ WINDOW_SIZE);
    const auto sliceA = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];  /// end 100
    const auto sliceB = store->getSlicesOrCreate(Timestamp(150), spillableSliceFactory())[0]; /// end 200
    const auto sliceC = store->getSlicesOrCreate(Timestamp(250), spillableSliceFactory())[0]; /// end 300
    populate(sliceA, WorkerThreadId(0), 3, 0);
    populate(sliceB, WorkerThreadId(0), 3, 100);
    populate(sliceC, WorkerThreadId(0), 3, 200);

    /// True watermark 250, L = 100 ⇒ lagged emit cutoff 150 ⇒ only window end 100 emits; ends 200 (in the retained
    /// band [150,250)) and 300 (still hot) do NOT.
    const auto first = store->getTriggerableWindowSlices(Timestamp(250));
    EXPECT_TRUE(triggeredContains(first, sliceA->getSliceEnd()));
    EXPECT_FALSE(triggeredContains(first, sliceB->getSliceEnd()));
    EXPECT_FALSE(triggeredContains(first, sliceC->getSliceEnd()));

    /// Advance the frontier to 350 ⇒ lagged cutoff 250 ⇒ the previously-retained window end 200 now emits (deferred).
    const auto second = store->getTriggerableWindowSlices(Timestamp(350));
    EXPECT_TRUE(triggeredContains(second, sliceB->getSliceEnd()));
}

/// C-T3 (headline correctness, OPEN #5): deferring emission changes only TIMING, not VALUES. The same input stream run
/// through an L=0 store and an L>0 store yields identical per-window aggregate contents (after draining both at EOS).
TEST_F(SpillableTimeBasedSliceStoreTest, emitLagDifferentialIdenticalResults)
{
    /// Fold every emitted/drained slice's sorted entries, keyed by sliceEnd. Large thresholds ⇒ no spilling interferes;
    /// this isolates the emit-timing behavior.
    auto run = [this](const uint64_t emitLag) -> std::map<SliceEnd, std::vector<Entry>>
    {
        InMemorySpillBackend* backend = nullptr;
        auto store = makeStoreWithThresholds(1024ULL * 1024ULL, 2ULL * 1024ULL * 1024ULL, &backend, emitLag);
        std::map<SliceEnd, std::vector<Entry>> emitted;
        auto collect = [&](const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& triggered)
        {
            for (const auto& [windowInfo, slices] : triggered)
            {
                for (const auto& slice : slices)
                {
                    emitted[slice->getSliceEnd()] = sortedEntries(*dynamic_cast<SpillableAggregationSlice*>(slice.get()), WorkerThreadId(0));
                }
            }
        };
        /// Identical input stream: four tumbling windows (ends 100,200,300,400), distinct deterministic payloads.
        for (const uint64_t ts : {50ULL, 150ULL, 250ULL, 350ULL})
        {
            const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
            populate(slice, WorkerThreadId(0), 3, ts);
        }
        /// Identical watermark progression.
        for (const uint64_t watermark : {150ULL, 250ULL, 350ULL, 450ULL})
        {
            collect(store->getTriggerableWindowSlices(Timestamp(watermark)));
        }
        /// EOS: flush whatever is still retained (L>0 holds back a tail; L=0 already emitted everything).
        store->incrementNumberOfInputPipelines();
        collect(store->getAllNonTriggeredSlices());
        return emitted;
    };

    const auto baseline = run(/*emitLag*/ 0);
    const auto lagged = run(/*emitLag*/ WINDOW_SIZE);
    EXPECT_EQ(baseline.size(), 4U); /// all four windows accounted for
    EXPECT_EQ(baseline, lagged);    /// identical contents — only the emission timing differed
}

/// C-T4 (LOCKED #8): the terminate path flushes the ENTIRE retained backlog regardless of emitLag — a deferred window
/// must never be lost. With a lag so large nothing emits via triggers, EOS must still drain every window.
TEST_F(SpillableTimeBasedSliceStoreTest, emitLagBacklogFullyFlushedOnTerminate)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithThresholds(1024ULL * 1024ULL, 2ULL * 1024ULL * 1024ULL, &backend, /*emitLag*/ 10000);
    std::vector<SliceEnd> created;
    for (const uint64_t ts : {50ULL, 150ULL, 250ULL})
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        populate(slice, WorkerThreadId(0), 3, ts);
        created.push_back(slice->getSliceEnd());
    }
    /// Spill the WHOLE retained backlog first, so the terminal flush must re-materialise every window from disk via the
    /// Phase-1 paced ensureWindowSlicesResident path — proving nothing is orphaned even when every window starts spilled.
    for (const auto end : created)
    {
        store->forceSpill(end);
        ASSERT_FALSE(store->isSliceResident(end));
    }

    /// Huge lag ⇒ lagged cutoff clamps to 0 ⇒ NOTHING emits via the trigger even at a high true watermark.
    EXPECT_TRUE(store->getTriggerableWindowSlices(Timestamp(5000)).empty());

    /// Terminate (Phase 1 L1): getAllNonTriggeredSlices is LAZY — it returns the band still spilled; the handler
    /// re-materialises one window at a time via ensureWindowSlicesResident. Mirror that here, then assert every window is
    /// flushed AND resident on the way out.
    store->incrementNumberOfInputPipelines();
    auto drained = store->getAllNonTriggeredSlices();
    std::vector<SliceEnd> drainedEnds;
    for (auto& [windowInfo, slices] : drained)
    {
        store->ensureWindowSlicesResident(slices); /// paced re-materialise: one window at a time
        for (const auto& slice : slices)
        {
            drainedEnds.push_back(slice->getSliceEnd());
            EXPECT_TRUE(store->isSliceResident(slice->getSliceEnd()));
        }
    }
    std::ranges::sort(drainedEnds);
    std::ranges::sort(created);
    EXPECT_EQ(drainedEnds, created); /// the whole backlog, nothing orphaned
}

/// ── Phase 1 (L1) — paced terminal flush. getAllNonTriggeredSlices is now LAZY (does NOT eagerly unspill the whole
/// band); the handler unspills ONE window at a time via the new ensureWindowSlicesResident hook just before emitting it,
/// bounding the terminal-flush unspill peak (Option 1). These prove the store-side mechanism; the end-to-end process-RSS
/// win is measured out-of-gate by the Phase-0c harness. ──────────────────────────────────────────────────────────────

/// P1-T1 (RED proof, existing API only): the terminal drain returns SPILLED slices STILL spilled — it no longer
/// re-materialises the whole retained band up front. Pre-Phase-1 this FAILS (getAllNonTriggeredSlices eagerly unspilled
/// every slice). All windows must still be present (nothing dropped by going lazy).
TEST_F(SpillableTimeBasedSliceStoreTest, terminalFlushReturnsSlicesStillSpilled)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    std::vector<SliceEnd> ends;
    for (const uint64_t ts : {50ULL, 150ULL, 250ULL})
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        populate(slice, WorkerThreadId(0), 5, ts);
        ends.push_back(slice->getSliceEnd());
    }
    for (const auto end : ends)
    {
        store->forceSpill(end);
        ASSERT_FALSE(store->isSliceResident(end));
    }

    store->incrementNumberOfInputPipelines();
    const auto drained = store->getAllNonTriggeredSlices();
    for (const auto end : ends)
    {
        EXPECT_FALSE(store->isSliceResident(end)); /// lazy: NOT unspilled by the terminal drain itself
        EXPECT_TRUE(triggeredContains(drained, end)); /// ...but every window is still returned
    }
}

/// P1-T2 (the peak-bounding mechanism): ensureWindowSlicesResident unspills ONLY the window it is handed; the rest of
/// the band stays on disk, so at most one window's footprint is re-materialised at a time.
TEST_F(SpillableTimeBasedSliceStoreTest, ensureWindowSlicesResidentUnspillsOnlyThatWindow)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    for (const uint64_t ts : {50ULL, 150ULL, 250ULL})
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        populate(slice, WorkerThreadId(0), 5, ts);
        store->forceSpill(slice->getSliceEnd());
    }

    store->incrementNumberOfInputPipelines();
    auto drained = store->getAllNonTriggeredSlices();
    ASSERT_EQ(drained.size(), 3U);
    std::vector<std::vector<std::shared_ptr<Slice>>> windows; /// ordered by windowEnd (map ordering)
    for (auto& [windowInfo, slices] : drained)
    {
        windows.push_back(slices);
    }
    store->ensureWindowSlicesResident(windows[0]);
    EXPECT_TRUE(store->isSliceResident(windows[0].front()->getSliceEnd()));
    EXPECT_FALSE(store->isSliceResident(windows[1].front()->getSliceEnd()));
    EXPECT_FALSE(store->isSliceResident(windows[2].front()->getSliceEnd()));
}

/// P1-T3 (correct + complete): the full paced drain (ensure every window) yields the SAME window set and per-window
/// contents as before spilling — pacing the unspill loses nothing and duplicates nothing.
TEST_F(SpillableTimeBasedSliceStoreTest, pacedTerminalFlushFullDrainCorrectAndComplete)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    std::map<SliceEnd, std::vector<Entry>> expected;
    std::vector<std::shared_ptr<Slice>> slices;
    for (const uint64_t ts : {50ULL, 150ULL, 250ULL})
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        populate(slice, WorkerThreadId(0), 5, ts);
        expected[slice->getSliceEnd()] = sortedEntries(*dynamic_cast<SpillableAggregationSlice*>(slice.get()), WorkerThreadId(0));
        slices.push_back(slice);
    }
    for (const auto& s : slices)
    {
        store->forceSpill(s->getSliceEnd());
        ASSERT_FALSE(store->isSliceResident(s->getSliceEnd()));
    }

    store->incrementNumberOfInputPipelines();
    auto drained = store->getAllNonTriggeredSlices();
    std::map<SliceEnd, std::vector<Entry>> got;
    for (auto& [windowInfo, windowSlices] : drained)
    {
        store->ensureWindowSlicesResident(windowSlices); /// paced: one window at a time, just before reading it
        for (const auto& s : windowSlices)
        {
            EXPECT_TRUE(store->isSliceResident(s->getSliceEnd()));
            got[s->getSliceEnd()] = sortedEntries(*dynamic_cast<SpillableAggregationSlice*>(s.get()), WorkerThreadId(0));
        }
    }
    EXPECT_EQ(got, expected); /// same windows, same contents
}

/// P1-T4 (invariant preserved): ensureWindowSlicesResident marks each window emitted, so the async-probe-drain guard
/// still holds — an emitted slice can never be re-spilled (forceSpill rejects it). This proves the mark-emitted step did
/// not get lost when it moved out of getAllNonTriggeredSlices into the per-window hook.
TEST_F(SpillableTimeBasedSliceStoreTest, ensureWindowSlicesResidentMarksEmittedAndPreventsRespill)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    const auto sliceEnd = slice->getSliceEnd();
    populate(slice, WorkerThreadId(0), 5, 0);
    store->forceSpill(sliceEnd);

    store->incrementNumberOfInputPipelines();
    auto drained = store->getAllNonTriggeredSlices();
    ASSERT_EQ(drained.size(), 1U);
    store->ensureWindowSlicesResident(drained.begin()->second);
    EXPECT_TRUE(store->isSliceResident(sliceEnd));
    /// Emitted now ⇒ the spiller / hard-eviction can never touch it again.
    ASSERT_EXCEPTION_ERRORCODE(store->forceSpill(sliceEnd), ErrorCode::SpillStoreFailure);
}

/// C-T5 (the E0 → fixed proof): emitLag > 0 makes the cold-AND-unemitted set NON-EMPTY (the retained band), so the
/// spiller finally engages on a real trigger drive (NOT the setLastSeenWatermarkForTest seeding hook). The emitted
/// window and the still-hot window are never spilled.
TEST_F(SpillableTimeBasedSliceStoreTest, emitLagMakesColdSetNonEmptyAndSpillerEngages)
{
    InMemorySpillBackend* backend = nullptr;
    /// Tiny thresholds so the synchronous hard-eviction has work to do once the band is cold-but-unemitted.
    auto store = makeStoreWithThresholds(/*soft*/ PAGE_SIZE, /*hard*/ 2ULL * PAGE_SIZE, &backend, /*emitLag*/ 3ULL * WINDOW_SIZE);
    std::vector<std::shared_ptr<Slice>> slices;
    for (const uint64_t ts : {50ULL, 150ULL, 250ULL, 350ULL, 450ULL}) /// ends 100,200,300,400,500
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        populate(slice, WorkerThreadId(0), 3, ts);
        slices.push_back(slice);
    }

    /// Drive the PRODUCTION trigger to true watermark 450 (L=300 ⇒ lagged cutoff 150). Only window end 100 emits;
    /// ends 200/300/400 are retained (cold by true wm 450, unemitted by lagged cutoff 150); end 500 is still hot.
    const auto triggered = store->getTriggerableWindowSlices(Timestamp(450));
    EXPECT_TRUE(triggeredContains(triggered, slices[0]->getSliceEnd()));   /// end 100 emitted
    EXPECT_FALSE(triggeredContains(triggered, slices[1]->getSliceEnd()));  /// end 200 retained

    store->spillColdSlicesUnderHardForTest();

    /// The spiller engaged on the retained band (≥1 real spill). The oldest band slice (end 200) is spilled; the
    /// emitted slice (end 100, async-probe-drain) and the hot slice (end 500, not cold) are never spilled.
    EXPECT_GE(store->spillCountForTest(), 1U);
    EXPECT_FALSE(store->isSliceResident(slices[1]->getSliceEnd())); /// oldest band slice spilled
    EXPECT_TRUE(store->isSliceResident(slices[0]->getSliceEnd()));  /// emitted slice never spilled
    EXPECT_TRUE(store->isSliceResident(slices[4]->getSliceEnd()));  /// hot slice never spilled
    EXPECT_TRUE(backend->contains(slices[1]->getSliceEnd(), WorkerThreadId(0)));
    EXPECT_FALSE(backend->contains(slices[0]->getSliceEnd(), WorkerThreadId(0)));
}

/// C-T6 (the E0 empty-set, regression for emitLag == 0): with no lag every cold window is ALSO emitted, so the
/// cold-AND-unemitted set is empty and nothing spills — the exact E0 finding, reproduced as a unit invariant.
TEST_F(SpillableTimeBasedSliceStoreTest, emitLagZeroColdSetEmptyNoSpill)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithThresholds(/*soft*/ PAGE_SIZE, /*hard*/ 2ULL * PAGE_SIZE, &backend, /*emitLag*/ 0);
    for (const uint64_t ts : {50ULL, 150ULL, 250ULL, 350ULL, 450ULL})
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        populate(slice, WorkerThreadId(0), 3, ts);
    }

    /// True watermark 450, L=0 ⇒ every window with end < 450 is emitted; cold ≡ emitted ⇒ no spill candidate.
    store->getTriggerableWindowSlices(Timestamp(450));
    store->spillColdSlicesUnderHardForTest();

    EXPECT_EQ(store->spillCountForTest(), 0U);
    EXPECT_EQ(backend->sliceCount(), 0U);
}

/// C-T7 (clamp, R1): a true watermark below emitLag must SATURATE the lagged emit watermark to 0 (Timestamp::operator-
/// is a raw unsigned subtract that would otherwise wrap and spuriously emit). Nothing emits, nothing terminates.
TEST_F(SpillableTimeBasedSliceStoreTest, emitLagUnderflowClampsToZeroEmit)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithThresholds(1024ULL * 1024ULL, 2ULL * 1024ULL * 1024ULL, &backend, /*emitLag*/ 10ULL * WINDOW_SIZE);
    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0]; /// end 100
    populate(slice, WorkerThreadId(0), 3, 0);

    /// True watermark 100 < emitLag 1000 ⇒ lagged clamps to 0 ⇒ empty trigger, no wrap, no spurious emission.
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> triggered;
    EXPECT_NO_THROW(triggered = store->getTriggerableWindowSlices(Timestamp(100)));
    EXPECT_TRUE(triggered.empty());
    EXPECT_FALSE(triggeredContains(triggered, slice->getSliceEnd()));
}

/// C-T8 (PR3, the late-write guard): a late/out-of-order record into a retained band window whose slice has been
/// SPILLED must NOT terminate. getSlicesOrCreate unspills it on the worker thread before the write AND pins it, so a
/// subsequent eviction cannot re-spill it while its raw HashMap* is cached. Contents survive the spill→unspill→append.
TEST_F(SpillableTimeBasedSliceStoreTest, lateWriteIntoSpilledRetainedSliceUnspillsPinsAndDoesNotTerminate)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithThresholds(/*soft*/ PAGE_SIZE, /*hard*/ 2ULL * PAGE_SIZE, &backend, /*emitLag*/ 3ULL * WINDOW_SIZE);
    std::vector<std::shared_ptr<Slice>> slices;
    for (const uint64_t ts : {50ULL, 150ULL, 250ULL, 350ULL}) /// ends 100,200,300,400
    {
        const auto slice = store->getSlicesOrCreate(Timestamp(ts), spillableSliceFactory())[0];
        populate(slice, WorkerThreadId(0), 3, ts);
        slices.push_back(slice);
    }
    const auto bandEnd = slices[0]->getSliceEnd(); /// window [0,100)
    const auto expectedBefore = sortedEntries(*dynamic_cast<SpillableAggregationSlice*>(slices[0].get()), WorkerThreadId(0));
    ASSERT_EQ(expectedBefore.size(), 3U);

    /// True watermark 400, L=300 ⇒ lagged cutoff 100 ⇒ window end 100 is cold (100 < 400) but NOT emitted (100 is not
    /// < 100): it sits in the retained band. Spilling drives it to disk (oldest cold-unemitted first).
    store->getTriggerableWindowSlices(Timestamp(400));
    store->spillColdSlicesUnderHardForTest();
    ASSERT_FALSE(store->isSliceResident(bandEnd)); /// the band slice is now spilled

    /// LATE WRITE: a record for event-time 50 (window [0,100)) arrives → cache miss → getSlicesOrCreate returns the
    /// existing SPILLED slice. Without the guard, the following getHashMapPtrOrCreate (inside populate) would trip
    /// INVARIANT(resident) → std::terminate. The guard must have unspilled it.
    const auto lateSlice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    EXPECT_EQ(lateSlice->getSliceEnd(), bandEnd);
    EXPECT_TRUE(store->isSliceResident(bandEnd)); /// guard unspilled it before the write
    EXPECT_NO_THROW(populate(lateSlice, WorkerThreadId(0), 2, 1000)); /// the late write itself must not terminate

    /// The PIN must prevent re-spilling while the slice is cached: force the synchronous hard-eviction again — the
    /// late-touched band slice stays resident (the spiller/eviction skip pinned slices), closing the re-spill race.
    store->spillColdSlicesUnderHardForTest();
    EXPECT_TRUE(store->isSliceResident(bandEnd)); /// pinned ⇒ never re-spilled

    /// Contents intact: the spill→unspill round-trip preserved the original 3 entries and the 2 late ones were appended.
    const auto after = sortedEntries(*dynamic_cast<SpillableAggregationSlice*>(lateSlice.get()), WorkerThreadId(0));
    EXPECT_EQ(after.size(), expectedBefore.size() + 2);

    /// Pin hand-off via the emit path: advance the frontier so the lagged cutoff passes bandEnd (100 < 500-300=200),
    /// emitting the late-touched slice. unspillAndMarkEmitted runs the pinnedKeys.erase and moves the slice into the
    /// emitted regime — after which forceSpill refuses it (async-probe-drain), confirming the late-write pin was
    /// correctly handed off to emittedKeys rather than stranding the slice. (The pin's removal is otherwise masked by
    /// emittedKeys, which dominates the same pickAndReserveColdSlice skip, so this asserts the emit transition itself.)
    const auto emitted = store->getTriggerableWindowSlices(Timestamp(500));
    EXPECT_TRUE(triggeredContains(emitted, bandEnd));
    EXPECT_TRUE(store->isSliceResident(bandEnd)); /// emit unspills+keeps it resident for the async probe
    ASSERT_EXCEPTION_ERRORCODE(store->forceSpill(bandEnd), ErrorCode::SpillStoreFailure);
}

/// --- 2d: partitioned terminal-emit primitives (claimWindowForPartitionedEmit + streamWindowPartition) ---

namespace
{
/// (hash, payload) of any ChainedHashMap (resident worker map or a freshly-materialised partition map), order-independent.
void collectAnyMap(const ChainedHashMap* map, std::vector<Entry>& out, const uint64_t payloadSize)
{
    if (map != nullptr && map->getNumberOfTuples() > 0)
    {
        for (uint64_t chain = 0; chain < map->getNumberOfChains(); ++chain)
        {
            for (const auto* entry = map->getStartOfChain(chain); entry != nullptr; entry = entry->next)
            {
                const auto* const payload = reinterpret_cast<const std::byte*>(entry) + sizeof(ChainedHashMapEntry);
                out.emplace_back(entry->hash, std::vector<std::byte>(payload, payload + payloadSize));
            }
        }
    }
}
}

/// Drive the 2d partitioned terminal-emit primitives over ONE window that mixes a SPILLED and a RESIDENT slice. The
/// PRIMARY correctness proof is: the union of the per-partition streamed maps over ALL P partitions equals the full
/// window aggregate exactly (key-disjoint + complete — every original (hash,payload) appears once, none added/dropped).
/// Secondary: the resident slice stays resident and the spilled slice stays spilled across streaming, the streamed
/// entry COUNT equals the full window (no double-count / no loss), and — after the claim marks both slices emitted —
/// they cannot be re-spilled (the live spiller is fenced off). (The skip-empty + contiguous-renumber emit semantics are
/// proven deterministically in AggregationOperatorHandlerRegistryTest's Group D, which forces empties via pigeonhole;
/// here P=8 over many keys may or may not leave a residue empty, so this test does NOT assert on the empty count.)
TEST_F(SpillableTimeBasedSliceStoreTest, partitionedEmitUnionEqualsWindowMixedResidentAndSpilled)
{
    constexpr uint64_t P = 8;
    const uint64_t payloadSize = entrySize() - sizeof(ChainedHashMapEntry);

    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithPartitions(P, &backend);

    /// Two distinct slices (each its own tumbling window/slice); we then drive the store primitives over a manually-built
    /// vector that contains BOTH, exercising the resident and spilled branches of streamWindowPartition in one call.
    const auto residentSlice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    populate(residentSlice, WorkerThreadId(0), 40, 0);
    populate(residentSlice, WorkerThreadId(1), 25, 1000);

    const auto spilledSlice = store->getSlicesOrCreate(Timestamp(150), spillableSliceFactory())[0];
    populate(spilledSlice, WorkerThreadId(0), 33, 5000);
    populate(spilledSlice, WorkerThreadId(2), 12, 9000);

    /// Capture the full expected entry set across BOTH slices' workers before spilling.
    std::vector<Entry> expected;
    for (const auto& slice : {residentSlice, spilledSlice})
    {
        const auto* const sp = dynamic_cast<const SpillableAggregationSlice*>(slice.get());
        for (uint64_t w = 0; w < NUMBER_OF_HASH_MAPS; ++w)
        {
            collectAnyMap(dynamic_cast<const ChainedHashMap*>(sp->getHashMapPtr(WorkerThreadId(w))), expected, payloadSize);
        }
    }
    std::ranges::sort(expected, [](const Entry& a, const Entry& b) { return a.first != b.first ? a.first < b.first : a.second < b.second; });
    ASSERT_FALSE(expected.empty());

    /// Spill ONE of the two slices P-way so the window mixes a spilled + a resident slice.
    store->forceSpill(spilledSlice->getSliceEnd());
    ASSERT_FALSE(store->isSliceResident(spilledSlice->getSliceEnd()));
    ASSERT_TRUE(store->isSliceResident(residentSlice->getSliceEnd()));

    const std::vector<std::shared_ptr<Slice>> window{residentSlice, spilledSlice};

    /// CLAIM: marks both slices emitted (fences the live spiller). Residency is unchanged by the claim.
    store->claimWindowForPartitionedEmit(window);
    EXPECT_TRUE(store->isSliceResident(residentSlice->getSliceEnd()));
    EXPECT_FALSE(store->isSliceResident(spilledSlice->getSliceEnd()));

    /// STREAM each partition and union the entries. Streaming a partition must NEVER change either slice's residency.
    std::vector<Entry> unioned;
    for (uint64_t p = 0; p < P; ++p)
    {
        auto maps = store->streamWindowPartition(window, p);
        for (const auto& map : maps)
        {
            collectAnyMap(dynamic_cast<const ChainedHashMap*>(map.get()), unioned, payloadSize);
        }
        /// Streaming is a 1/P read-out, not an unspill: residency is invariant across every partition.
        EXPECT_TRUE(store->isSliceResident(residentSlice->getSliceEnd()));
        EXPECT_FALSE(store->isSliceResident(spilledSlice->getSliceEnd()));
    }

    /// PRIMARY correctness proof (key-disjoint + complete): the multiset of streamed (hash,payload) entries over all P
    /// partitions equals the full window aggregate EXACTLY — nothing added, dropped, or double-counted by partitioning.
    /// The COUNT check fires first so a loss/duplication is reported as a count mismatch even before the content compare.
    ASSERT_EQ(unioned.size(), expected.size())
        << "partitioned stream produced " << unioned.size() << " entries but the window holds " << expected.size();
    std::ranges::sort(unioned, [](const Entry& a, const Entry& b) { return a.first != b.first ? a.first < b.first : a.second < b.second; });
    ASSERT_EQ(unioned, expected) << "union of the P streamed partitions != the full window aggregate (key-disjoint+complete violated)";

    /// The claim marked both slices emitted ⇒ forceSpill must refuse them (async-probe-drain invariant), proving the
    /// spiller is fenced off the claimed window.
    ASSERT_EXCEPTION_ERRORCODE(store->forceSpill(residentSlice->getSliceEnd()), ErrorCode::SpillStoreFailure);
}

/// H1 regression guard (no data loss across a store spill→unspill round-trip at P>1). The store's spill sites now write
/// P blobs AND the store's unspill sites read all P blobs; if the unspill site were left at P=1 it would read only
/// partition 0 and silently lose partitions 1..P-1. With P=8 and keys spanning multiple partitions, force-spill a slice
/// (writes P-way), then re-materialise it through a STORE unspill path (ensureWindowSlicesResident, the terminal-flush
/// per-window primitive) and assert EVERY original key is restored — without the unspill-site fix this fails (only ~1/P
/// keys present). Spanning multiple partitions is guaranteed: hash=(i+1)*2654435761 and 2654435761%8==1 ⇒ the keys
/// cover residues 0..7, so several partition blobs exist and the P-consistent unspill must merge them all.
TEST_F(SpillableTimeBasedSliceStoreTest, storeSpillUnspillRoundTripPreservesAllKeysAtPGreaterThanOne)
{
    constexpr uint64_t P = 8;
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithPartitions(P, &backend);

    const auto slice = store->getSlicesOrCreate(Timestamp(50), spillableSliceFactory())[0];
    /// keys land across all 8 residues, so the spill writes multiple non-empty partition blobs per worker.
    populate(slice, WorkerThreadId(0), 40, 0);
    populate(slice, WorkerThreadId(1), 23, 1000);
    auto* const spillable = dynamic_cast<SpillableAggregationSlice*>(slice.get());
    const auto expected0 = sortedEntries(*spillable, WorkerThreadId(0));
    const auto expected1 = sortedEntries(*spillable, WorkerThreadId(1));
    ASSERT_EQ(expected0.size(), 40U);
    ASSERT_EQ(expected1.size(), 23U);

    /// Spill P-way (the store now passes numberOfPartitions): each worker map is split into P blobs.
    store->forceSpill(slice->getSliceEnd());
    ASSERT_FALSE(store->isSliceResident(slice->getSliceEnd()));
    /// Sanity: worker 0 (40 keys) wrote MORE THAN ONE partition blob — confirming the spill actually partitioned the
    /// worker map rather than writing a single P=1 blob. (If only partition 0 existed, the P fix would be a no-op here.)
    uint64_t worker0Blobs = 0;
    for (PartitionId p = 0; p < P; ++p)
    {
        if (backend->contains(slice->getSliceEnd(), WorkerThreadId(0), p))
        {
            ++worker0Blobs;
        }
    }
    EXPECT_GT(worker0Blobs, 1U);

    /// Re-materialise via a STORE unspill path. ensureWindowSlicesResident claims + unspills with the store's own P.
    store->ensureWindowSlicesResident({slice});
    ASSERT_TRUE(store->isSliceResident(slice->getSliceEnd()));

    /// NO DATA LOSS: every original key/value of every worker is back, exactly once. A P=1 unspill of P-way blobs would
    /// restore only partition 0 (~1/P of the keys) and these EXPECT_EQ would fail — that is the regression this guards.
    EXPECT_EQ(sortedEntries(*spillable, WorkerThreadId(0)), expected0);
    EXPECT_EQ(sortedEntries(*spillable, WorkerThreadId(1)), expected1);
    EXPECT_EQ(spillable->getNumberOfTuples(), expected0.size() + expected1.size());
}

/// P==1 store: getNumberOfPartitions() reports 1 (the default), confirming the byte-identical fast-path is selected.
TEST_F(SpillableTimeBasedSliceStoreTest, defaultStoreReportsSinglePartition)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStore(&backend);
    EXPECT_EQ(store->getNumberOfPartitions(), 1U);
}

TEST_F(SpillableTimeBasedSliceStoreTest, configuredStoreReportsPartitionCount)
{
    InMemorySpillBackend* backend = nullptr;
    auto store = makeStoreWithPartitions(8, &backend);
    EXPECT_EQ(store->getNumberOfPartitions(), 8U);
}

}
