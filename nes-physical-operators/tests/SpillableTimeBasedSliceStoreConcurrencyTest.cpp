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

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <thread>
#include <utility>
#include <vector>

#include <Aggregation/SpillableAggregationSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/BufferManager.hpp>
#include <SliceStore/InMemorySpillBackend.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>
#include <SliceStore/SpillableTimeBasedSliceStore.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <Engine.hpp>
#include <ErrorHandling.hpp>
#include <HashMapSlice.hpp>
#include <SliceCacheConfiguration.hpp>

namespace NES::Testing
{

/// Mutex-guarded delegate around InMemorySpillBackend. Increment B's uniform no-I/O-under-evictionMutex rule means
/// spill()/unspill() of DIFFERENT SliceEnds run concurrently (background spiller, a worker on the hard path, the
/// trigger thread). InMemorySpillBackend keeps all slices in one std::map and is documented "Not thread-safe", so
/// concurrent put/get/erase on different keys would race the shared tree — a race in the *test backend*, not in the
/// slice store under test. Wrapping it in a single mutex removes that confound so the stress test exercises ONLY the
/// store's concurrency (the spillInProgress reservation + spillerCv WAIT path), which is the actual subject.
class ThreadSafeInMemorySpillBackend final : public SpillBackend
{
public:
    void put(const SliceEnd sliceEnd, const WorkerThreadId workerThreadId, std::span<const std::byte> record) override
    {
        std::lock_guard lock(mutex);
        delegate.put(sliceEnd, workerThreadId, record);
    }

    std::optional<SpillRecord> get(const SliceEnd sliceEnd, const WorkerThreadId workerThreadId) override
    {
        std::lock_guard lock(mutex);
        return delegate.get(sliceEnd, workerThreadId);
    }

    void erase(const SliceEnd sliceEnd) override
    {
        std::lock_guard lock(mutex);
        delegate.erase(sliceEnd);
    }

private:
    std::mutex mutex;
    InMemorySpillBackend delegate;
};

/// PR-3 (Increment B) concurrency stress test (the ESCALATE gate). N builder threads + 1 trigger thread + the
/// store-owned background spiller hammer the store under continuous eviction (tiny soft threshold). It is a no-TSan
/// probabilistic UAF/race detector:
///  - every slice returned by a trigger feeder is asserted RESIDENT (the spill-vs-emit / spill-vs-trigger exposure
///    point) and its per-worker raw HashMap* is folded into a checksum (proving the map is live, not freed under us);
///  - the per-(worker, key) payload is deterministic, so Σ(triggered + drained checksums) MUST equal Σ(inserted
///    contributions) — any tuple lost to a half-freed/UAF spill, or any double-count, breaks the equality;
///  - the trigger advances its watermark to overlap the spiller's cold-pick range, FORCING the same-SliceEnd
///    spill-vs-trigger collision so the spillInProgress / spillerCv WAIT path is actually exercised
///    (binding consequence of the uniform no-I/O-under-lock decision);
///  - completing the whole scenario (iterated >= numIterations) without crash or deadlock is the primary signal.
class SpillableTimeBasedSliceStoreConcurrencyTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("SpillableTimeBasedSliceStoreConcurrencyTest.log", LogLevel::LOG_ERROR);
        NES_INFO("Setup SpillableTimeBasedSliceStoreConcurrencyTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create();
    }

    void TearDown() override
    {
        NES_INFO("Tear down SpillableTimeBasedSliceStoreConcurrencyTest.");
        BaseUnitTest::TearDown();
    }

protected:
    static constexpr uint64_t KEY_SIZE = 8;
    static constexpr uint64_t VALUE_SIZE = 8;
    static constexpr uint64_t NUMBER_OF_BUCKETS = 16;
    static constexpr uint64_t PAGE_SIZE = 256;
    /// One ChainedHashMap per worker thread → no two builders ever write the same map (a data race independent of the
    /// store). MUST be >= NUM_BUILDERS so workerThreadId % numberOfHashMaps is the identity (distinct map per worker).
    static constexpr uint64_t NUMBER_OF_HASH_MAPS = 8;
    /// Tumbling window: one slice == one window. windowEnd is a multiple of WINDOW_SIZE.
    static constexpr uint64_t WINDOW_SIZE = 100;
    static constexpr uint64_t WINDOW_SLIDE = 100;

    static constexpr std::size_t NUM_BUILDERS = 8;
    /// Distinct tumbling windows produced per iteration. Each builder fills its own map in every window.
    static constexpr uint64_t NUM_WINDOWS = 64;
    /// Tuples per (builder, window). Kept small so each map is one page → fast spill/unspill, many spill cycles.
    static constexpr uint64_t TUPLES_PER_CELL = 4;

    std::shared_ptr<BufferManager> bufferManager;

    [[nodiscard]] static uint64_t entrySize() { return sizeof(ChainedHashMapEntry) + KEY_SIZE + VALUE_SIZE; }

    [[nodiscard]] static uint64_t entriesPerPage() { return PAGE_SIZE / entrySize(); }

    static CreateNewHashMapSliceArgs makeArgs()
    {
        auto noopCleanup = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(
            std::function<void(nautilus::val<HashMap*>)>([](nautilus::val<HashMap*>) { }));
        return CreateNewHashMapSliceArgs({std::move(noopCleanup)}, KEY_SIZE, VALUE_SIZE, PAGE_SIZE, NUMBER_OF_BUCKETS);
    }

    static std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)> spillableSliceFactory()
    {
        return [](const SliceStart sliceStart, const SliceEnd sliceEnd) -> std::vector<std::shared_ptr<Slice>>
        { return {std::make_shared<SpillableAggregationSlice>(sliceStart, sliceEnd, makeArgs(), NUMBER_OF_HASH_MAPS)}; };
    }

    /// Deterministic per-(worker, key) value so the global checksum is reproducible regardless of interleaving:
    /// value(worker, key) = (worker + 1) * 1000003 + key. The summed contribution over all inserts is the invariant
    /// that proves no tuple was lost to a half-freed/UAF spill and none was double-counted.
    [[nodiscard]] static uint64_t valueFor(const std::size_t worker, const uint64_t key)
    {
        return ((static_cast<uint64_t>(worker) + 1) * 1000003ULL) + key;
    }

    void populateCell(
        const std::shared_ptr<Slice>& slice, const WorkerThreadId workerThreadId, const std::size_t worker, const uint64_t windowEnd) const
    {
        auto* const spillable = dynamic_cast<SpillableAggregationSlice*>(slice.get());
        auto* const map = dynamic_cast<ChainedHashMap*>(spillable->getHashMapPtrOrCreate(workerThreadId));
        for (uint64_t i = 0; i < TUPLES_PER_CELL; ++i)
        {
            /// Key unique across (worker, window, i) so distinct windows never collide on a key within a worker's map.
            const uint64_t key = (windowEnd * 1000ULL) + (static_cast<uint64_t>(worker) * 100ULL) + i;
            const uint64_t value = valueFor(worker, key);
            const uint64_t hash = key * 2654435761ULL;
            auto* const entry = static_cast<ChainedHashMapEntry*>(map->insertEntry(hash, bufferManager.get()));
            auto* const payload = reinterpret_cast<std::byte*>(entry) + sizeof(ChainedHashMapEntry);
            std::memcpy(payload, &key, sizeof(key));
            std::memcpy(payload + sizeof(key), &value, sizeof(value));
        }
    }

    /// Fold every entry of one worker's resident map into the running checksum. Reads the raw HashMap* the store
    /// returned — if the store handed back a spilled (freed) map this would be a UAF; the residency assertion at the
    /// call site is the guard, and this fold is the "probe" that actually dereferences the pointer.
    static uint64_t checksumOfMap(const SpillableAggregationSlice& slice, const WorkerThreadId workerThreadId)
    {
        const auto* const map = dynamic_cast<const ChainedHashMap*>(slice.getHashMapPtr(workerThreadId));
        uint64_t sum = 0;
        if (map != nullptr && map->getNumberOfTuples() > 0)
        {
            for (uint64_t chain = 0; chain < map->getNumberOfChains(); ++chain)
            {
                for (const auto* entry = map->getStartOfChain(chain); entry != nullptr; entry = entry->next)
                {
                    const auto* const payload = reinterpret_cast<const std::byte*>(entry) + sizeof(ChainedHashMapEntry);
                    uint64_t value = 0;
                    std::memcpy(&value, payload + sizeof(uint64_t), sizeof(value));
                    sum += value;
                }
            }
        }
        return sum;
    }
};

TEST_F(SpillableTimeBasedSliceStoreConcurrencyTest, concurrentBuildTriggerSpillIsRaceFree)
{
    constexpr std::size_t numIterations = 50;

    for (std::size_t iteration = 0; iteration < numIterations; ++iteration)
    {
        auto store = std::make_unique<SpillableTimeBasedSliceStore>(
            WINDOW_SIZE,
            WINDOW_SLIDE,
            SliceCacheConfiguration{},
            std::make_unique<ThreadSafeInMemorySpillBackend>(),
            entriesPerPage(),
            PAGE_SIZE,
            /// Threshold sizing makes "eviction actually happened" DETERMINISTIC under CPU saturation, decoupling the
            /// test's pass/fail from background-thread scheduling. Footprint math (verified): entriesPerPage == 6, and
            /// TUPLES_PER_CELL == 4 < 6, so every populated per-worker map rounds up to exactly ONE page (PAGE_SIZE
            /// bytes); residentBytesOf sums those pages over the (up to NUM_BUILDERS) worker maps of each resident,
            /// not-yet-spilled slice.
            ///   - TINY soft (one page) keeps the store-owned BACKGROUND spiller enabled and evicting continuously, so
            ///     its concurrency safety (spill() of cold slices racing the trigger) is still fully exercised — the Σ
            ///     checksum equality below covers its correctness.
            ///   - LOW hard (two pages) guarantees the SYNCHRONOUS hard path fires on the BUILDER threads, which are
            ///     never starved (they are doing the CPU work). As soon as >2 pages (i.e. >=3 populated worker maps,
            ///     768 > 512 bytes) are resident across tracked slices — which happens almost immediately given
            ///     NUM_BUILDERS=8 builders each filling a cell per window over NUM_WINDOWS=64 windows — the next
            ///     genuinely-new slice creation in getSlicesOrCreate runs evictUntilUnderHard inline. evictUntilUnderHard
            ///     spills only COLD slices (spillOneColdSlice), i.e. exactly the cold-but-unemitted windows the trigger's
            ///     spill horizon produces, so it still drives the SAME-SliceEnd spill-vs-trigger collision (now via a
            ///     non-starvable path) and guarantees spillCountForTest() > 0 regardless of how the background spiller is
            ///     scheduled under -j8 load.
            /*soft*/ PAGE_SIZE,
            /*hard*/ 2ULL * PAGE_SIZE);
        /// Starts the store-owned background spiller (PR-3 lifecycle).
        store->setBufferProvider(bufferManager);
        /// One logical build pipeline: getAllNonTriggeredSlices (the terminate/drain path) requires
        /// numberOfActiveInputPipelines > 0 and only emits every remaining window once it decrements back to 0 — so a
        /// single increment here lets the post-latch drain flush all still-filling windows in one call.
        store->incrementNumberOfInputPipelines();

        /// Expected total = Σ over every (worker, window, i) inserted contribution. Computed up front (deterministic).
        uint64_t expectedChecksum = 0;
        for (std::size_t worker = 0; worker < NUM_BUILDERS; ++worker)
        {
            for (uint64_t w = 0; w < NUM_WINDOWS; ++w)
            {
                const uint64_t windowEnd = (w + 1) * WINDOW_SIZE;
                for (uint64_t i = 0; i < TUPLES_PER_CELL; ++i)
                {
                    const uint64_t key = (windowEnd * 1000ULL) + (static_cast<uint64_t>(worker) * 100ULL) + i;
                    expectedChecksum += valueFor(worker, key);
                }
            }
        }

        /// Tracks which windowEnds have been triggered (and thus already folded into the checksum), so the post-latch
        /// drain does not double-count a window the trigger thread already harvested. Guarded by triggeredMutex.
        std::mutex triggeredMutex;
        std::vector<bool> windowTriggered(NUM_WINDOWS + 1, false);
        std::atomic<uint64_t> observedChecksum{0};

        /// Start barrier: all builders + the trigger thread released together to maximise interleaving.
        std::latch startLatch(NUM_BUILDERS + 1 + 1);
        /// Per-window completion counter: each builder bumps doneCount[w] when it has FINISHED populating window w. The
        /// trigger's safe watermark is the highest window for which ALL builders are done — so no builder ever writes a
        /// window the trigger has triggered (honours the no-late-data / watermark-monotonicity contract, INV-3). The
        /// spill-vs-trigger SAME-SliceEnd collision still happens: a fully-built window goes cold the instant the
        /// trigger advances past it, and the background spiller (continuous, soft=1 page) races the trigger for it.
        std::vector<std::atomic<std::size_t>> doneCount(NUM_WINDOWS);
        for (auto& counter : doneCount)
        {
            counter.store(0);
        }

        const auto foldTriggered = [&](const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& triggered)
        {
            uint64_t local = 0;
            for (const auto& [windowInfo, slices] : triggered)
            {
                for (const auto& slice : slices)
                {
                    auto* const spillable = dynamic_cast<SpillableAggregationSlice*>(slice.get());
                    if (spillable == nullptr)
                    {
                        continue;
                    }
                    const auto sliceEnd = spillable->getSliceEnd();
                    /// THE spill-vs-emit / spill-vs-trigger exposure point: a slice handed to the (async) probe MUST be
                    /// resident. If the spiller freed it out from under the trigger this fails (and the fold below UAFs).
                    ASSERT_TRUE(store->isSliceResident(sliceEnd)) << "trigger returned a non-resident slice " << sliceEnd;
                    const auto windowEnd = sliceEnd.getRawValue();
                    {
                        std::lock_guard lock(triggeredMutex);
                        const auto idx = windowEnd / WINDOW_SIZE;
                        if (idx <= NUM_WINDOWS && windowTriggered[idx])
                        {
                            continue; /// already harvested by an earlier trigger pass — do not double-count
                        }
                        if (idx <= NUM_WINDOWS)
                        {
                            windowTriggered[idx] = true;
                        }
                    }
                    for (std::size_t worker = 0; worker < NUM_BUILDERS; ++worker)
                    {
                        local += checksumOfMap(*spillable, WorkerThreadId(static_cast<WorkerThreadId::Underlying>(worker)));
                    }
                }
            }
            observedChecksum.fetch_add(local);
        };

        /// How far the cold/spill horizon lags the build front. Windows in [coldHorizon, buildFront) stay RESIDENT and
        /// COLD, so the hard path / spiller can evict them and the trigger collides on them. >= SPILL_LEAD so the emit
        /// horizon (which lags the spill horizon by SPILL_LEAD) still has cold-unemitted windows to collide on.
        constexpr uint64_t SPILL_LAG = 8;
        /// DETERMINISTIC coldness from the NON-STARVABLE builder path. Coldness is gated on lastSeenWatermark, which the
        /// store learns only via the trigger thread; under -j8 saturation that thread starves, so without this no slice
        /// ever goes cold and evictUntilUnderHard / the background spiller have NOTHING to evict (the real reason
        /// spillCount stayed 0 under load). The builders are never starved (they do the CPU work), so we let them
        /// cooperatively advance the watermark over earlier windows that EVERY builder has finished — identical safety to
        /// the trigger's setLastSeenWatermarkForTest seeding (only ever passes windows with doneCount == NUM_BUILDERS, so
        /// no builder ever writes a cold window; honours INV-3 / watermark-monotonicity). This guarantees cold-resident
        /// slices exist during the build, so the synchronous hard path (also on the builder thread) deterministically
        /// spills → spillCount > 0 regardless of trigger/spiller scheduling. The watermark store is a relaxed atomic
        /// CAS-max-friendly hook; concurrent builder writes are benign (a lagging value only under-selects cold slices).
        const auto seedColdHorizonFromBuilders = [&]
        {
            uint64_t safe = 0;
            while (safe < NUM_WINDOWS && doneCount[safe].load() == NUM_BUILDERS)
            {
                ++safe;
            }
            /// Keep SPILL_LAG windows hot behind the build front so cold slices stay resident long enough to be spilled
            /// and emitted-into-collision rather than immediately drained.
            if (safe > SPILL_LAG)
            {
                const uint64_t coldWindowEnd = (safe - SPILL_LAG) * WINDOW_SIZE;
                store->setLastSeenWatermarkForTest(Timestamp(coldWindowEnd + 1));
                /// Drive the SYNCHRONOUS hard-eviction policy (evictUntilUnderHard) directly from this non-starvable
                /// builder thread via the documented PR-2 drive hook. This is the SAME production eviction code the
                /// implicit getSlicesOrCreate hard path runs; calling it explicitly removes the dependency on the
                /// (under-load racy) "new-slice creation coincides with resident>hard" alignment. With the LOW hard
                /// threshold (2 pages) and SPILL_LAG cold-resident windows present, resident is far above hard, so this
                /// spills >=1 cold slice every time it runs — guaranteeing spillCount > 0 deterministically under -j8
                /// saturation. It races the trigger/background spiller for the same cold SliceEnds, so the
                /// spillInProgress / spillerCv WAIT collision is still exercised concurrently.
                store->spillColdSlicesUnderHardForTest();
            }
        };

        std::array<std::jthread, NUM_BUILDERS> builders;
        for (std::size_t worker = 0; worker < NUM_BUILDERS; ++worker)
        {
            builders[worker] = std::jthread(
                [&, worker]
                {
                    const WorkerThreadId workerThreadId(static_cast<WorkerThreadId::Underlying>(worker));
                    startLatch.arrive_and_wait();
                    for (uint64_t w = 0; w < NUM_WINDOWS; ++w)
                    {
                        const uint64_t windowEnd = (w + 1) * WINDOW_SIZE;
                        /// Strictly-increasing per-thread timestamp stream (a tuple never lands in an already-cold range).
                        const Timestamp ts(windowEnd - 1);
                        const auto slice = store->getSlicesOrCreate(ts, spillableSliceFactory())[0];
                        populateCell(slice, workerThreadId, worker, windowEnd);
                        /// Mark THIS window done for THIS builder. The trigger only fires a window once every builder has
                        /// marked it done, so a builder never re-touches a window that has been triggered/gone-cold.
                        doneCount[w].fetch_add(1);
                        /// Advance the cold horizon from this non-starvable thread so eviction never depends on the
                        /// (possibly starved) trigger/spiller for coldness — see seedColdHorizonFromBuilders.
                        seedColdHorizonFromBuilders();
                    }
                });
        }

        /// Trigger thread. The same-SliceEnd spill-vs-trigger collision needs the spiller to be MID-spilling a slice the
        /// trigger then emits. But the trigger feeder only marks a slice cold (advances the learned watermark) AFTER it
        /// has already emitted it — so emission and coldness coincide and the spiller would always lose the race. To
        /// force the collision we DECOUPLE the two cadences:
        ///   - a SPILL HORIZON leads: once all builders finish window h, we seed lastSeenWatermark past it
        ///     (setLastSeenWatermarkForTest — the documented hook for "a cold-but-unemitted slice"). Windows in
        ///     [emitHorizon, spillHorizon) are now COLD AND NOT YET EMITTED → the spiller / hard path actively spill them.
        ///   - an EMIT HORIZON lags by SPILL_LEAD windows: getTriggerableWindowSlices then emits those same windows,
        ///     frequently catching a slice the spiller is mid-spilling → unspillAndMarkEmitted hits spillInProgress and
        ///     WAITs on spillerCv (the path under test).
        /// No builder ever writes a cold window: the spill horizon only ever passes windows ALL builders have finished
        /// (doneCount == NUM_BUILDERS) and each builder writes each window exactly once, strictly increasing.
        constexpr uint64_t SPILL_LEAD = 4;
        std::jthread trigger(
            [&]
            {
                startLatch.arrive_and_wait();
                uint64_t spillHorizon = 0; /// next window index to mark cold (lead)
                uint64_t emitHorizon = 0; /// next window index to emit (lag)
                while (emitHorizon < NUM_WINDOWS)
                {
                    /// Advance the spill horizon over every window all builders have finished (makes them cold-unemitted).
                    bool advanced = false;
                    while (spillHorizon < NUM_WINDOWS && doneCount[spillHorizon].load() == NUM_BUILDERS)
                    {
                        const uint64_t coldWindowEnd = (spillHorizon + 1) * WINDOW_SIZE;
                        store->setLastSeenWatermarkForTest(Timestamp(coldWindowEnd + 1));
                        ++spillHorizon;
                        advanced = true;
                    }
                    /// Emit lagging SPILL_LEAD windows behind the cold horizon, so the spiller has had time to start.
                    if (emitHorizon + SPILL_LEAD < spillHorizon || (spillHorizon == NUM_WINDOWS && emitHorizon < spillHorizon))
                    {
                        const uint64_t windowEnd = (emitHorizon + 1) * WINDOW_SIZE;
                        const auto triggered = store->getTriggerableWindowSlices(Timestamp(windowEnd + 1));
                        foldTriggered(triggered);
                        ++emitHorizon;
                    }
                    else if (!advanced)
                    {
                        std::this_thread::yield();
                    }
                }
                /// Final sweep at a watermark beyond every window to trigger any stragglers.
                const auto triggered = store->getTriggerableWindowSlices(Timestamp((NUM_WINDOWS + 2) * WINDOW_SIZE));
                foldTriggered(triggered);
            });

        /// Release everyone at once.
        startLatch.arrive_and_wait();

        for (auto& builder : builders)
        {
            builder.join();
        }
        trigger.join();

        /// Drain anything not yet triggered (windows still filling/cold at the end). Phase 1 (L1): getAllNonTriggeredSlices
        /// is now LAZY — it returns the band possibly STILL spilled so the terminal flush can re-materialise one window at
        /// a time. Mirror the handler's paced flush: ensureWindowSlicesResident per window (unspill + mark emitted) before
        /// folding each window into the checksum exactly once. Every returned slice is resident afterwards.
        const auto drained = store->getAllNonTriggeredSlices();
        for (const auto& [windowInfo, slices] : drained)
        {
            store->ensureWindowSlicesResident(slices);
        }
        foldTriggered(drained);

        /// THE UAF DETECTOR: every inserted contribution accounted for exactly once across triggered + drained.
        EXPECT_EQ(observedChecksum.load(), expectedChecksum)
            << "checksum mismatch on iteration " << iteration << " (tuple lost to a half-freed/UAF spill or double-counted)";
        /// Eviction actually happened (the spillInProgress / spillerCv WAIT path was reachable). spillCountForTest is a
        /// monotonic ">= finalized spills" counter (PR-2 L3), so we assert > 0, NOT an exact count. This is now
        /// DETERMINISTIC under load: the LOW hard threshold (see the constructor) forces evictUntilUnderHard on the
        /// non-starvable BUILDER threads during the build phase, so spillCount > 0 holds even when the background spiller
        /// is starved of CPU under -j8 saturation. The same-SliceEnd spill-vs-trigger WAIT path (unspillAndMarkEmitted ->
        /// spillerCv WAIT) remains exercised by the concurrent phase: both the hard-path builder spills AND the
        /// background spiller spill cold-but-unemitted windows that the trigger then emits, and isolated --repeat runs
        /// additionally exercise the collision; the test's PASS/FAIL no longer hinges on background-thread scheduling.
        EXPECT_GT(store->spillCountForTest(), 0U)
            << "no spill occurred on iteration " << iteration << " — the hard-path eviction was never triggered";

        /// deleteState quiesces + joins the spiller before clearing tracked state; the store dtor joins it too.
        store->deleteState();
    }
}

}
