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

/// 2c0 leak-totality gate for the handler-owned chunk-registry ownership refactor.
///
/// The invariant under test (after EVERY teardown path):
///     pendingChunkCount() == 0  AND  numReleasedChunks() == numRegisteredChunks()
///
/// Leak detection here is COUNTER-BASED, NOT sanitizer-based: ASAN/LSAN is not a usable gate against the
/// `local-rocksdb` Docker image (triplet `none`; CI runs ASAN with `detect_leaks=0`), so the gate observes the
/// handler's own registry directly via the public test accessors. Because every entry in `chunkRegistry` owns its
/// maps via `unique_ptr<HashMap>`, "registry empty AND released == registered" proves every owned map was destroyed
/// exactly once (per-chunk release on drain-ack OR stop()-sweep) and none leaked.
///
/// Group A drives the registry mechanism directly (deterministic, no trigger machinery). It is the primary gate:
/// register / release / stop-sweep / idempotency are the entire ownership mechanism and are fully observable here.
/// Group B drives the public triggerAllWindows() emit path to prove the handler registers exactly one chunk per
/// non-empty window and that the emitted buffer carries the (seq,chunk) key the probe needs to release it.
///
/// Reused harness pieces:
///  - MockedPipelineContext: the emit-capture pattern from EmitPhysicalOperatorTest.cpp:63-111 (a
///    PipelineExecutionContext subclass overriding emitBuffer to capture buffers, backed by a real BufferManager
///    that supplies the getUnpooledBuffer the handler needs).
///  - Slice construction (makeArgs / populate via getHashMapPtrOrCreate + ChainedHashMap::insertEntry): the recipe
///    from SpillableAggregationSliceTest.cpp:71-94, applied here to AggregationSlice (shares the HashMapSlice base).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sequencing/SequenceData.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <folly/Synchronized.h>
#include <gtest/gtest.h>

#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/SpillableAggregationSlice.hpp>
#include <BaseUnitTest.hpp>
#include <HashMapSlice.hpp>
#include <PipelineExecutionContext.hpp>
#include <SliceCacheConfiguration.hpp>
#include <SliceStore/InMemorySpillBackend.hpp>
#include <SliceStore/SpillableTimeBasedSliceStore.hpp>

namespace NES::Testing
{

namespace
{
/// Aggregation-state map geometry (mirrors SpillableAggregationSliceTest constants).
constexpr uint64_t KEY_SIZE = 8;
constexpr uint64_t VALUE_SIZE = 8;
constexpr uint64_t PAGE_SIZE = 256;
constexpr uint64_t NUMBER_OF_BUCKETS = 16;
constexpr uint64_t NUMBER_OF_HASH_MAPS = 1; /// one worker map per slice keeps the unit scope minimal

/// Args with a single no-op cleanup function (HashMapSlice's dtor requires one per input stream).
CreateNewHashMapSliceArgs makeArgs()
{
    auto noopCleanup = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(
        std::function<void(nautilus::val<HashMap*>)>([](nautilus::val<HashMap*>) { }));
    return CreateNewHashMapSliceArgs({std::move(noopCleanup)}, KEY_SIZE, VALUE_SIZE, PAGE_SIZE, NUMBER_OF_BUCKETS);
}
}

/// Emit-capture mock PEC. Same shape as EmitPhysicalOperatorTest::MockedPipelineContext (only the members the
/// aggregation handler actually touches are overridden): emitBuffer (capture) + getBufferManager (real
/// BufferManager => real getUnpooledBuffer) + getNumberOfWorkerThreads (drives WindowBasedOperatorHandler::start).
struct MockedPipelineContext final : PipelineExecutionContext
{
    MockedPipelineContext(folly::Synchronized<std::vector<TupleBuffer>>& buffers, std::shared_ptr<BufferManager> bufferManager)
        : buffers(buffers), bufferManager(std::move(bufferManager))
    {
    }

    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
    {
        buffers.wlock()->emplace_back(buffer);
        return true;
    }

    TupleBuffer allocateTupleBuffer() override { return bufferManager->getBufferBlocking(); }

    TupleBuffer& pinBuffer(TupleBuffer&& tupleBuffer) override
    {
        pinnedBuffers.emplace_back(std::make_unique<TupleBuffer>(tupleBuffer));
        return *pinnedBuffers.back();
    }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return INITIAL<WorkerThreadId>; }
    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }
    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bufferManager; }
    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(1); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }
    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& opHandlers) override
    {
        operatorHandlers = opHandlers;
    }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { INVARIANT(false, "This function should not be called"); }

    ///NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members) lifetime ensured by the test scope.
    folly::Synchronized<std::vector<TupleBuffer>>& buffers;
    std::shared_ptr<BufferManager> bufferManager;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
    std::vector<std::unique_ptr<TupleBuffer>> pinnedBuffers;
};

/// Minimal slice store stub for Group B: getAllNonTriggeredSlices() hands back the windows we pre-populate, so
/// triggerAllWindows() drives the real handler emit path without the full DefaultTimeBasedSliceStore machinery
/// (watermarks, GC, slice assignment). Only the methods triggerAllWindows() reaches are implemented; the rest are
/// unreachable in this test and terminate if hit.
class StubSliceStore final : public WindowSlicesStoreInterface
{
public:
    void addWindow(const WindowInfoAndSequenceNumber& windowInfo, std::vector<std::shared_ptr<Slice>> slices)
    {
        nonTriggered.emplace(windowInfo, std::move(slices));
    }

    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getAllNonTriggeredSlices() override { return nonTriggered; }

    std::vector<std::shared_ptr<Slice>>
    getSlicesOrCreate(Timestamp, const std::function<std::vector<std::shared_ptr<Slice>>(SliceStart, SliceEnd)>&) override
    {
        INVARIANT(false, "StubSliceStore::getSlicesOrCreate must not be called by this test");
    }
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> getTriggerableWindowSlices(Timestamp) override
    {
        INVARIANT(false, "StubSliceStore::getTriggerableWindowSlices must not be called by this test");
    }
    std::optional<std::shared_ptr<Slice>> getSliceBySliceEnd(SliceEnd) override
    {
        INVARIANT(false, "StubSliceStore::getSliceBySliceEnd must not be called by this test");
    }
    void garbageCollectSlicesAndWindows(Timestamp) override { }
    void deleteState() override { nonTriggered.clear(); }
    void incrementNumberOfInputPipelines() override { }
    [[nodiscard]] uint64_t getWindowSize() const override { return 1; }

    /// ensureWindowSlicesResident is intentionally NOT overridden: the base no-op is correct here because this stub's
    /// test slices are always in-memory resident (never spilled), so there is nothing to re-materialise before emit.

private:
    std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>> nonTriggered;
};

class AggregationOperatorHandlerRegistryTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("AggregationOperatorHandlerRegistryTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup AggregationOperatorHandlerRegistryTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create();
    }

protected:
    std::shared_ptr<BufferManager> bufferManager;

    /// Builds a fresh empty ChainedHashMap accumulator suitable for registerChunk (Group A).
    static std::unique_ptr<ChainedHashMap> makeAccumulator()
    {
        return std::make_unique<ChainedHashMap>(KEY_SIZE, VALUE_SIZE, NUMBER_OF_BUCKETS, PAGE_SIZE);
    }

    /// Constructs a handler with a stub slice store. Group A passes a fresh stub it never populates; Group B passes a
    /// populated one. maxNumberOfBuckets only affects slice-creation (unused by these tests' direct/triggered paths).
    static std::unique_ptr<AggregationOperatorHandler> makeHandler(std::unique_ptr<WindowSlicesStoreInterface> store)
    {
        const std::vector<OriginId> inputOrigins{OriginId(1)};
        return std::make_unique<AggregationOperatorHandler>(
            inputOrigins, OriginId(2), std::move(store), /*maxNumberOfBuckets*/ NUMBER_OF_BUCKETS, /*spillEnabled*/ false);
    }

    /// Inserts `count` deterministic tuples into worker 0 of an AggregationSlice (recipe from
    /// SpillableAggregationSliceTest::populate, via the polymorphic HashMapSlice seam).
    void populate(AggregationSlice& slice, const uint64_t count) const
    {
        auto* const map = dynamic_cast<ChainedHashMap*>(slice.getHashMapPtrOrCreate(WorkerThreadId(0)));
        for (uint64_t i = 0; i < count; ++i)
        {
            const uint64_t hash = (i + 1) * 2654435761ULL;
            auto* const entry = static_cast<ChainedHashMapEntry*>(map->insertEntry(hash, bufferManager.get()));
            auto* const payload = reinterpret_cast<std::byte*>(entry) + sizeof(ChainedHashMapEntry);
            const uint64_t key = i;
            const uint64_t value = (i * 7) + 1;
            std::memcpy(payload, &key, sizeof(key));
            std::memcpy(payload + sizeof(key), &value, sizeof(value));
        }
    }

    std::shared_ptr<AggregationSlice> makeSlice(const uint64_t tuplesInWorker0)
    {
        auto slice = std::make_shared<AggregationSlice>(SliceStart(Timestamp(0)), SliceEnd(Timestamp(100)), makeArgs(), NUMBER_OF_HASH_MAPS);
        if (tuplesInWorker0 > 0)
        {
            populate(*slice, tuplesInWorker0);
        }
        return slice;
    }
};

/// ==================== Group A — direct registry mechanism (the primary leak-totality gate) ====================

/// register K distinct chunks, release each → registry empties, released == registered == K.
TEST_F(AggregationOperatorHandlerRegistryTest, registerThenReleaseBalances)
{
    auto handler = makeHandler(std::make_unique<StubSliceStore>());
    constexpr uint64_t K = 5;

    for (uint64_t i = 0; i < K; ++i)
    {
        HashMap* const raw = handler->registerChunk(SequenceNumber(i + 1), ChunkNumber(ChunkNumber::INITIAL), makeAccumulator());
        EXPECT_NE(raw, nullptr); /// returns the live accumulator pointer for EmittedAggregationWindow::finalHashMapPtr
    }
    EXPECT_EQ(handler->pendingChunkCount(), K);
    EXPECT_EQ(handler->numRegisteredChunks(), K);
    EXPECT_EQ(handler->numReleasedChunks(), 0U);

    for (uint64_t i = 0; i < K; ++i)
    {
        handler->releaseChunk(SequenceNumber(i + 1), ChunkNumber(ChunkNumber::INITIAL));
    }
    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numReleasedChunks(), K);
    EXPECT_EQ(handler->numReleasedChunks(), handler->numRegisteredChunks());
}

/// register K, drain J<K, stop() sweeps the rest → registry empty, released == registered == K.
TEST_F(AggregationOperatorHandlerRegistryTest, stopSweepsUndrained)
{
    auto handler = makeHandler(std::make_unique<StubSliceStore>());
    constexpr uint64_t K = 6;
    constexpr uint64_t J = 2;

    for (uint64_t i = 0; i < K; ++i)
    {
        handler->registerChunk(SequenceNumber(i + 1), ChunkNumber(ChunkNumber::INITIAL), makeAccumulator());
    }
    for (uint64_t i = 0; i < J; ++i)
    {
        handler->releaseChunk(SequenceNumber(i + 1), ChunkNumber(ChunkNumber::INITIAL));
    }
    EXPECT_EQ(handler->pendingChunkCount(), K - J);

    folly::Synchronized<std::vector<TupleBuffer>> buffers;
    MockedPipelineContext ctx{buffers, bufferManager};
    handler->stop(QueryTerminationType::Graceful, ctx);

    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numRegisteredChunks(), K);
    EXPECT_EQ(handler->numReleasedChunks(), K);
    EXPECT_EQ(handler->numReleasedChunks(), handler->numRegisteredChunks());
}

/// register K, drain none, stop() sweeps all → registry empty, released == registered == K.
TEST_F(AggregationOperatorHandlerRegistryTest, stopSweepsAllWhenNoneDrained)
{
    auto handler = makeHandler(std::make_unique<StubSliceStore>());
    constexpr uint64_t K = 4;

    for (uint64_t i = 0; i < K; ++i)
    {
        handler->registerChunk(SequenceNumber(i + 1), ChunkNumber(ChunkNumber::INITIAL), makeAccumulator());
    }
    EXPECT_EQ(handler->pendingChunkCount(), K);

    folly::Synchronized<std::vector<TupleBuffer>> buffers;
    MockedPipelineContext ctx{buffers, bufferManager};
    handler->stop(QueryTerminationType::Graceful, ctx);

    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numRegisteredChunks(), K);
    EXPECT_EQ(handler->numReleasedChunks(), K);
    EXPECT_EQ(handler->numReleasedChunks(), handler->numRegisteredChunks());
}

/// releasing the same key twice counts exactly once (no double-count, no double-free crash).
TEST_F(AggregationOperatorHandlerRegistryTest, releaseIsIdempotent)
{
    auto handler = makeHandler(std::make_unique<StubSliceStore>());
    handler->registerChunk(SequenceNumber(1), ChunkNumber(ChunkNumber::INITIAL), makeAccumulator());

    handler->releaseChunk(SequenceNumber(1), ChunkNumber(ChunkNumber::INITIAL));
    handler->releaseChunk(SequenceNumber(1), ChunkNumber(ChunkNumber::INITIAL)); /// second release: absent key, no-op

    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numRegisteredChunks(), 1U);
    EXPECT_EQ(handler->numReleasedChunks(), 1U);
}

/// releasing a never-registered key is a no-op (counters untouched, no crash).
TEST_F(AggregationOperatorHandlerRegistryTest, releaseAbsentKeyIsNoop)
{
    auto handler = makeHandler(std::make_unique<StubSliceStore>());

    handler->releaseChunk(SequenceNumber(42), ChunkNumber(ChunkNumber::INITIAL));

    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numRegisteredChunks(), 0U);
    EXPECT_EQ(handler->numReleasedChunks(), 0U);
}

/// ==================== Group C — concurrent registry mutation (thread-safety gate) ====================
///
/// The registry is folly::Synchronized and the counters are atomics; in production registerChunk (build worker),
/// releaseChunk (probe worker), and stop() (teardown) race on different threads. The single-threaded Group A/B cases
/// leave that thread-safety claim unverified. These cases drive the same mechanism from multiple threads but keep the
/// FINAL assertion deterministic (ordering via thread joins, no sleeps, no liveness asserts), so the leak-totality
/// invariant — pendingChunkCount()==0 AND numReleasedChunks()==numRegisteredChunks()==N — is a hard, non-flaky gate.

/// Concurrent DISJOINT release: pre-register N chunks, then T threads each release a disjoint partition of the keys.
/// No two threads touch the same key, so the registry erase + atomic counters are exercised under contention without
/// any timing dependence; after all joins the registry must be empty and every chunk released exactly once.
TEST_F(AggregationOperatorHandlerRegistryTest, concurrentDisjointReleaseBalances)
{
    auto handler = makeHandler(std::make_unique<StubSliceStore>());
    constexpr uint64_t N = 200;
    constexpr uint64_t NUM_THREADS = 4;

    for (uint64_t i = 0; i < N; ++i)
    {
        handler->registerChunk(SequenceNumber(i + 1), ChunkNumber(ChunkNumber::INITIAL), makeAccumulator());
    }
    ASSERT_EQ(handler->pendingChunkCount(), N);
    ASSERT_EQ(handler->numRegisteredChunks(), N);

    /// Partition [0, N) across threads by stride so each thread owns a disjoint, interleaved subset of the key space.
    std::vector<std::jthread> releasers;
    releasers.reserve(NUM_THREADS);
    for (uint64_t t = 0; t < NUM_THREADS; ++t)
    {
        releasers.emplace_back(
            [&handler, t]
            {
                for (uint64_t i = t; i < N; i += NUM_THREADS)
                {
                    handler->releaseChunk(SequenceNumber(i + 1), ChunkNumber(ChunkNumber::INITIAL));
                }
            });
    }
    releasers.clear(); /// join all jthreads (dtor joins) — establishes ordering for the final assertion

    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numReleasedChunks(), N);
    EXPECT_EQ(handler->numReleasedChunks(), handler->numRegisteredChunks());
}

/// Concurrent register-vs-release race: thread A registers keys [0, N); thread B repeatedly drains whatever keys are
/// currently present, looping until A has finished AND the registry is empty. Producer/consumer overlap is real (no
/// sleeps), but the loop's termination condition (A done && registry empty) makes the post-join assertion
/// deterministic: every registered key has been released exactly once regardless of interleaving.
TEST_F(AggregationOperatorHandlerRegistryTest, concurrentRegisterVsReleaseDrainsAll)
{
    auto handler = makeHandler(std::make_unique<StubSliceStore>());
    constexpr uint64_t N = 200;
    std::atomic<bool> registerDone{false};
    std::atomic<uint64_t> highestRegistered{0}; /// count of keys A has registered so far (release only chases these)

    std::jthread producer(
        [&handler, &registerDone, &highestRegistered]
        {
            for (uint64_t i = 0; i < N; ++i)
            {
                handler->registerChunk(SequenceNumber(i + 1), ChunkNumber(ChunkNumber::INITIAL), makeAccumulator());
                highestRegistered.store(i + 1, std::memory_order_release);
            }
            registerDone.store(true, std::memory_order_release);
        });

    std::jthread consumer(
        [&handler, &registerDone, &highestRegistered]
        {
            uint64_t next = 0; /// next key index to attempt to release; releaseChunk on an absent key is a safe no-op
            while (true)
            {
                const uint64_t available = highestRegistered.load(std::memory_order_acquire);
                while (next < available)
                {
                    handler->releaseChunk(SequenceNumber(next + 1), ChunkNumber(ChunkNumber::INITIAL));
                    ++next;
                }
                if (registerDone.load(std::memory_order_acquire) && next >= N)
                {
                    break; /// A finished and we have attempted release of every key it produced
                }
            }
        });

    producer.join();
    consumer.join();

    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numRegisteredChunks(), N);
    EXPECT_EQ(handler->numReleasedChunks(), N);
    EXPECT_EQ(handler->numReleasedChunks(), handler->numRegisteredChunks());
}

/// ==================== Group B — integration via the emit path (triggerAllWindows) ====================

/// triggerAllWindows registers exactly one chunk per non-empty window; the emitted buffer carries the (seq,chunk)
/// key the probe uses to release it. Draining via the buffer keys then balances released == registered.
TEST_F(AggregationOperatorHandlerRegistryTest, triggerRegistersOnePerNonEmptyWindow)
{
    auto store = std::make_unique<StubSliceStore>();
    auto* const storePtr = store.get();

    /// Three non-empty windows (distinct window ends => distinct map keys; distinct sequence numbers).
    constexpr uint64_t NUM_NON_EMPTY_WINDOWS = 3;
    for (uint64_t w = 0; w < NUM_NON_EMPTY_WINDOWS; ++w)
    {
        const WindowInfoAndSequenceNumber wi{WindowInfo(w * 100, (w * 100) + 100), SequenceNumber(w + 1)};
        storePtr->addWindow(wi, {makeSlice(/*tuples*/ 4 + w)});
    }

    auto handler = makeHandler(std::move(store));

    folly::Synchronized<std::vector<TupleBuffer>> buffers;
    MockedPipelineContext ctx{buffers, bufferManager};
    handler->start(ctx, /*localStateVariableId*/ 0); /// sets numberOfWorkerThreads from the mock PEC

    handler->triggerAllWindows(&ctx);

    /// One registered chunk per non-empty window; one emitted buffer per window.
    EXPECT_EQ(handler->numRegisteredChunks(), NUM_NON_EMPTY_WINDOWS);
    EXPECT_EQ(handler->pendingChunkCount(), NUM_NON_EMPTY_WINDOWS);
    EXPECT_EQ(buffers.rlock()->size(), NUM_NON_EMPTY_WINDOWS);

    /// Simulate the probe drain: read (seq,chunk) from each captured buffer and release the registry entry.
    for (const auto& buffer : *buffers.rlock())
    {
        handler->releaseChunk(buffer.getSequenceNumber(), buffer.getChunkNumber());
    }

    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numReleasedChunks(), handler->numRegisteredChunks());
    EXPECT_EQ(handler->numReleasedChunks(), NUM_NON_EMPTY_WINDOWS);
}

/// A window whose slices hold zero tuples still emits a buffer but registers NO chunk (null accumulator). Mixed with
/// non-empty windows, only the non-empty ones are registered; the empty window's (seq,chunk) release is a no-op.
TEST_F(AggregationOperatorHandlerRegistryTest, emptyWindowRegistersNothing)
{
    auto store = std::make_unique<StubSliceStore>();
    auto* const storePtr = store.get();

    /// Window 1: non-empty (registers). Window 2: all-empty slices (emits a buffer, registers nothing).
    storePtr->addWindow({WindowInfo(0, 100), SequenceNumber(1)}, {makeSlice(/*tuples*/ 4)});
    storePtr->addWindow({WindowInfo(100, 200), SequenceNumber(2)}, {makeSlice(/*tuples*/ 0)});

    auto handler = makeHandler(std::move(store));

    folly::Synchronized<std::vector<TupleBuffer>> buffers;
    MockedPipelineContext ctx{buffers, bufferManager};
    handler->start(ctx, /*localStateVariableId*/ 0);

    handler->triggerAllWindows(&ctx);

    /// Both windows emit a buffer, but only the non-empty one registers a chunk.
    EXPECT_EQ(buffers.rlock()->size(), 2U);
    EXPECT_EQ(handler->numRegisteredChunks(), 1U);
    EXPECT_EQ(handler->pendingChunkCount(), 1U);

    /// Releasing every emitted buffer's key drains the one registered chunk; the empty window's key is a no-op.
    for (const auto& buffer : *buffers.rlock())
    {
        handler->releaseChunk(buffer.getSequenceNumber(), buffer.getChunkNumber());
    }

    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numReleasedChunks(), 1U);
    EXPECT_EQ(handler->numReleasedChunks(), handler->numRegisteredChunks());
}

/// ==================== Group D — 2d partitioned terminal emit via triggerAllWindows (P>1) ====================
///
/// Drives the REAL handler->triggerAllWindows() over a SpillableTimeBasedSliceStore configured with P>1, with the
/// window's slice SPILLED P-way (forceSpill) so the emit path exercises the spilled-stream branch. 3 distinct keys over
/// P=8 residues: by pigeonhole at most 3 partitions can be non-empty, so K < P is GUARANTEED (skip-empty proven
/// deterministically). The keys are chosen so they land in DISTINCT residues (hash=(i+1)*2654435761 and 2654435761%8==1
/// ⇒ residue==(i+1)%8, so i=0,1,2 → residues 1,2,3), so K==3 > 1 is also guaranteed — proving REAL partitioning happens
/// (before the H1 unspill/spill-P fix the store spilled P-way but a P=1 read collapsed everything to one partition).
/// Asserts the REVISED L3 skip-empty + contiguous-renumber contract end-to-end:
///   - K chunks emitted, 1 < K < P (real partitioning AND skip-empty: empty partitions are NOT emitted);
///   - chunk numbers are contiguous INITIAL..INITIAL+K-1, with lastChunk set ONLY on the highest-numbered (K-th) chunk;
///   - every emitted chunk carries >=1 tuple (no empty/null map ever reaches the probe);
///   - the UNION of the K chunks' per-worker input-map contents == the full window aggregate (key-disjoint + complete);
///   - leak-totality: registered==released after probe drain AND again after stop().
TEST_F(AggregationOperatorHandlerRegistryTest, partitionedTriggerEmitsKContiguousChunksAndSkipsEmpty)
{
    constexpr uint64_t KEY_SIZE_LOCAL = 8;
    constexpr uint64_t VALUE_SIZE_LOCAL = 8;
    constexpr uint64_t PAGE_SIZE_LOCAL = 256;
    constexpr uint64_t NUMBER_OF_BUCKETS_LOCAL = 16;
    constexpr uint64_t NUMBER_OF_HASH_MAPS_LOCAL = 4;
    constexpr uint64_t WINDOW_SIZE_LOCAL = 100;
    constexpr uint64_t P = 8; /// FEWER distinct keys than P below ⇒ at least P-keys partitions empty by pigeonhole.
    constexpr uint64_t DISTINCT_KEYS = 3; /// 3 keys in distinct residues ⇒ 1 < K==3 < 8 GUARANTEED (real split + skip-empty).
    const uint64_t entrySizeLocal = sizeof(ChainedHashMapEntry) + KEY_SIZE_LOCAL + VALUE_SIZE_LOCAL;
    const uint64_t entriesPerPageLocal = PAGE_SIZE_LOCAL / entrySizeLocal;
    const uint64_t payloadSize = entrySizeLocal - sizeof(ChainedHashMapEntry);

    using Entry = std::pair<uint64_t, std::vector<std::byte>>; /// (hash, key+value payload)
    auto collectFromMap = [&](const ChainedHashMap* map, std::vector<Entry>& out)
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
    };
    auto sortEntries = [](std::vector<Entry>& v)
    { std::ranges::sort(v, [](const Entry& a, const Entry& b) { return a.first != b.first ? a.first < b.first : a.second < b.second; }); };

    auto sliceArgs = [&]
    {
        auto noopCleanup = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(
            std::function<void(nautilus::val<HashMap*>)>([](nautilus::val<HashMap*>) { }));
        return CreateNewHashMapSliceArgs({std::move(noopCleanup)}, KEY_SIZE_LOCAL, VALUE_SIZE_LOCAL, PAGE_SIZE_LOCAL, NUMBER_OF_BUCKETS_LOCAL);
    };

    auto backend = std::make_unique<InMemorySpillBackend>();
    auto store = std::make_unique<SpillableTimeBasedSliceStore>(
        WINDOW_SIZE_LOCAL,
        WINDOW_SIZE_LOCAL,
        SliceCacheConfiguration{},
        std::move(backend),
        entriesPerPageLocal,
        PAGE_SIZE_LOCAL,
        /*soft*/ 1024ULL * 1024ULL,
        /*hard*/ 2ULL * 1024ULL * 1024ULL,
        /*emitLag*/ 0,
        /*numberOfPartitions*/ P);
    store->setBufferProvider(bufferManager);
    auto* const storePtr = store.get();

    /// One window (one tumbling slice). Populate one worker map with DISTINCT_KEYS distinct keys.
    const auto slice = storePtr->getSlicesOrCreate(Timestamp(50), [&](const SliceStart s, const SliceEnd e)
        -> std::vector<std::shared_ptr<Slice>> { return {std::make_shared<SpillableAggregationSlice>(s, e, sliceArgs(), NUMBER_OF_HASH_MAPS_LOCAL)}; })[0];
    {
        auto* const map = dynamic_cast<ChainedHashMap*>(dynamic_cast<SpillableAggregationSlice*>(slice.get())->getHashMapPtrOrCreate(WorkerThreadId(0)));
        for (uint64_t i = 0; i < DISTINCT_KEYS; ++i)
        {
            const uint64_t hash = (i + 1) * 2654435761ULL;
            auto* const entry = static_cast<ChainedHashMapEntry*>(map->insertEntry(hash, bufferManager.get()));
            auto* const payload = reinterpret_cast<std::byte*>(entry) + sizeof(ChainedHashMapEntry);
            const uint64_t key = i;
            const uint64_t value = (i * 7) + 1;
            std::memcpy(payload, &key, sizeof(key));
            std::memcpy(payload + sizeof(key), &value, sizeof(value));
        }
    }

    /// Capture the full window's expected (hash,payload) set BEFORE spilling.
    std::vector<Entry> expected;
    collectFromMap(dynamic_cast<const ChainedHashMap*>(dynamic_cast<const SpillableAggregationSlice*>(slice.get())->getHashMapPtr(WorkerThreadId(0))), expected);
    sortEntries(expected);
    ASSERT_EQ(expected.size(), DISTINCT_KEYS);

    /// Spill the slice P-way so triggerAllWindows exercises the spilled-stream branch (few keys are still spillable).
    storePtr->forceSpill(slice->getSliceEnd());
    ASSERT_FALSE(storePtr->isSliceResident(slice->getSliceEnd()));

    const std::vector<OriginId> inputOrigins{OriginId(1)};
    auto handler = std::make_unique<AggregationOperatorHandler>(
        inputOrigins, OriginId(2), std::move(store), /*maxNumberOfBuckets*/ NUMBER_OF_BUCKETS_LOCAL, /*spillEnabled*/ true);

    folly::Synchronized<std::vector<TupleBuffer>> buffers;
    MockedPipelineContext ctx{buffers, bufferManager};
    handler->start(ctx, /*localStateVariableId*/ 0);

    /// getAllNonTriggeredSlices() (reached via the partitioned triggerAllWindows) decrements numberOfActiveInputPipelines
    /// and PRECONDITIONs it > 0, so arm it once first (mirrors SpillableTimeBasedSliceStoreTest.cpp:897-898).
    storePtr->incrementNumberOfInputPipelines();
    handler->triggerAllWindows(&ctx);

    /// K = number of emitted chunks = number of non-empty partitions. The 3 keys land in 3 DISTINCT residues, so K==3.
    const auto emitted = *buffers.rlock();
    const auto K = emitted.size();
    ASSERT_GT(K, 1U); /// REAL partitioning happened — the spilled state was split across MULTIPLE chunks (H1 proof).
    ASSERT_LE(K, DISTINCT_KEYS); /// at most one non-empty partition per distinct key
    ASSERT_LT(K, P); /// skip-empty: >=1 of the 8 partitions was empty and therefore NOT emitted (pigeonhole, 3 < 8)
    EXPECT_EQ(handler->numRegisteredChunks(), K); /// one registered chunk per non-empty partition
    EXPECT_EQ(handler->pendingChunkCount(), K);

    /// Contiguous chunk numbers [INITIAL, INITIAL+K-1]; lastChunk ONLY on the highest; collect the union of contents.
    std::vector<uint64_t> chunkNumbers;
    std::vector<Entry> unioned;
    uint64_t totalTuples = 0;
    uint64_t lastChunkCount = 0;
    uint64_t highestChunkWithLast = 0;
    const auto windowSeq = emitted.front().getSequenceNumber();
    for (const auto& buffer : emitted)
    {
        chunkNumbers.push_back(buffer.getChunkNumber().getRawValue());
        totalTuples += buffer.getNumberOfTuples();
        if (buffer.isLastChunk())
        {
            ++lastChunkCount;
            highestChunkWithLast = buffer.getChunkNumber().getRawValue();
        }
        EXPECT_EQ(buffer.getSequenceNumber(), windowSeq); /// one window ⇒ ALL K chunks share one sequence number
        /// Every emitted chunk has >=1 tuple (skip-empty ⇒ no empty/null map ever reaches the probe).
        EXPECT_GT(buffer.getNumberOfTuples(), 0U);

        /// Read the EmittedAggregationWindow out of the buffer and union its per-worker input maps' contents — the SAME
        /// maps the probe combines (handler-owned in chunkRegistry; the buffer carries non-owning raw pointers).
        const auto* const window = reinterpret_cast<const EmittedAggregationWindow*>(buffer.getAvailableMemoryArea().data());
        for (uint64_t h = 0; h < window->numberOfHashMaps; ++h)
        {
            collectFromMap(dynamic_cast<const ChainedHashMap*>(window->hashMaps[h]), unioned);
        }
    }
    std::ranges::sort(chunkNumbers);
    for (uint64_t j = 0; j < K; ++j)
    {
        EXPECT_EQ(chunkNumbers[j], ChunkNumber::INITIAL + j); /// contiguous renumber from INITIAL, no gaps
    }
    EXPECT_EQ(lastChunkCount, 1U); /// exactly one lastChunk
    EXPECT_EQ(highestChunkWithLast, ChunkNumber::INITIAL + (K - 1)); /// ...and it is the highest-numbered chunk

    /// PRIMARY correctness proof: total tuples AND the unioned contents over the K chunks equal the full window aggregate
    /// (key-disjoint + complete — every key emitted exactly once across the K chunks, nothing added/dropped/duplicated).
    EXPECT_EQ(totalTuples, DISTINCT_KEYS);
    ASSERT_EQ(unioned.size(), expected.size()) << "K chunks streamed " << unioned.size() << " entries; window holds " << expected.size();
    sortEntries(unioned);
    ASSERT_EQ(unioned, expected) << "union of the K emitted chunks != the full window aggregate (key-disjoint+complete violated)";

    /// Probe drain: release every emitted chunk's (seq,chunk) key. Leak-totality after drain.
    for (const auto& buffer : emitted)
    {
        handler->releaseChunk(buffer.getSequenceNumber(), buffer.getChunkNumber());
    }
    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numReleasedChunks(), handler->numRegisteredChunks());

    /// stop() sweep is a no-op now (all drained), and re-confirms leak-totality after teardown.
    handler->stop(QueryTerminationType::Graceful, ctx);
    EXPECT_EQ(handler->pendingChunkCount(), 0U);
    EXPECT_EQ(handler->numReleasedChunks(), handler->numRegisteredChunks());

    /// deleteState() to quiesce the store's background spiller cleanly before teardown.
    storePtr->deleteState();
}

}
