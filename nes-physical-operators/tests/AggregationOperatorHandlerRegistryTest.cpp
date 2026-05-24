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
#include <BaseUnitTest.hpp>
#include <HashMapSlice.hpp>
#include <PipelineExecutionContext.hpp>

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

}
