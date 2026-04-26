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

/// Self-contained micro-benchmark for slice-store layouts.
///
/// The production interface lives in WindowSlicesStoreInterface.hpp; this file
/// deliberately re-implements a minimal version so we can iterate on layouts
/// (locking hashmap, ring buffer, ...) without dragging in folly, nautilus,
/// the slice cache, etc. The minimal contract is:
///
///   build phase   (concurrent): getOrCreate(sliceEnd) -> Slice*
///   trigger phase (single thr): drainUpTo(watermark)  -> Vec<Slice>
///
/// "Single-threaded trigger" reflects the assumption from the operator: once
/// the watermark passes a slice's end, no late tuples arrive for it, so drain
/// can run without holding the build lock. The naive impl below is still fully
/// locked because it is the baseline; the ring-buffer variant is where lock-free
/// drain becomes interesting.

#include <algorithm>
#include <atomic>
#include <barrier>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <benchmark/benchmark.h>

namespace
{

using Timestamp = std::uint64_t;
using SequenceNumber = std::uint64_t;
using WorkerThreadId = std::uint32_t;

/// Per-thread aggregate slot. Aligned to a cache line so adjacent worker
/// threads writing to the same slice don't false-share. Kept tiny on purpose
/// so the benchmark measures store overhead, not the aggregator.
struct alignas(64) ThreadLocalState
{
    std::uint64_t count = 0;
    std::uint64_t sum = 0;
    std::byte _pad[64 - 2 * sizeof(std::uint64_t)]{};
};
static_assert(sizeof(ThreadLocalState) == 64);

struct Slice
{
    Timestamp sliceStart;
    Timestamp sliceEnd;
    std::vector<ThreadLocalState> perThread;

    Slice(Timestamp s, Timestamp e, std::size_t threads) : sliceStart(s), sliceEnd(e), perThread(threads) { }
};

/// Minimal slice-store interface. Mirrors getSlicesOrCreate / drainUpTo from
/// WindowSlicesStoreInterface but stripped to the operations we want to time.
class SliceStore
{
public:
    virtual ~SliceStore() = default;
    /// Build phase: lookup or create a slice by its end timestamp.
    virtual std::shared_ptr<Slice> getOrCreate(Timestamp sliceEnd) = 0;
    /// Trigger phase: remove and return all slices whose end <= watermark.
    /// Caller assumes no concurrent build for those slices (no late tuples).
    virtual std::vector<std::shared_ptr<Slice>> drainUpTo(Timestamp watermark) = 0;
};

/// Naive baseline: std::unordered_map guarded by one mutex. Both build and
/// drain take the lock. Note that slice ends are typically multiples of the
/// window size, so identity-hashing them into a power-of-two bucket count
/// causes systematic collisions; that is part of what we want to measure.
class LockingHashMapSliceStore final : public SliceStore
{
public:
    LockingHashMapSliceStore(std::uint64_t windowSize, std::size_t numThreads)
        : windowSize(windowSize), numThreads(numThreads)
    {
    }

    std::shared_ptr<Slice> getOrCreate(Timestamp sliceEnd) override
    {
        std::lock_guard<std::mutex> lock(mutex);
        if (auto it = slices.find(sliceEnd); it != slices.end())
        {
            return it->second;
        }
        auto slice = std::make_shared<Slice>(sliceEnd - windowSize, sliceEnd, numThreads);
        slices.emplace(sliceEnd, slice);
        return slice;
    }

    std::vector<std::shared_ptr<Slice>> drainUpTo(Timestamp watermark) override
    {
        std::vector<std::shared_ptr<Slice>> drained;
        std::lock_guard<std::mutex> lock(mutex);
        for (auto it = slices.begin(); it != slices.end();)
        {
            if (it->first <= watermark)
            {
                drained.push_back(std::move(it->second));
                it = slices.erase(it);
            }
            else
            {
                ++it;
            }
        }
        return drained;
    }

private:
    std::uint64_t windowSize;
    std::size_t numThreads;
    std::mutex mutex;
    std::unordered_map<Timestamp, std::shared_ptr<Slice>> slices;
};

/// Ring-buffer variant: TODO.
/// Design sketch:
///   - Bounded watermark lag => bounded number of live slices (capacity = lag/windowSize).
///   - Slot index = (sliceEnd / windowSize) % capacity. O(1) lookup, no hashing.
///   - Build phase publishes via atomic CAS on the slot's slice pointer.
///   - Drain advances a tail cursor; freed slots can be reused without ref tracking
///     because the no-late-tuples invariant holds past the watermark.
///   - Watermark gap > capacity is a programming error / requires fallback path.
/// class RingBufferSliceStore final : public SliceStore { ... };

/// ---------------------------------------------------------------------------
/// Workload
/// ---------------------------------------------------------------------------
///
/// Generation pipeline (matches the engine's invariants):
///   1. Produce one global, monotonically-increasing event stream.
///   2. Chunk it into buffers of random size (Uniform[min,max], non-empty).
///      Each buffer gets a contiguous sequence number in chunking order.
///      Within a buffer, event timestamps are strictly monotonic.
///   3. Each buffer's advisory watermark is its LAST event's timestamp - the
///      source guarantees it will not emit any further event below that ts.
///   4. Shuffle the arrival order of buffers within a sliding window of
///      `bufferOutOfOrderSpan` to model out-of-order delivery to the operator.
///      Sequence numbers stay attached to their buffers; only arrival changes.
///   5. (Done in BM_SliceStore_Concurrent) Round-robin the shuffled buffers
///      across N worker threads.

struct Event
{
    Timestamp ts;
    std::uint64_t value;
};

/// A buffer as it arrives at the operator: monotonic seq, advisory watermark
/// equal to its last event's timestamp, and a strictly ts-monotonic batch.
struct Buffer
{
    SequenceNumber seq;
    Timestamp watermark;
    std::vector<Event> events;
};

struct WorkloadConfig
{
    std::uint64_t windowSlide = 1000;        ///< Slice width.
    std::uint64_t windowSize = 4000;         ///< Window size; > slide => sliding window.
    std::size_t numBuffers = 4096;           ///< Total buffers per iteration.
    std::size_t minBufferSize = 128;         ///< Lower bound of Uniform[min,max] events/buffer.
    std::size_t maxBufferSize = 384;         ///< Upper bound; mean ~ 256.
    std::size_t bufferOutOfOrderSpan = 4;    ///< Sliding shuffle window over arrivals.
    std::uint64_t eventInterArrival = 1;     ///< ts increment between consecutive events.
    unsigned seed = 0xC0FFEEu;
};

std::vector<Buffer> generateWorkload(const WorkloadConfig& cfg)
{
    std::mt19937_64 rng(cfg.seed);
    std::uniform_int_distribution<std::size_t> sizeDist(
        std::max<std::size_t>(1, cfg.minBufferSize), std::max<std::size_t>(1, cfg.maxBufferSize));

    /// Steps 1 + 2: emit a monotonic event stream and chunk it into buffers.
    /// Done in one pass since each buffer's events are a contiguous slice of
    /// the global stream.
    Timestamp ts = cfg.windowSlide; ///< Start past the first slice boundary.
    std::uint64_t valueCounter = 0;
    std::vector<Buffer> buffers;
    buffers.reserve(cfg.numBuffers);
    for (std::size_t s = 0; s < cfg.numBuffers; ++s)
    {
        Buffer b;
        b.seq = s;
        const std::size_t sz = sizeDist(rng);
        b.events.reserve(sz);
        for (std::size_t e = 0; e < sz; ++e)
        {
            b.events.push_back({ts, valueCounter++});
            ts += cfg.eventInterArrival;
        }
        b.watermark = b.events.back().ts;
        buffers.push_back(std::move(b));
    }

    /// Step 4: shuffle arrival order within a sliding window.
    if (cfg.bufferOutOfOrderSpan > 0)
    {
        for (std::size_t i = 0; i + 1 < buffers.size(); ++i)
        {
            const std::size_t hi = std::min(i + cfg.bufferOutOfOrderSpan + 1, buffers.size());
            const std::size_t pick = std::uniform_int_distribution<std::size_t>(i, hi - 1)(rng);
            if (pick != i)
            {
                std::swap(buffers[i], buffers[pick]);
            }
        }
    }
    return buffers;
}

/// Tracks the global watermark: it only advances once all seq numbers up to
/// the current one have been acknowledged (matches MultiOriginWatermarkProcessor
/// semantics for a single origin). Thread-safe: build threads ack their own
/// partition's seqs; whichever thread closes the next contiguous prefix gets
/// to observe the advance and trigger a drain.
class WatermarkTracker
{
public:
    Timestamp ack(SequenceNumber seq, Timestamp advisory)
    {
        std::lock_guard<std::mutex> lock(mutex);
        pending.emplace(seq, advisory);
        for (auto it = pending.find(nextSeq); it != pending.end(); it = pending.find(nextSeq))
        {
            global = std::max(global, it->second);
            pending.erase(it);
            ++nextSeq;
        }
        return global;
    }

    [[nodiscard]] Timestamp current() const
    {
        std::lock_guard<std::mutex> lock(mutex);
        return global;
    }

private:
    mutable std::mutex mutex;
    SequenceNumber nextSeq = 0;
    Timestamp global = 0;
    std::unordered_map<SequenceNumber, Timestamp> pending;
};

/// Helper: compute the slice end for an event timestamp under a tumbling
/// model (slice width == windowSlide).
inline Timestamp sliceEndFor(Timestamp ts, std::uint64_t windowSlide)
{
    return ((ts / windowSlide) + 1) * windowSlide;
}

/// Inspect a generated workload: counts what the slice store will actually
/// see. Slice fan-out (windowSize / windowSlide) is applied here so the
/// reported counters match what the runtime executes.
struct WorkloadStats
{
    std::size_t totalEvents = 0;          ///< Total events across all buffers.
    std::size_t totalLookups = 0;         ///< getOrCreate calls (= events * slicesPerWindow).
    std::size_t uniqueSliceEnds = 0;      ///< Number of distinct slice ends across the run.
    std::size_t peakLiveSlices = 0;       ///< Max concurrent live slices (after each buffer).
    std::size_t outOfOrderBuffers = 0;    ///< Buffers whose seq arrives below the running max.
};

WorkloadStats analyzeWorkload(const std::vector<Buffer>& buffers, const WorkloadConfig& cfg)
{
    const std::uint64_t slicesPerWindow = std::max<std::uint64_t>(1, cfg.windowSize / cfg.windowSlide);
    WorkloadStats stats;
    std::unordered_set<Timestamp> allEnds;
    std::set<Timestamp> live;
    WatermarkTracker wm;
    SequenceNumber maxSeq = 0;
    bool firstBuffer = true;
    for (const auto& b : buffers)
    {
        if (!firstBuffer && b.seq < maxSeq)
        {
            ++stats.outOfOrderBuffers;
        }
        firstBuffer = false;
        maxSeq = std::max(maxSeq, b.seq);

        for (const auto& e : b.events)
        {
            ++stats.totalEvents;
            const Timestamp first = sliceEndFor(e.ts, cfg.windowSlide);
            for (std::uint64_t k = 0; k < slicesPerWindow; ++k)
            {
                const Timestamp se = first + k * cfg.windowSlide;
                ++stats.totalLookups;
                allEnds.insert(se);
                live.insert(se);
            }
        }
        const Timestamp newWm = wm.ack(b.seq, b.watermark);
        for (auto it = live.begin(); it != live.end();)
        {
            if (*it <= newWm)
            {
                it = live.erase(it);
            }
            else
            {
                ++it;
            }
        }
        stats.peakLiveSlices = std::max(stats.peakLiveSlices, live.size());
    }
    stats.uniqueSliceEnds = allEnds.size();
    return stats;
}

/// One pass of the workload through a store. Each event fans out to
/// `windowSize / windowSlide` consecutive slice ends to model sliding
/// windows; for tumbling (slide == size) this is one getOrCreate per event.
template <typename Store>
std::uint64_t runWorkload(Store& store, const std::vector<Buffer>& buffers, const WorkloadConfig& cfg, WorkerThreadId tid)
{
    const std::uint64_t slicesPerWindow = std::max<std::uint64_t>(1, cfg.windowSize / cfg.windowSlide);
    WatermarkTracker wm;
    std::uint64_t sink = 0;
    for (const auto& b : buffers)
    {
        for (const auto& e : b.events)
        {
            const Timestamp first = sliceEndFor(e.ts, cfg.windowSlide);
            for (std::uint64_t k = 0; k < slicesPerWindow; ++k)
            {
                auto slice = store.getOrCreate(first + k * cfg.windowSlide);
                auto& slot = slice->perThread[tid];
                slot.count += 1;
                slot.sum += e.value;
            }
        }
        const Timestamp newWm = wm.ack(b.seq, b.watermark);
        auto drained = store.drainUpTo(newWm);
        for (auto& d : drained)
        {
            for (auto& slot : d->perThread)
            {
                sink ^= slot.count + slot.sum;
            }
        }
    }
    return sink;
}

/// ---------------------------------------------------------------------------
/// Benchmarks
/// ---------------------------------------------------------------------------

void attachStatsCounters(benchmark::State& state, const WorkloadStats& s)
{
    state.counters["events"] = static_cast<double>(s.totalEvents);
    state.counters["lookups"] = static_cast<double>(s.totalLookups);
    state.counters["uniqSlices"] = static_cast<double>(s.uniqueSliceEnds);
    state.counters["peakLive"] = static_cast<double>(s.peakLiveSlices);
    state.counters["ooBuffers"] = static_cast<double>(s.outOfOrderBuffers);
}

/// Sweep the buffer-level out-of-order span. With the new model this is the
/// only source of out-of-orderness: a larger span means buffers arrive in a
/// more scrambled seq order, the WatermarkTracker pending set grows, and more
/// slices stay live at once.
template <class Store>
void BM_SliceStore_BufferOOO(benchmark::State& state)
{
    WorkloadConfig cfg;
    cfg.bufferOutOfOrderSpan = static_cast<std::size_t>(state.range(0));
    const auto buffers = generateWorkload(cfg);
    const auto stats = analyzeWorkload(buffers, cfg);

    for (auto _ : state)
    {
        Store store(cfg.windowSlide, /*numThreads=*/1);
        std::uint64_t sink = runWorkload(store, buffers, cfg, /*tid=*/0);
        benchmark::DoNotOptimize(sink);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * stats.totalLookups));
    attachStatsCounters(state, stats);
}

/// Sweep the sliding-window fan-out (windowSize / windowSlide). 1 = tumbling.
/// Higher = each event hits more slices, increasing both lookup volume and
/// the live-slice working set.
template <class Store>
void BM_SliceStore_SliceFanout(benchmark::State& state)
{
    WorkloadConfig cfg;
    cfg.windowSize = cfg.windowSlide * static_cast<std::uint64_t>(state.range(0));
    const auto buffers = generateWorkload(cfg);
    const auto stats = analyzeWorkload(buffers, cfg);

    for (auto _ : state)
    {
        Store store(cfg.windowSlide, /*numThreads=*/1);
        std::uint64_t sink = runWorkload(store, buffers, cfg, /*tid=*/0);
        benchmark::DoNotOptimize(sink);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * stats.totalLookups));
    attachStatsCounters(state, stats);
}

BENCHMARK_TEMPLATE(BM_SliceStore_BufferOOO, LockingHashMapSliceStore)
    ->Arg(0)
    ->Arg(2)
    ->Arg(8)
    ->Arg(32)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_TEMPLATE(BM_SliceStore_SliceFanout, LockingHashMapSliceStore)
    ->Arg(1)
    ->Arg(4)
    ->Arg(16)
    ->Arg(64)
    ->Unit(benchmark::kMillisecond);

/// Pre-partition buffers round-robin across N worker threads. Each thread
/// owns a fixed list of buffer indices for the lifetime of the benchmark.
struct ThreadPartition
{
    std::vector<std::size_t> bufferIndices;
};

std::vector<ThreadPartition> partitionBuffers(const std::vector<Buffer>& buffers, std::size_t numThreads)
{
    std::vector<ThreadPartition> parts(numThreads);
    for (auto& p : parts)
    {
        p.bufferIndices.reserve((buffers.size() / numThreads) + 1);
    }
    for (std::size_t i = 0; i < buffers.size(); ++i)
    {
        parts[i % numThreads].bufferIndices.push_back(i);
    }
    return parts;
}

/// Multi-threaded build phase. state.range(0) = number of worker threads.
///
/// Workers are spawned once for the whole benchmark and synchronised with two
/// barriers, so per-iteration thread spawn/join is not on the timing path.
/// All threads share one store and one WatermarkTracker - getOrCreate
/// contends on the store's mutex; ack() contends on the tracker's. Whichever
/// thread closes the next contiguous prefix of seqs observes the watermark
/// advance and triggers a drain.
///
/// UseRealTime() is set on the registration so throughput is computed against
/// wall-clock; without it Google Benchmark only counts the controlling
/// thread's CPU time and reports nonsense items/s for spawned-thread work.
template <class Store>
void BM_SliceStore_Concurrent(benchmark::State& state)
{
    const auto numThreads = static_cast<std::size_t>(state.range(0));
    WorkloadConfig cfg;
    const auto buffers = generateWorkload(cfg);
    const auto stats = analyzeWorkload(buffers, cfg);
    const auto partitions = partitionBuffers(buffers, numThreads);
    const std::uint64_t slicesPerWindow = std::max<std::uint64_t>(1, cfg.windowSize / cfg.windowSlide);

    /// Per-iteration shared state. Populated by the controller before each
    /// startBarrier; workers consume it; controller resets between iterations.
    std::unique_ptr<Store> store;
    std::unique_ptr<WatermarkTracker> wm;
    std::atomic<std::uint64_t> sink{0};
    std::atomic<bool> stop{false};

    /// numThreads + 1: workers + controller (the benchmark thread).
    std::barrier startBarrier(static_cast<std::ptrdiff_t>(numThreads + 1));
    std::barrier doneBarrier(static_cast<std::ptrdiff_t>(numThreads + 1));

    std::vector<std::thread> workers;
    workers.reserve(numThreads);
    for (std::size_t tid = 0; tid < numThreads; ++tid)
    {
        workers.emplace_back(
            [&, tid]()
            {
                for (;;)
                {
                    startBarrier.arrive_and_wait();
                    if (stop.load(std::memory_order_acquire))
                    {
                        return;
                    }
                    std::uint64_t local = 0;
                    for (const std::size_t idx : partitions[tid].bufferIndices)
                    {
                        const auto& b = buffers[idx];
                        for (const auto& e : b.events)
                        {
                            const Timestamp first = sliceEndFor(e.ts, cfg.windowSlide);
                            for (std::uint64_t k = 0; k < slicesPerWindow; ++k)
                            {
                                auto slice = store->getOrCreate(first + k * cfg.windowSlide);
                                auto& slot = slice->perThread[tid];
                                slot.count += 1;
                                slot.sum += e.value;
                            }
                        }
                        const Timestamp newWm = wm->ack(b.seq, b.watermark);
                        auto drained = store->drainUpTo(newWm);
                        for (auto& d : drained)
                        {
                            for (auto& slot : d->perThread)
                            {
                                local ^= slot.count + slot.sum;
                            }
                        }
                    }
                    sink.fetch_xor(local, std::memory_order_relaxed);
                    doneBarrier.arrive_and_wait();
                }
            });
    }

    for (auto _ : state)
    {
        store = std::make_unique<Store>(cfg.windowSlide, numThreads);
        wm = std::make_unique<WatermarkTracker>();
        sink.store(0, std::memory_order_relaxed);
        startBarrier.arrive_and_wait();
        doneBarrier.arrive_and_wait();
        std::uint64_t observed = sink.load(std::memory_order_relaxed);
        benchmark::DoNotOptimize(observed);
        benchmark::ClobberMemory();
    }

    stop.store(true, std::memory_order_release);
    startBarrier.arrive_and_wait();
    for (auto& t : workers)
    {
        t.join();
    }

    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * stats.totalLookups));
    attachStatsCounters(state, stats);
    state.counters["threads"] = static_cast<double>(numThreads);
}

BENCHMARK_TEMPLATE(BM_SliceStore_Concurrent, LockingHashMapSliceStore)
    ->Arg(1)
    ->Arg(2)
    ->Arg(4)
    ->UseRealTime()
    ->Unit(benchmark::kMillisecond);

}

BENCHMARK_MAIN();
