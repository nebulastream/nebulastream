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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <random>
#include <set>
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

struct Tuple
{
    Timestamp sliceEnd; ///< Pre-assigned by a SliceAssigner-equivalent.
    std::uint64_t value;
};

/// A buffer as it arrives at the operator: monotonic seq, advisory watermark,
/// and a batch of tuples. Buffers may arrive out of order; the operator's
/// effective watermark only advances over the contiguous prefix of seen seqs.
struct Buffer
{
    SequenceNumber seq;
    Timestamp watermark;
    std::vector<Tuple> tuples;
};

/// Workload configuration. Defaults give a moderately out-of-order stream:
/// 10 % of tuples are late by up to 5 windows, watermark trails 8 windows
/// behind, sliding window of size 4 * slide -> tens of live slices at a time.
struct WorkloadConfig
{
    std::uint64_t windowSlide = 1000;        ///< Slice width (== window size for tumbling).
    std::uint64_t windowSize = 4000;         ///< Window size; if > slide -> sliding window.
    std::size_t numBuffers = 4096;           ///< Total buffers per iteration.
    std::size_t tuplesPerBuffer = 256;       ///< Batch size.
    std::size_t bufferOutOfOrderSpan = 4;    ///< Reorder window over arriving buffers.
    std::uint64_t timestampStride = 200;     ///< Mean event-time advance per buffer.
    double lateTupleFraction = 0.10;         ///< Fraction of tuples emitted late.
    std::uint64_t maxTupleLateness = 5000;   ///< Max event-time lateness for late tuples.
    std::uint64_t watermarkLag = 8000;       ///< buffer.watermark = clock - lag.
    unsigned seed = 0xC0FFEEu;
};

/// Generate an out-of-order stream:
///   - Each buffer's `clock` advances monotonically by `timestampStride`.
///   - In-order tuples have event time near the buffer's clock.
///   - A configurable fraction of tuples are LATE: their event time is up to
///     `maxTupleLateness` behind the clock, so they land in older slices that
///     the watermark hasn't passed yet (bounded by `watermarkLag`).
///   - Tuples within a buffer are then shuffled so the late ones aren't all at
///     the head; the operator sees lookups jumping backwards in slice end.
///   - Buffers themselves are shuffled within a window of `bufferOutOfOrderSpan`
///     so their seq numbers arrive non-monotonically (forces the WatermarkTracker
///     to hold pending advisories).
///   - Sliding window: each tuple is fanned out into `windowSize / windowSlide`
///     consecutive slice ends, matching how a build operator inserts a tuple
///     into every slice the tuple participates in.
std::vector<Buffer> generateWorkload(const WorkloadConfig& cfg)
{
    std::mt19937_64 rng(cfg.seed);
    std::uniform_real_distribution<double> coin(0.0, 1.0);
    std::uniform_int_distribution<std::uint64_t> latenessDist(0, cfg.maxTupleLateness);
    std::uniform_int_distribution<std::uint64_t> inOrderJitter(0, cfg.timestampStride);

    const std::uint64_t slicesPerWindow = std::max<std::uint64_t>(1, cfg.windowSize / cfg.windowSlide);

    std::vector<Buffer> buffers;
    buffers.reserve(cfg.numBuffers);

    Timestamp clock = cfg.windowSize;
    std::uint64_t valueCounter = 0;
    for (std::size_t s = 0; s < cfg.numBuffers; ++s)
    {
        Buffer b;
        b.seq = s;
        b.tuples.reserve(cfg.tuplesPerBuffer * slicesPerWindow);
        for (std::size_t t = 0; t < cfg.tuplesPerBuffer; ++t)
        {
            Timestamp eventTime;
            if (coin(rng) < cfg.lateTupleFraction)
            {
                const std::uint64_t lateness = latenessDist(rng);
                eventTime = clock > lateness ? clock - lateness : 0;
            }
            else
            {
                eventTime = clock + inOrderJitter(rng);
            }
            /// Fan out across all slices this tuple belongs to. For tumbling
            /// (slicesPerWindow == 1) this is a single insert.
            const Timestamp firstSliceEnd = ((eventTime / cfg.windowSlide) + 1) * cfg.windowSlide;
            for (std::uint64_t k = 0; k < slicesPerWindow; ++k)
            {
                b.tuples.push_back({firstSliceEnd + k * cfg.windowSlide, valueCounter++});
            }
        }
        std::shuffle(b.tuples.begin(), b.tuples.end(), rng);
        b.watermark = clock > cfg.watermarkLag ? clock - cfg.watermarkLag : 0;
        clock += cfg.timestampStride;
        buffers.push_back(std::move(b));
    }

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
/// semantics for a single origin).
class WatermarkTracker
{
public:
    Timestamp ack(SequenceNumber seq, Timestamp advisory)
    {
        pending.emplace(seq, advisory);
        for (auto it = pending.find(nextSeq); it != pending.end(); it = pending.find(nextSeq))
        {
            global = std::max(global, it->second);
            pending.erase(it);
            ++nextSeq;
        }
        return global;
    }

    [[nodiscard]] Timestamp current() const { return global; }

private:
    SequenceNumber nextSeq = 0;
    Timestamp global = 0;
    std::unordered_map<SequenceNumber, Timestamp> pending;
};

/// Inspect a generated workload: counts what the slice store will actually
/// see. Useful both for sanity checks during development and for emitting
/// counters alongside benchmark results.
struct WorkloadStats
{
    std::size_t totalTuples = 0;          ///< Sum of tuples across all buffers.
    std::size_t uniqueSliceEnds = 0;      ///< Number of distinct slice ends across the run.
    std::size_t peakLiveSlices = 0;       ///< Max concurrent live slices (after each buffer).
    std::size_t backwardLookups = 0;      ///< Lookups whose sliceEnd < the previous lookup's end.
    std::size_t outOfOrderBuffers = 0;    ///< Buffers whose seq arrives below the running max.
};

WorkloadStats analyzeWorkload(const std::vector<Buffer>& buffers)
{
    WorkloadStats stats;
    std::unordered_set<Timestamp> allEnds;
    std::set<Timestamp> live;
    WatermarkTracker wm;
    Timestamp prevSliceEnd = 0;
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

        for (const auto& t : b.tuples)
        {
            ++stats.totalTuples;
            if (t.sliceEnd < prevSliceEnd)
            {
                ++stats.backwardLookups;
            }
            prevSliceEnd = t.sliceEnd;
            allEnds.insert(t.sliceEnd);
            live.insert(t.sliceEnd);
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

/// One pass of the workload through a store. Touches the per-thread slot of
/// each returned slice so the build path is realistic.
template <typename Store>
std::uint64_t runWorkload(Store& store, const std::vector<Buffer>& buffers, WorkerThreadId tid)
{
    WatermarkTracker wm;
    std::uint64_t sink = 0;
    for (const auto& b : buffers)
    {
        for (const auto& t : b.tuples)
        {
            auto slice = store.getOrCreate(t.sliceEnd);
            auto& slot = slice->perThread[tid];
            slot.count += 1;
            slot.sum += t.value;
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
    state.counters["totalTuples"] = static_cast<double>(s.totalTuples);
    state.counters["uniqSlices"] = static_cast<double>(s.uniqueSliceEnds);
    state.counters["peakLive"] = static_cast<double>(s.peakLiveSlices);
    state.counters["backwardLookups"] = static_cast<double>(s.backwardLookups);
    state.counters["ooBuffers"] = static_cast<double>(s.outOfOrderBuffers);
}

/// Sweep the per-tuple lateness. state.range(0) = maxTupleLateness in timestamp
/// units. 0 means in-order tuples; larger values fan lookups across older slices
/// that haven't been drained yet.
template <class Store>
void BM_SliceStore_TupleLateness(benchmark::State& state)
{
    WorkloadConfig cfg;
    cfg.maxTupleLateness = static_cast<std::uint64_t>(state.range(0));
    /// Make sure the watermark trails at least the max lateness so late tuples
    /// have a slice to land in.
    cfg.watermarkLag = std::max<std::uint64_t>(cfg.watermarkLag, cfg.maxTupleLateness + 2 * cfg.windowSlide);
    const auto buffers = generateWorkload(cfg);
    const auto stats = analyzeWorkload(buffers);

    for (auto _ : state)
    {
        Store store(cfg.windowSlide, /*numThreads=*/1);
        std::uint64_t sink = runWorkload(store, buffers, /*tid=*/0);
        benchmark::DoNotOptimize(sink);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * stats.totalTuples));
    attachStatsCounters(state, stats);
}

/// Sweep how many slices are live at the same time, by varying watermarkLag.
/// state.range(0) = watermarkLag in timestamp units.
template <class Store>
void BM_SliceStore_LiveSlices(benchmark::State& state)
{
    WorkloadConfig cfg;
    cfg.watermarkLag = static_cast<std::uint64_t>(state.range(0));
    cfg.maxTupleLateness = std::min(cfg.maxTupleLateness, cfg.watermarkLag);
    const auto buffers = generateWorkload(cfg);
    const auto stats = analyzeWorkload(buffers);

    for (auto _ : state)
    {
        Store store(cfg.windowSlide, /*numThreads=*/1);
        std::uint64_t sink = runWorkload(store, buffers, /*tid=*/0);
        benchmark::DoNotOptimize(sink);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * stats.totalTuples));
    attachStatsCounters(state, stats);
}

/// Sweep the buffer-level out-of-order span: how far apart in seq number can
/// two adjacent arrivals be?
template <class Store>
void BM_SliceStore_BufferOOO(benchmark::State& state)
{
    WorkloadConfig cfg;
    cfg.bufferOutOfOrderSpan = static_cast<std::size_t>(state.range(0));
    const auto buffers = generateWorkload(cfg);
    const auto stats = analyzeWorkload(buffers);

    for (auto _ : state)
    {
        Store store(cfg.windowSlide, /*numThreads=*/1);
        std::uint64_t sink = runWorkload(store, buffers, /*tid=*/0);
        benchmark::DoNotOptimize(sink);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * stats.totalTuples));
    attachStatsCounters(state, stats);
}

BENCHMARK_TEMPLATE(BM_SliceStore_TupleLateness, LockingHashMapSliceStore)
    ->Arg(0)
    ->Arg(1000)
    ->Arg(5000)
    ->Arg(20000)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_TEMPLATE(BM_SliceStore_LiveSlices, LockingHashMapSliceStore)
    ->Arg(2000)
    ->Arg(8000)
    ->Arg(32000)
    ->Arg(128000)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_TEMPLATE(BM_SliceStore_BufferOOO, LockingHashMapSliceStore)
    ->Arg(0)
    ->Arg(2)
    ->Arg(8)
    ->Arg(32)
    ->Unit(benchmark::kMillisecond);

}

BENCHMARK_MAIN();
