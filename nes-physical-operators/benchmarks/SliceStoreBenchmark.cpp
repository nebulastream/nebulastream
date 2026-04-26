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
#include <unordered_map>
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

struct WorkloadConfig
{
    std::uint64_t windowSize = 1000;     ///< Tumbling window size (timestamp units).
    std::size_t numBuffers = 4096;       ///< Total buffers per benchmark iteration.
    std::size_t tuplesPerBuffer = 256;   ///< Batch size.
    std::size_t outOfOrderSpan = 4;      ///< Reorder window over arriving buffers.
    std::uint64_t timestampStride = 50;  ///< Mean ts increment per buffer.
    std::uint64_t timestampJitter = 200; ///< Per-tuple jitter -> tuples land in nearby slices.
    std::uint64_t watermarkLag = 2000;   ///< wm = clock - lag; controls number of live slices.
    unsigned seed = 0xC0FFEEu;
};

/// Generate buffers in seq order with monotonic advisory watermarks, then
/// shuffle within a sliding window of `outOfOrderSpan + 1` to model out-of-order
/// arrival at the operator. All randomness is seeded for reproducibility.
std::vector<Buffer> generateWorkload(const WorkloadConfig& cfg)
{
    std::mt19937_64 rng(cfg.seed);
    std::uniform_int_distribution<std::uint64_t> jitter(0, cfg.timestampJitter);

    std::vector<Buffer> buffers;
    buffers.reserve(cfg.numBuffers);

    Timestamp clock = cfg.windowSize;
    for (std::size_t s = 0; s < cfg.numBuffers; ++s)
    {
        Buffer b;
        b.seq = s;
        b.tuples.reserve(cfg.tuplesPerBuffer);
        for (std::size_t t = 0; t < cfg.tuplesPerBuffer; ++t)
        {
            const Timestamp base = clock + (t * cfg.timestampStride) / std::max<std::size_t>(1, cfg.tuplesPerBuffer);
            const std::int64_t signedJitter = static_cast<std::int64_t>(jitter(rng)) - static_cast<std::int64_t>(cfg.timestampJitter / 2);
            const Timestamp ts = (signedJitter < 0 && static_cast<Timestamp>(-signedJitter) > base) ? 0 : base + signedJitter;
            const Timestamp sliceEnd = ((ts / cfg.windowSize) + 1) * cfg.windowSize;
            b.tuples.push_back({sliceEnd, ts});
        }
        b.watermark = clock > cfg.watermarkLag ? clock - cfg.watermarkLag : 0;
        clock += cfg.timestampStride;
        buffers.push_back(std::move(b));
    }

    if (cfg.outOfOrderSpan > 0)
    {
        for (std::size_t i = 0; i + 1 < buffers.size(); ++i)
        {
            const std::size_t hi = std::min(i + cfg.outOfOrderSpan + 1, buffers.size());
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

/// Single-threaded baseline. Out-of-order span varied via state.range(0).
template <class Store>
void BM_SliceStore_Single(benchmark::State& state)
{
    WorkloadConfig cfg;
    cfg.outOfOrderSpan = static_cast<std::size_t>(state.range(0));
    const auto buffers = generateWorkload(cfg);
    std::size_t totalTuples = 0;
    for (const auto& b : buffers)
    {
        totalTuples += b.tuples.size();
    }

    for (auto _ : state)
    {
        Store store(cfg.windowSize, /*numThreads=*/1);
        std::uint64_t sink = runWorkload(store, buffers, /*tid=*/0);
        benchmark::DoNotOptimize(sink);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * totalTuples));
    state.counters["tuples/buf"] = static_cast<double>(cfg.tuplesPerBuffer);
    state.counters["buffers"] = static_cast<double>(cfg.numBuffers);
}

/// Sweep window size to see how the live-slice count interacts with the layout.
/// state.range(0) = window size; outOfOrderSpan held at 4.
template <class Store>
void BM_SliceStore_WindowSize(benchmark::State& state)
{
    WorkloadConfig cfg;
    cfg.windowSize = static_cast<std::uint64_t>(state.range(0));
    cfg.watermarkLag = cfg.windowSize * 2;
    const auto buffers = generateWorkload(cfg);
    std::size_t totalTuples = 0;
    for (const auto& b : buffers)
    {
        totalTuples += b.tuples.size();
    }

    for (auto _ : state)
    {
        Store store(cfg.windowSize, /*numThreads=*/1);
        std::uint64_t sink = runWorkload(store, buffers, /*tid=*/0);
        benchmark::DoNotOptimize(sink);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(static_cast<std::int64_t>(state.iterations() * totalTuples));
}

BENCHMARK_TEMPLATE(BM_SliceStore_Single, LockingHashMapSliceStore)
    ->Arg(0)
    ->Arg(2)
    ->Arg(8)
    ->Arg(32)
    ->Unit(benchmark::kMillisecond);

BENCHMARK_TEMPLATE(BM_SliceStore_WindowSize, LockingHashMapSliceStore)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000)
    ->Unit(benchmark::kMillisecond);

}

BENCHMARK_MAIN();
