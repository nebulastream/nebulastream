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

/// Formatter LATENCY-vs-#streams benchmark (standalone; isolated from the gtest suite).
///
/// Question: how does per-buffer parse latency behave as the number of concurrent text streams N grows on a
/// fixed core budget, for four parallelisation models that all run the SAME compiled Scan(CSV)->Emit stage
/// (identical per-byte index + parse work) and differ only in thread structure?
///
///   dpp       one task per 128 KiB buffer, any of W pooled workers, buffer boundaries via the lock-free
///             SequenceShredder. No per-stream thread, no join.                                  (ours)
///   gshared   the buffer is split into k sub-chunks; the first pooled worker to take it forks the other
///             chunks into the shared queue and does one itself; a per-buffer join counter completes the
///             buffer. Same W workers, no per-stream thread. Isolates fork/join cost from oversubscription.
///   gprivate  each stream owns a GANG of k dedicated threads (leader + k-1 members): leader takes the
///             buffer, forks (generation counter), every member indexes+parses its sub-chunk, last arriver
///             completes the buffer (join). SotA speculative/enumerative parallel parsing dropped into a
///             thread-per-source SPE; k=1 = plain thread-per-stream parsing. N*k threads on the cpuset.
///   chunker   each stream owns ONE thread that CUTS the buffer into record-aligned chunks and hands the
///             self-contained chunks to the W pooled workers, which parse them with per-worker SEQUENTIAL
///             stages: no SequenceShredder, no join, no speculation. N cutter threads + W workers. The cutter
///             finds the record boundaries with ONE sequential pass over every byte of the buffer (newline
///             detection only, no field offsets -- NES_LAT_CUT=scan, default): the chunker's inherent work.
///             NES_LAT_CUT=quote makes that pass QUOTE-AWARE (carries the quote parity through the same pass and
///             ignores newlines inside quoted fields -- the work a general-CSV cutter must do). NES_LAT_CUT=index
///             runs the full SIMDCSV index pass instead (fields too), NES_LAT_CUT=memchr only k local newline
///             searches; both kept for reference.
///   rr        ROUND-ROBIN PINNING: W pooled workers, each with its OWN queue; stream s is pinned to worker s mod W
///             for the whole run and only that worker ever parses it, in order, through a per-stream SEQUENTIAL
///             stage (no SequenceShredder, no shared queue, no join, no wake of another thread): the
///             shared-nothing / partitioned SPE model. Its cost is imbalance: a worker whose streams need more
///             than one core's worth delays every stream pinned to it while other workers idle (visible at
///             N mod W != 0 even with identical streams; a per-stream cost skew makes it severe).
///
/// Load model: CLOSED LOOP, full load. Every stream has exactly ONE buffer in flight (D=1): the moment buffer
/// s completes, buffer s+1 becomes available (enq stamp = done of s) and is offered to the arm's front-end
/// (queue / gang leader / chunker). Latency = done_s - enq_s, so it INCLUDES the wake-up of any per-stream
/// thread and the queue wait, and every tuple that ends in buffer s is emitted by done_s. All N streams are
/// always active, so the engine is always at capacity: past saturation residence = backlog / throughput for
/// every arm (Little's law); the signal is the knee position (N = cores vs cores/k), the throughput lost to
/// per-buffer switching, and the tail (p99/p50).
///
/// Data: each stream owns a private RING of R raw buffers (R x 128 KiB, its own memory copy of the file's
/// first R buffers, last one trimmed to its last newline so the cycle wrap is a clean record boundary) and
/// cycles through it with monotone sequence numbers. Sub-chunk / cut views are zero-copy TupleBuffers wrapped
/// over the stream's memory ONCE at setup (BufferManager::wrapExternalMemory retains a segment per call).
///
/// Build + run (host mode, release build):
///   NES_BUILD_DIR=cmake-build-release .claude/skills/nes-build/in-docker.sh \
///       cmake --build {build} --target formatter-latency-benchmark
///   taskset -c 0-23 env NES_LAT_ARM=gprivate NES_LAT_K=4 NES_LAT_STREAMS=1,6,24,128 \
///       {build}/nes-input-formatters/tests/Benchmark/formatter-latency-benchmark \
///       [file.csv] [rawBufKiB=128] [cols=5] [projectField=f1] [fnattrYaml=-] [uint64Parser=FastUINT64]
///
/// Env knobs (all optional):
///   NES_LAT_ARM        dpp | gshared | gprivate | chunker | rr       (default dpp)
///   NES_LAT_K          sub-chunks per buffer for gshared/gprivate/chunker (default 4; dpp ignores)
///   NES_LAT_STREAMS    comma list of N                               (default 1,4,16,64)
///   NES_LAT_WORKERS    pooled workers W for dpp/gshared/chunker      (default 23)
///   NES_LAT_PIN        1 = pin worker w to the w-th CPU of the process affinity mask (default 1)
///   NES_LAT_RING       buffers per stream ring R                    (default 64)
///   NES_LAT_D          buffers in flight per stream (closed-loop window; D>1 = the stream has a backlog, so
///                      a per-stream thread never idles between buffers)        (default 1)
///   NES_LAT_GENS       view generations per ring slot (control-block reuse distance = ring*gens) (default 64)
///   NES_LAT_WS_MIB     floor on the total data working set N*ring*128KiB (default 0 = off): the per-stream ring grows
///                      (wrapping the file) so every N sits in the SAME memory regime. Without it the Threadripper
///                      shows a throughput trough (-20 %, DPP and chunker alike) where the working set falls into
///                      ~128 MiB-1 GiB; 2048 keeps every N in the streaming regime. Generations scale down to keep
///                      the control-block reuse distance ring*gens of the base configuration.
///   NES_LAT_CUT        chunker cut method (default scan):
///                        scan   = ONE sequential pass over every byte, record boundaries only (the chunker's inherent work)
///                        quote  = the same pass + quote parity (prefix-XOR of the quote mask, carried across buffers);
///                                 newlines inside quotes are not boundaries (general CSV). CSV arm label: chunker_quote
///                        index  = the full SIMDCSV index pass (fields too; the general-CSV / quote-aware analogue)
///                        memchr = k local newline searches (~200 bytes touched; the quote-free shortcut)
///   NES_LAT_FMT_KIB    formatted (output) buffer KiB (default = raw buffer size; 32 suffices for 1 projected col)
///   NES_LAT_WARMUP_S / NES_LAT_WINDOW_S   discarded warm-up / measured window seconds (default 2 / 10)
///   NES_LAT_REPS       repetitions per N                             (default 3)
///   NES_LAT_SPIN_US    spin budget before futex wait for gang/chunker threads (default 50; 0 = block)
///   NES_LAT_CAP        global admission cap: max buffers in flight across all queries, FIFO admission
///                      (the engine: 1000-task admission queue); 0 = none                 (default 0)
///   NES_LAT_OUT        CSV file to append one row per (N, rep)       (default: none)
///   NES_LAT_HEAVY      <fraction>,<mult>: per-query COST SKEW (default off). One stream per block of round(1/<fraction>)
///                      consecutive ids is HEAVY, at a hash-chosen position in the block (the same set in every arm, rep
///                      and N; ~<fraction>*N at every N; not aligned with id mod W). Every stage call on one of its
///                      buffers is followed by a busy burn of (<mult>-1)x that
///                      call's own wall time on the same thread = a query whose work per byte is <mult>x the parse
///                      (the parse runs once; tuples counted once; no shredder/carry replay). Adds per-class columns:
///                      light_/heavy_ p50/p99 and buffers (throughput share), and light_strm_p99_max (the worst LIGHT
///                      stream's p99 = the innocent bystander pinned next to heavy streams).
///   NES_LAT_BURST      <active_ms>,<idle_ms>: BURSTY streams (default off). Every stream alternates an ACTIVE phase
///                      (closed loop as usual) and an IDLE phase (no successor buffers; its window drains) with period
///                      active+idle and a per-stream pseudo-random phase offset (same in every arm). At the start of an
///                      active phase the stream offers a backlog of D buffers at once (one timer thread; the availability
///                      stamp is the actual offer instant, so timer lateness is not charged to the buffer). Under
///                      NES_LAT_CAP an idle stream hands its admission slots to the longest-waiting query (or back to the
///                      pool) and a waking stream takes free slots or queues FIFO, like the seed. idle must be < 3000 ms
///                      (the 3 s no-completion stall detector). Columns burst_active_ms / burst_idle_ms.
///   NES_INDEX_ALLOW_COMMAS  forwarded to the indexer's allow_commas_in_strings (default: unset -> true)

#include <algorithm>
#include <atomic>
#include <chrono>
#include <climits>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#if defined(__x86_64__)
    #include <immintrin.h>
#elif defined(__aarch64__)
    #include <arm_neon.h>
#endif
#include <filesystem>
#include <fstream>
#include <functional>
#include <latch>
#include <memory>
#include <mutex>
#include <numeric>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <csignal>
#include <execinfo.h>
#include <pthread.h>
#include <sched.h>
#include <semaphore.h>
#include <unistd.h>
#include <linux/futex.h>
#include <sys/resource.h>
#include <sys/syscall.h>

#include <folly/MPMCQueue.h>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

#include <CSVInputFormatIndexer.hpp>
#include <FieldBand.hpp>
#include <InputFormatterDescriptor.hpp>
#include <InputFormatterTestUtil.hpp>
#include <InputFormatterValidationProvider.hpp>
#include <InvokeConfiguration.hpp>
#include <RawTupleBuffer.hpp>
#include <SIMDCSVInputFormatIndexer.hpp>
#include <TestTaskQueue.hpp>

namespace
{
using namespace NES;
using InputFormatterTestUtil::TestDataTypes;

std::atomic<uint64_t> g_repeats{0};
std::atomic<uint64_t> g_completions{0}; /// watchdog: total completions so far
std::atomic<uint64_t> g_slotWaits{0}; /// offers that had to wait for the slot's previous buffer (long queue lag)
std::atomic<uint64_t> g_slotWaitMaxNs{0};
std::atomic<uint64_t> g_timerWakes{0}; /// NES_LAT_BURST: burst starts fired by the timer thread
std::atomic<uint64_t> g_timerLateMaxNs{0}; /// NES_LAT_BURST: worst lateness of a burst start (diagnostics)

void segvHandler(int sig)
{
    void* frames[64];
    const int n = backtrace(frames, 64);
    std::fprintf(stderr, "\n*** signal %d, backtrace (%d frames):\n", sig, n);
    backtrace_symbols_fd(frames, n, 2);
    std::_Exit(139);
}

/// ---------------------------------------------------------------------------------------------------------
/// small helpers
/// ---------------------------------------------------------------------------------------------------------
std::string envStr(const char* name, const std::string& dflt)
{
    const char* const v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::string(v) : dflt;
}

uint64_t envU64(const char* name, const uint64_t dflt)
{
    const char* const v = std::getenv(name);
    return (v != nullptr && *v != '\0') ? std::strtoull(v, nullptr, 10) : dflt;
}

std::vector<size_t> parseList(const std::string& s)
{
    std::vector<size_t> out;
    size_t pos = 0;
    while (pos <= s.size())
    {
        const auto comma = s.find(',', pos);
        const auto tok = s.substr(pos, comma == std::string::npos ? std::string::npos : comma - pos);
        if (not tok.empty())
        {
            out.push_back(std::strtoull(tok.c_str(), nullptr, 10));
        }
        if (comma == std::string::npos)
        {
            break;
        }
        pos = comma + 1;
    }
    return out;
}

inline uint64_t nowNs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}

inline void cpuRelax()
{
#if defined(__x86_64__) || defined(__i386__)
    __builtin_ia32_pause();
#else
    std::this_thread::yield();
#endif
}

/// splitmix64: a fixed, machine-independent pseudo-random draw per stream id (heavy set, burst phase offset)
inline uint64_t splitmix64(uint64_t x)
{
    x += 0x9E3779B97F4A7C15ULL;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
    return x ^ (x >> 31);
}

/// NES_LAT_HEAVY: STRATIFIED draw -- in every block of B = round(1/frac) consecutive stream ids exactly one is heavy, at a
/// hash-chosen position within the block. So every N holds ~frac*N heavy streams (a plain per-id coin flip left N <= 8
/// with none), the set is the same in every arm, rep and N (a prefix property), and the position within the block is
/// pseudo-random, i.e. not aligned with id mod W in any systematic way.
inline size_t heavyBlock(const double frac)
{
    return (frac > 0.0) ? std::max<size_t>(1, static_cast<size_t>(std::llround(1.0 / frac))) : 0;
}

inline bool heavyPick(const size_t id, const double frac)
{
    const size_t B = heavyBlock(frac);
    return B != 0 && (id % B) == (splitmix64(0x4845415659ULL ^ (id / B)) % B);
}

/// NES_LAT_HEAVY: after a heavy stream's stage call [t0, t1] burn (mult-1)x its wall time of CPU on the same thread --
/// a query whose work per byte is mult x the parse; the parse itself ran once (no shredder / carry replay)
inline void burnExtra(const uint64_t t0, const uint64_t t1, const uint32_t mult)
{
    if (mult <= 1)
    {
        return;
    }
    const uint64_t end = t1 + (t1 - t0) * (mult - 1);
    while (nowNs() < end)
    {
        cpuRelax();
    }
}

/// Prefix XOR of a 64-bit mask: bit i of the result = parity of bits 0..i (simdjson's find_quote_mask). Carry-less
/// multiply by all-ones where available (x86 PCLMUL), otherwise the 6-step shift ladder (aarch64 without PMULL).
inline uint64_t prefixXor(uint64_t m)
{
#if defined(__PCLMUL__)
    const __m128i r = _mm_clmulepi64_si128(_mm_set_epi64x(0, static_cast<long long>(m)), _mm_set1_epi8(static_cast<char>(0xFF)), 0);
    return static_cast<uint64_t>(_mm_cvtsi128_si64(r));
#else
    m ^= m << 1;
    m ^= m << 2;
    m ^= m << 4;
    m ^= m << 8;
    m ^= m << 16;
    m ^= m << 32;
    return m;
#endif
}

#if defined(__aarch64__)
/// 64 bytes -> 64-bit byte mask (bit i set <=> lane i of the compare result is all-ones); simdjson's arm64 to_bitmask.
inline uint64_t neonBitmask64(uint8x16_t c0, uint8x16_t c1, uint8x16_t c2, uint8x16_t c3)
{
    const uint8x16_t bits = {0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80};
    uint8x16_t s0 = vpaddq_u8(vandq_u8(c0, bits), vandq_u8(c1, bits));
    const uint8x16_t s1 = vpaddq_u8(vandq_u8(c2, bits), vandq_u8(c3, bits));
    s0 = vpaddq_u8(s0, s1);
    s0 = vpaddq_u8(s0, s0);
    return vgetq_lane_u64(vreinterpretq_u64_u8(s0), 0);
}
#endif

/// Quote-aware record-boundary scan (NES_LAT_CUT=quote): ONE pass over every byte producing, per 64-byte block, the
/// quote mask and the newline mask; the in-quote mask is the prefix XOR of the quote mask (carried across blocks AND
/// across buffers: a field opened in buffer b may close in b+1), and a newline is a record boundary only outside
/// quotes. This is exactly what a general-CSV cutter must do before it can hand out self-contained chunks (the
/// stage-1 quote pass of simdjson/simdcsv); no field offsets. `inQuoteCarry` is 0 or ~0.
inline void scanNewlinesQuoted(const char* data, const size_t bytes, uint64_t& inQuoteCarry, std::vector<uint32_t>& out)
{
    out.clear();
    size_t i = 0;
    uint64_t carry = inQuoteCarry;
    for (; i + 64 <= bytes; i += 64)
    {
        uint64_t q, nl;
#if defined(__AVX2__)
        const __m256i qv = _mm256_set1_epi8('"');
        const __m256i nv = _mm256_set1_epi8('\n');
        const __m256i lo = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        const __m256i hi = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i + 32));
        q = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, qv)))
            | (static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, qv)))) << 32);
        nl = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, nv)))
            | (static_cast<uint64_t>(static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, nv)))) << 32);
#elif defined(__aarch64__)
        const uint8x16_t qv = vdupq_n_u8(static_cast<uint8_t>('"'));
        const uint8x16_t nv = vdupq_n_u8(static_cast<uint8_t>('\n'));
        const auto* p = reinterpret_cast<const uint8_t*>(data + i);
        const uint8x16_t b0 = vld1q_u8(p), b1 = vld1q_u8(p + 16), b2 = vld1q_u8(p + 32), b3 = vld1q_u8(p + 48);
        q = neonBitmask64(vceqq_u8(b0, qv), vceqq_u8(b1, qv), vceqq_u8(b2, qv), vceqq_u8(b3, qv));
        nl = neonBitmask64(vceqq_u8(b0, nv), vceqq_u8(b1, nv), vceqq_u8(b2, nv), vceqq_u8(b3, nv));
#else
        q = nl = 0;
        for (size_t j = 0; j < 64; ++j)
        {
            q |= static_cast<uint64_t>(data[i + j] == '"') << j;
            nl |= static_cast<uint64_t>(data[i + j] == '\n') << j;
        }
#endif
        const uint64_t inQuote = prefixXor(q) ^ carry;
        carry = static_cast<uint64_t>(static_cast<int64_t>(inQuote) >> 63);
        uint64_t m = nl & ~inQuote;
        while (m != 0)
        {
            out.push_back(static_cast<uint32_t>(i + static_cast<size_t>(__builtin_ctzll(m))));
            m &= m - 1;
        }
    }
    bool in = carry != 0;
    for (; i < bytes; ++i)
    {
        if (data[i] == '"')
        {
            in = !in;
        }
        else if (data[i] == '\n' && !in)
        {
            out.push_back(static_cast<uint32_t>(i));
        }
    }
    inQuoteCarry = in ? ~0ULL : 0ULL;
}

/// Sequential record-boundary scan: ONE pass over every byte, newline detection only (no field offsets), positions
/// appended in order. SIMD compare + movemask (AVX2 on x86-64, NEON on aarch64), scalar tail. This is the chunker's
/// inherent work under the quote-free assumption: consume the byte stream once and find the record boundaries;
/// with quotes it would additionally carry the quote parity through the same pass.
inline void scanNewlines(const char* data, const size_t bytes, std::vector<uint32_t>& out)
{
    out.clear();
    size_t i = 0;
#if defined(__AVX2__)
    const __m256i nl = _mm256_set1_epi8('\n');
    for (; i + 32 <= bytes; i += 32)
    {
        const __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        auto m = static_cast<uint32_t>(_mm256_movemask_epi8(_mm256_cmpeq_epi8(v, nl)));
        while (m != 0)
        {
            out.push_back(static_cast<uint32_t>(i + static_cast<size_t>(__builtin_ctz(m))));
            m &= m - 1;
        }
    }
#elif defined(__aarch64__)
    const uint8x16_t nl = vdupq_n_u8(static_cast<uint8_t>('\n'));
    for (; i + 16 <= bytes; i += 16)
    {
        const uint8x16_t eq = vceqq_u8(vld1q_u8(reinterpret_cast<const uint8_t*>(data + i)), nl);
        /// narrow 16 x 8-bit lanes to 16 x 4-bit nibbles = one 64-bit mask (nibble j set <=> lane j matched)
        uint64_t m = vget_lane_u64(vreinterpret_u64_u8(vshrn_n_u16(vreinterpretq_u16_u8(eq), 4)), 0);
        while (m != 0)
        {
            const int lane = __builtin_ctzll(m) >> 2;
            out.push_back(static_cast<uint32_t>(i + static_cast<size_t>(lane)));
            m &= ~(0xFULL << (lane * 4));
        }
    }
#endif
    for (; i < bytes; ++i)
    {
        if (data[i] == '\n')
        {
            out.push_back(static_cast<uint32_t>(i));
        }
    }
}

/// Raw futex wait/wake on a 32-bit atomic: a parked thread costs NO CPU (libc++'s std::atomic::wait /
/// counting_semaphore poll with timed sleeps and burned ~3 idle cores in the first smoke run).
inline void futexWait(std::atomic<uint32_t>& a, const uint32_t expected)
{
    syscall(SYS_futex, reinterpret_cast<uint32_t*>(&a), FUTEX_WAIT_PRIVATE, expected, nullptr, nullptr, 0);
}

inline void futexWake(std::atomic<uint32_t>& a, const int n)
{
    syscall(SYS_futex, reinterpret_cast<uint32_t*>(&a), FUTEX_WAKE_PRIVATE, n, nullptr, nullptr, 0);
}

/// Spin-then-block wait until `a != old` (OpenMP/TBB-style adaptive wait). Returns the new value.
uint32_t waitChange(std::atomic<uint32_t>& a, const uint32_t old, const uint64_t spinNs)
{
    const uint64_t start = nowNs();
    uint32_t v = a.load(std::memory_order_acquire);
    while (v == old)
    {
        if (spinNs == 0 || (nowNs() - start) > spinNs)
        {
            futexWait(a, old);
        }
        else
        {
            cpuRelax();
        }
        v = a.load(std::memory_order_acquire);
    }
    return v;
}

/// Counting semaphore on a POSIX sem_t (glibc: pure futex, no polling) with the same spin-then-block policy.
struct Sem
{
    sem_t s{};

    Sem() { sem_init(&s, 0, 0); }

    ~Sem() { sem_destroy(&s); }

    void release() { sem_post(&s); }

    void acquire(const uint64_t spinNs)
    {
        const uint64_t start = nowNs();
        while (sem_trywait(&s) != 0)
        {
            if (spinNs == 0 || (nowNs() - start) > spinNs)
            {
                while (sem_wait(&s) != 0)
                {
                }
                return;
            }
            cpuRelax();
        }
    }
};

/// CPUs in this process's affinity mask (respects `taskset`), ascending.
std::vector<int> allowedCpus()
{
    cpu_set_t set;
    CPU_ZERO(&set);
    std::vector<int> cpus;
    if (sched_getaffinity(0, sizeof(set), &set) == 0)
    {
        for (int c = 0; c < CPU_SETSIZE; ++c)
        {
            if (CPU_ISSET(c, &set))
            {
                cpus.push_back(c);
            }
        }
    }
    return cpus;
}

void pinTo(const int cpu)
{
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
}

/// ---------------------------------------------------------------------------------------------------------
/// pec that DROPS emitted buffers (output recycles immediately) but COUNTS emitted tuples (correctness gate)
/// ---------------------------------------------------------------------------------------------------------
struct CountingPec final : PipelineExecutionContext
{
    std::shared_ptr<AbstractBufferProvider> bm;
    WorkerThreadId threadId;
    PipelineId pipelineId;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
    std::vector<std::unique_ptr<TupleBuffer>> pinnedBuffers;
    uint64_t tuples{0};
    bool repeatRequested{false};

    CountingPec(std::shared_ptr<AbstractBufferProvider> bufferManager, const WorkerThreadId threadId, const PipelineId pipelineId)
        : bm(std::move(bufferManager)), threadId(threadId), pipelineId(pipelineId)
    {
    }

    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
    {
        tuples += buffer.getNumberOfTuples();
        return true;
    }

    TupleBuffer allocateTupleBuffer() override { return bm->getBufferBlocking(); }

    TupleBuffer& pinBuffer(TupleBuffer&& tupleBuffer) override
    {
        pinnedBuffers.emplace_back(std::make_unique<TupleBuffer>(tupleBuffer));
        return *pinnedBuffers.back();
    }

    [[nodiscard]] WorkerThreadId getWorkerThreadId() const override { return threadId; }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 0; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bm; }

    [[nodiscard]] PipelineId getPipelineId() const override { return pipelineId; }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return operatorHandlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers) override
    {
        operatorHandlers = handlers;
    }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { repeatRequested = true; }
};

/// Run the stage on one raw buffer. Returns emitted tuples, or nullopt when the formatter asked for a REPEAT
/// (the buffer's sequence number is outside the SequenceShredder's current window): the caller must re-queue
/// the task at the TAIL and move on, as the engine does -- retrying in place would let every worker spin on an
/// out-of-range buffer while the buffers that advance the window sit behind them in the queue (livelock).
std::optional<uint64_t> runStageOnce(
    ExecutablePipelineStage* eps,
    const TupleBuffer& buffer,
    const std::shared_ptr<BufferManager>& pool,
    const uint64_t threadId,
    const uint32_t mult = 1)
{
    CountingPec pec(pool, WorkerThreadId(threadId), PipelineId(0));
    const uint64_t t0 = (mult > 1) ? nowNs() : 0;
    eps->execute(buffer, pec);
    if (mult > 1 && not pec.repeatRequested)
    {
        burnExtra(t0, nowNs(), mult); /// NES_LAT_HEAVY: the stream's extra per-byte work
    }
    if (pec.repeatRequested)
    {
        const uint64_t n = g_repeats.fetch_add(1, std::memory_order_relaxed);
        if (n < 8)
        {
            std::fprintf(
                stderr,
                "*** repeat #%lu: seq=%lu bytes=%lu thread=%lu\n",
                static_cast<unsigned long>(n),
                static_cast<unsigned long>(buffer.getSequenceNumber().getRawValue()),
                static_cast<unsigned long>(buffer.getNumberOfTuples()),
                static_cast<unsigned long>(threadId));
        }
        return std::nullopt;
    }
    return pec.tuples;
}

/// Gang variant: the k chunks of ONE buffer are always inside the window, so a repeat cannot occur; retry in
/// place after a yield (kept for safety).
uint64_t runStage(
    ExecutablePipelineStage* eps,
    const TupleBuffer& buffer,
    const std::shared_ptr<BufferManager>& pool,
    const uint64_t threadId,
    const uint32_t mult = 1)
{
    while (true)
    {
        if (const auto n = runStageOnce(eps, buffer, pool, threadId, mult))
        {
            return *n;
        }
        std::this_thread::yield();
    }
}

/// ---------------------------------------------------------------------------------------------------------
/// per-stream data + state
/// ---------------------------------------------------------------------------------------------------------
/// Views over a data slot. The DATA of a slot never changes, but its TupleBuffer control blocks carry mutable
/// metadata (sequence number) that the shredder reads while a buffer is in flight -- so a control block must
/// not be reused while its previous buffer may still be referenced. Each slot therefore owns VIEW_GENERATIONS
/// independent sets of views; buffer b uses generation (b / R) % VIEW_GENERATIONS of slot b % R, i.e. a control
/// block is reused only every R * VIEW_GENERATIONS buffers (4096 with R=64). `slotBusy` aborts if that is ever
/// still too soon.
size_t VIEW_GENERATIONS = 64; /// NES_LAT_GENS: 64 for D up to 64; 8 suffices for D=1 (reuse distance R*gens)

struct SlotViews
{
    TupleBuffer whole; /// [0, bytes) of the slot
    std::vector<TupleBuffer> gangChunks; /// fixed sub-ranges [c*B/k, min((c+1)*B/k, bytes)), non-empty only
    std::vector<TupleBuffer> cutChunks; /// chunker: record-aligned [cut_{c-1}+1 .. cut_c], each ends with '\n'
};

struct Slot
{
    std::vector<SlotViews> gen; /// VIEW_GENERATIONS view sets
    std::vector<std::pair<size_t, size_t>> cutRanges; /// (start, len) of cutChunks, for the runtime cross-check
    size_t bytes{0};
    size_t headEnd{0}; /// index of the first '\n' (head = [0, headEnd])
    size_t tailStart{0}; /// index after the last '\n' (tail = [tailStart, bytes))
};

constexpr uint32_t PENDING_TERMINATE = 0xFFFFFFFFU;

struct alignas(64) PaddedU32
{
    std::atomic<uint32_t> v{0};
};

struct alignas(64) PaddedU64
{
    std::atomic<uint64_t> v{0};
};

/// Per-thread statistics (no shared writes): merged after the run.
struct ThreadStats
{
    std::vector<std::pair<uint32_t, uint64_t>> samples; /// (stream, latency ns) inside the window
    uint64_t bytes{0};
    uint64_t buffers{0};
    uint64_t tuples{0};
    /// diagnostics: the task this worker is executing (0 = idle); read racily by the state dump
    uint64_t curStart{0};
    uint32_t curStream{0};
    uint32_t curSlot{0};
    uint8_t curKind{0};
};

struct Stream
{
    size_t id{0};
    uint8_t* mem{nullptr};
    size_t memBytes{0};
    std::vector<Slot> ring;
    std::shared_ptr<CompiledExecutablePipelineStage> stage; /// dpp / gshared / gprivate: own shredder + seq space
    std::shared_ptr<BufferManager> stagePool; /// chunker: staging buffers for the boundary row
    std::vector<char> carry; /// chunker: tail bytes of the previous buffer
    std::unique_ptr<SIMDCSVInputFormatIndexer> cutIndexer; /// chunker (index mode): the real structural scan
    std::unique_ptr<CSVMetaData> cutMeta;
    FieldBand cutBand;
    std::vector<uint32_t> cutNewlines; /// chunker (scan mode): record boundaries of the buffer being cut
    uint64_t cutQuoteCarry{0}; /// chunker (quote mode): quote parity carried into the next buffer (0 or ~0)
    size_t cutFields{0};

    /// sequence bookkeeping: buffer b (0-based, monotone per stream) lives in slot b % R and owns the chunk
    /// sequence numbers [seqBase(b), seqBase(b) + chunks(slot)) with seqBase(b) = 1 + (b / R) * chunksPerRing +
    /// prefixChunks[slot] -- deterministic, so concurrent offers (D > 1) never interleave two buffers' chunks.
    std::vector<uint64_t> prefixChunks;
    uint64_t chunksPerRing{0};
    alignas(64) std::atomic<uint64_t> nextBuf{0}; /// offer side (completers, possibly concurrent)
    size_t takeIdx{0}; /// take side (single leader / cutter thread)
    size_t curSlot{0}; /// gprivate: slot of the buffer being forked (leader writes, members read after gen)
    uint32_t curChunks{0};

    /// Per-buffer bookkeeping keyed by the VIRTUAL slot v = b % (R * VIEW_GENERATIONS): data slot v % R, view
    /// generation v / R. Reordering in flight can exceed the worker count by far (a buffer's chunks queue behind
    /// every task forked meanwhile), so nothing may be keyed by the data slot alone.
    std::vector<uint64_t> enqNs; /// per vslot: the instant the buffer became available
    std::vector<uint64_t> seqBase; /// per vslot: first chunk sequence number of the in-flight buffer
    std::vector<PaddedU32> remaining; /// per vslot: outstanding chunks of the in-flight buffer
    std::vector<PaddedU64> slotTuples; /// per vslot: tuples emitted for the in-flight buffer
    std::vector<PaddedU32> slotBusy; /// per vslot: 1 while its buffer is in flight (reuse-while-busy detector)
    std::vector<PaddedU64> ready; /// per vslot: buffer index + 1 once offer() finished its bookkeeping (taker waits on it)

    [[nodiscard]] size_t vslots() const { return ring.size() * VIEW_GENERATIONS; }

    [[nodiscard]] SlotViews& views(const size_t v) { return ring[v % ring.size()].gen[v / ring.size()]; }

    [[nodiscard]] size_t bytesOf(const size_t v) const { return ring[v % ring.size()].bytes; }

    uint32_t costMult{1}; /// NES_LAT_HEAVY: > 1 = heavy stream (burn (costMult-1)x the stage time after every stage call)
    uint64_t phaseOffNs{0}; /// NES_LAT_BURST: this stream's phase offset within the period
    alignas(64) std::atomic<uint32_t> queued{0}; /// entries of this stream in the admission FIFO (`waiting`)
    std::atomic<uint32_t> asleep{0}; /// NES_LAT_BURST: 1 from the first idle completion until the timer fires the burst
    alignas(64) std::atomic<uint32_t> pending{0}; /// gprivate leader / chunker mailbox: offered-but-not-taken count
    alignas(64) std::atomic<uint32_t> inFlight{0};
    alignas(64) std::atomic<uint32_t> gen{0}; /// gprivate fork generation
    alignas(64) std::atomic<uint32_t> terminate{0};
};

enum class Kind : uint8_t
{
    WHOLE, /// dpp: whole buffer through the stream's stage
    FORK, /// gshared: fork the slot's chunks, process chunk 0
    CHUNK, /// gshared: one sub-chunk through the stream's stage
    SEQCHUNK, /// chunker: self-contained chunk through the worker's sequential stage
    SEQWHOLE, /// rr: whole buffer, in order, through the stream's own sequential stage
    POISON
};

struct Task
{
    Kind kind{Kind::POISON};
    uint32_t stream{0};
    uint32_t slot{0};
    uint32_t chunk{0};
    TupleBuffer buf;
};

struct Config
{
    std::string arm{"dpp"};
    size_t k{4};
    size_t workers{23};
    bool pin{true};
    size_t ring{64};
    size_t D{1}; /// buffers in flight per stream (closed-loop window)
    std::string cut{"scan"}; /// chunker cut method: scan | quote | index | memchr
    std::string label; /// CSV/console arm label: arm, or chunker_<cut> for a non-default cut method
    size_t wsBytes{0}; /// NES_LAT_WS_MIB: floor on the total data working set N*ring*B (ring grows, wrapping the file)
    size_t gensBase{64}; /// NES_LAT_GENS: view generations at the base ring; scaled down when the ring grows (same reuse distance)
    double warmupS{2.0};
    double windowS{10.0};
    unsigned reps{3};
    uint64_t spinNs{50'000};
    size_t cap{0};
    std::string outCsv;
    size_t rawBufBytes{128 * 1024};
    size_t formattedBytes{128 * 1024};
    double heavyFrac{0}; /// NES_LAT_HEAVY: fraction of heavy streams (0 = off)
    uint32_t heavyMult{1}; /// NES_LAT_HEAVY: work multiplier of a heavy stream
    uint64_t burstActiveNs{0}; /// NES_LAT_BURST: active phase (0 = bursts off)
    uint64_t burstIdleNs{0}; /// NES_LAT_BURST: idle phase

    [[nodiscard]] bool bursty() const { return burstActiveNs != 0; }

    [[nodiscard]] uint64_t burstPeriodNs() const { return burstActiveNs + burstIdleNs; }
};

struct RunResult
{
    double windowS{0};
    uint64_t buffers{0};
    uint64_t bytes{0};
    uint64_t tuples{0};
    double p50us{0}, p99us{0}, p999us{0}, maxus{0};
    double perStreamP99MedUs{0}, perStreamP99MaxUs{0};
    double cpuS{0};
    uint64_t vcsw{0}, ivcsw{0};
    uint64_t repeats{0};
    /// NES_LAT_HEAVY per-class: light = costMult 1, heavy = the rest (with the skew off every stream is light)
    uint64_t lightBuffers{0}, heavyBuffers{0};
    double lightP50us{0}, lightP99us{0}, heavyP50us{0}, heavyP99us{0};
    double lightStreamP99MaxUs{0}; /// the worst LIGHT stream's p99: the bystander next to heavy streams
    uint64_t timerWakes{0}, timerLateMaxNs{0}; /// NES_LAT_BURST diagnostics
};

/// ---------------------------------------------------------------------------------------------------------
/// the run context: one arm, N streams, one (warm-up + window) run
/// ---------------------------------------------------------------------------------------------------------
struct Run
{
    const Config& cfg;
    std::vector<std::unique_ptr<Stream>>& streams;
    std::shared_ptr<BufferManager> formattedPool;
    std::vector<std::shared_ptr<CompiledExecutablePipelineStage>>& seqStages; /// chunker: per worker

    folly::MPMCQueue<Task> queue;
    /// Global admission (NES_LAT_CAP > 0): at most `cap` buffers in flight across ALL queries. A completed buffer frees
    /// one admission slot, which goes to the longest-waiting query (FIFO, like sources blocked on the engine's
    /// admission queue); the admitted buffer's latency clock starts at admission (= the e2e's read-start stamp).
    folly::MPMCQueue<uint32_t> waiting;
    /// rr: one queue + semaphore per worker; an item is a stream id ("kick"), the worker then takes that stream's
    /// next buffer in index order (takeNext), so the stream's sequential stage sees its buffers in order
    std::vector<std::unique_ptr<folly::MPMCQueue<uint32_t>>> rrQueues;
    std::vector<std::unique_ptr<Sem>> rrSems;
    static constexpr uint32_t RR_POISON = 0xFFFFFFFFU;
    Sem sem;
    std::atomic<bool> measuring{false};
    std::atomic<bool> stop{false};
    std::atomic<uint32_t> idle{0}; /// diagnostics only (streams that hit zero in flight after stop)
    std::atomic<int64_t> totalInFlight{0}; /// drain condition: every offered buffer has completed
    std::vector<ThreadStats> stats; /// one per worker / gang thread
    /// cap: admission slots held by no in-flight buffer. Zero throughout a run without bursts (a completion hands its
    /// slot straight to its successor or to the waiting head); an idle stream (NES_LAT_BURST) returns its slots here.
    std::atomic<int64_t> freeSlots{0};
    uint64_t phaseBase{0}; /// NES_LAT_BURST: phase reference = the run's start (same schedule in every rep and arm)
    /// NES_LAT_BURST timer: streams asleep until their next active phase; min-heap on the wake instant
    std::mutex timerMu;
    std::condition_variable timerCv;
    std::vector<std::pair<uint64_t, uint32_t>> timerHeap; /// (wakeNs, stream)
    bool timerStop{false};

    Run(const Config& c,
        std::vector<std::unique_ptr<Stream>>& s,
        std::shared_ptr<BufferManager> pool,
        std::vector<std::shared_ptr<CompiledExecutablePipelineStage>>& seq)
        : cfg(c)
        , streams(s)
        , formattedPool(std::move(pool))
        , seqStages(seq)
        , queue(s.size() * c.D * (c.k + 2) + c.workers + 4096)
        , waiting(s.size() * c.D + 2 * c.workers + 64) /// N*D is the hard bound (window per stream); margin for blocked writers
    {
        if (c.arm == "rr")
        {
            for (size_t w = 0; w < c.workers; ++w)
            {
                rrQueues.push_back(std::make_unique<folly::MPMCQueue<uint32_t>>(s.size() * c.D + 64));
                rrSems.push_back(std::make_unique<Sem>());
            }
        }
    }

    void push(Task&& t)
    {
        queue.blockingWrite(std::move(t));
        sem.release();
    }

    static void stampMeta(TupleBuffer& b, const Stream& st, const uint64_t seq)
    {
        b.setSequenceNumber(SequenceNumber(seq));
        b.setChunkNumber(INITIAL<ChunkNumber>);
        b.setLastChunk(true);
        b.setOriginId(OriginId(st.id + 1));
    }

    /// Offer the stream's next buffer to the arm's front-end; `availNs` = when it became available.
    void offer(Stream& st, const uint64_t availNs)
    {
        const uint64_t b = st.nextBuf.fetch_add(1, std::memory_order_acq_rel);
        const size_t R = st.ring.size();
        const size_t slot = b % st.vslots(); /// virtual slot (data slot = slot % R)
        /// The slot's previous buffer (b - vslots) may still be in flight when a task lagged far behind in the queue
        /// (a preempted worker, or one stuck in the stage): WAIT for it, bounded, before touching the slot's bookkeeping.
        if (st.slotBusy[slot].v.exchange(1, std::memory_order_acq_rel) != 0)
        {
            const uint64_t t0 = nowNs();
            g_slotWaits.fetch_add(1, std::memory_order_relaxed);
            while (true)
            {
                cpuRelax();
                if (st.slotBusy[slot].v.load(std::memory_order_acquire) == 0
                    && st.slotBusy[slot].v.exchange(1, std::memory_order_acq_rel) == 0)
                {
                    break;
                }
                const uint64_t waited = nowNs() - t0;
                if (waited > 2'000'000'000ULL)
                {
                    std::string busy;
                    for (size_t i = 0; i < st.vslots(); ++i)
                    {
                        if (st.slotBusy[i].v.load() != 0)
                        {
                            busy += " " + std::to_string(i);
                        }
                    }
                    std::fprintf(
                        stderr,
                        "*** slot still busy after 2 s: stream %zu slot %zu buffer %lu inFlight=%u repeats=%lu remaining[slot]=%u busy "
                        "slots:%s\n",
                        st.id,
                        slot,
                        static_cast<unsigned long>(b),
                        st.inFlight.load(),
                        static_cast<unsigned long>(g_repeats.load()),
                        st.remaining[slot].v.load(),
                        busy.c_str());
                    dumpState("slot reuse");
                    std::abort();
                }
            }
            const uint64_t waited = nowNs() - t0;
            uint64_t cur = g_slotWaitMaxNs.load(std::memory_order_relaxed);
            while (waited > cur && !g_slotWaitMaxNs.compare_exchange_weak(cur, waited))
            {
            }
        }
        auto& s = st.views(slot);
        st.enqNs[slot] = availNs;
        st.seqBase[slot] = 1 + (b / R) * st.chunksPerRing + st.prefixChunks[slot % R];
        st.slotTuples[slot].v.store(0, std::memory_order_relaxed);
        st.inFlight.fetch_add(1, std::memory_order_acq_rel);
        totalInFlight.fetch_add(1, std::memory_order_acq_rel);
        /// publish "bookkeeping of buffer b is complete": two completers of one query can offer concurrently and
        /// finish in either order; the cutter / gang leader takes buffers in index order and must not touch a
        /// slot whose offer() is still in progress (else its completion clears the busy flag before offer() sets it)
        st.ready[slot].v.store(b + 1, std::memory_order_release);
        if (cfg.arm == "dpp")
        {
            stampMeta(s.whole, st, st.seqBase[slot]);
            push(Task{
                .kind = Kind::WHOLE,
                .stream = static_cast<uint32_t>(st.id),
                .slot = static_cast<uint32_t>(slot),
                .chunk = 0,
                .buf = s.whole});
        }
        else if (cfg.arm == "gshared")
        {
            const auto n = static_cast<uint32_t>(s.gangChunks.size());
            for (uint32_t c = 0; c < n; ++c)
            {
                stampMeta(s.gangChunks[c], st, st.seqBase[slot] + c);
            }
            st.remaining[slot].v.store(n, std::memory_order_release);
            push(Task{
                .kind = Kind::FORK, .stream = static_cast<uint32_t>(st.id), .slot = static_cast<uint32_t>(slot), .chunk = 0, .buf = {}});
        }
        else if (cfg.arm == "rr") /// pinned: count on the stream's mailbox, kick the stream's owner worker
        {
            st.pending.fetch_add(1, std::memory_order_acq_rel);
            const size_t w = st.id % cfg.workers;
            rrQueues[w]->blockingWrite(static_cast<uint32_t>(st.id));
            rrSems[w]->release();
        }
        else /// gprivate / chunker: mailbox to the stream's own thread (count of offered buffers)
        {
            st.pending.fetch_add(1, std::memory_order_acq_rel);
            futexWake(st.pending, 1);
        }
    }

    /// ----- admission (NES_LAT_CAP) for buffers with no predecessor slot: the seed and a burst start ---------------
    bool takeSlot()
    {
        int64_t f = freeSlots.load(std::memory_order_acquire);
        while (f > 0)
        {
            if (freeSlots.compare_exchange_weak(f, f - 1, std::memory_order_acq_rel))
            {
                return true;
            }
        }
        return false;
    }

    /// Admit one buffer of `st`: a free slot now, else FIFO behind every query already waiting.
    void admit(Stream& st, const uint64_t availNs)
    {
        if (cfg.cap == 0 || takeSlot())
        {
            offer(st, availNs);
            return;
        }
        st.queued.fetch_add(1, std::memory_order_acq_rel);
        waiting.blockingWrite(static_cast<uint32_t>(st.id));
        drainWaiting(availNs); /// lost-wakeup guard: a slot may have been returned between the check and the push
    }

    /// A completed buffer with no successor (idle stream): its slot goes to the longest-waiting query, else to the pool.
    void releaseSlot(const uint64_t t)
    {
        if (cfg.cap == 0)
        {
            return;
        }
        freeSlots.fetch_add(1, std::memory_order_acq_rel);
        drainWaiting(t);
    }

    void drainWaiting(const uint64_t t)
    {
        uint32_t w = 0;
        while (freeSlots.load(std::memory_order_acquire) > 0 && waiting.readIfNotEmpty(w))
        {
            if (takeSlot())
            {
                streams[w]->queued.fetch_sub(1, std::memory_order_acq_rel);
                offer(*streams[w], t);
            }
            else
            {
                waiting.blockingWrite(w); /// the slot went to a concurrent admit meanwhile: back to the queue
                return;
            }
        }
    }

    /// ----- NES_LAT_BURST: phase schedule + timer thread -------------------------------------------------------------
    [[nodiscard]] bool activeAt(const Stream& st, const uint64_t t) const
    {
        return not cfg.bursty() || ((t - phaseBase + st.phaseOffNs) % cfg.burstPeriodNs()) < cfg.burstActiveNs;
    }

    [[nodiscard]] uint64_t nextActiveNs(const Stream& st, const uint64_t t) const
    {
        const uint64_t pos = (t - phaseBase + st.phaseOffNs) % cfg.burstPeriodNs();
        return (pos < cfg.burstActiveNs) ? t : t + (cfg.burstPeriodNs() - pos);
    }

    /// First idle completion of a phase: put the stream to sleep until its next active phase (asleep CAS = once).
    void sleepUntilActive(Stream& st, const uint64_t t)
    {
        if (st.asleep.exchange(1, std::memory_order_acq_rel) != 0)
        {
            return;
        }
        const uint64_t wake = nextActiveNs(st, t);
        std::lock_guard lock(timerMu);
        timerHeap.emplace_back(wake, static_cast<uint32_t>(st.id));
        std::ranges::push_heap(timerHeap, std::greater<>{});
        timerCv.notify_one();
    }

    /// The timer: at each burst start offer the stream a backlog of D buffers (minus what it still has in flight or
    /// queued for admission), stamped with the actual offer instant.
    void timerThread()
    {
        std::unique_lock lock(timerMu);
        while (true)
        {
            if (timerStop)
            {
                return;
            }
            if (timerHeap.empty())
            {
                timerCv.wait(lock);
                continue;
            }
            const uint64_t due = timerHeap.front().first;
            const uint64_t now = nowNs();
            if (now < due)
            {
                timerCv.wait_until(
                    lock,
                    std::chrono::steady_clock::time_point{
                        std::chrono::duration_cast<std::chrono::steady_clock::duration>(std::chrono::nanoseconds(due))});
                continue;
            }
            std::ranges::pop_heap(timerHeap, std::greater<>{});
            const uint32_t sid = timerHeap.back().second;
            timerHeap.pop_back();
            lock.unlock();
            g_timerWakes.fetch_add(1, std::memory_order_relaxed);
            const uint64_t late = now - due;
            uint64_t cur = g_timerLateMaxNs.load(std::memory_order_relaxed);
            while (late > cur && !g_timerLateMaxNs.compare_exchange_weak(cur, late))
            {
            }
            Stream& st = *streams[sid];
            st.asleep.store(0, std::memory_order_release);
            if (not stop.load(std::memory_order_acquire))
            {
                /// the burst: top the stream's demand (in flight + queued for admission) up to its window D, re-checking the
                /// counts per buffer (completions of leftovers re-offer concurrently; the window must never exceed D)
                while (static_cast<int64_t>(st.inFlight.load(std::memory_order_acquire))
                           + static_cast<int64_t>(st.queued.load(std::memory_order_acquire))
                       < static_cast<int64_t>(cfg.D))
                {
                    admit(st, nowNs());
                }
            }
            lock.lock();
        }
    }

    /// Buffer in `slot` completed: stamp, sample, and (closed loop, window D) offer the next buffer.
    void complete(ThreadStats& ts, Stream& st, const size_t slot)
    {
        const uint64_t done = nowNs();
        g_completions.fetch_add(1, std::memory_order_relaxed);
        st.slotBusy[slot].v.store(0, std::memory_order_release);
        if (measuring.load(std::memory_order_relaxed))
        {
            ts.samples.emplace_back(static_cast<uint32_t>(st.id), done - st.enqNs[slot]);
            ts.bytes += st.bytesOf(slot);
            ts.buffers += 1;
            ts.tuples += st.slotTuples[slot].v.load(std::memory_order_relaxed);
        }
        if (not stop.load(std::memory_order_acquire))
        {
            uint32_t w = 0;
            if (cfg.bursty() && not activeAt(st, done))
            {
                /// idle phase: no successor. Sleep until the next active phase; the admission slot goes away.
                sleepUntilActive(st, done);
                releaseSlot(done);
            }
            else if (cfg.cap != 0 && waiting.readIfNotEmpty(w))
            {
                /// the freed admission slot goes to the longest-waiting query; this query queues for the next one
                st.queued.fetch_add(1, std::memory_order_acq_rel);
                waiting.blockingWrite(static_cast<uint32_t>(st.id));
                streams[w]->queued.fetch_sub(1, std::memory_order_acq_rel);
                offer(*streams[w], done);
            }
            else
            {
                offer(st, done); /// the buffer D ahead becomes available the instant this one is done
            }
        }
        const uint32_t left = st.inFlight.fetch_sub(1, std::memory_order_acq_rel) - 1;
        if (left == 0 && stop.load(std::memory_order_acquire))
        {
            idle.fetch_add(1, std::memory_order_acq_rel);
        }
        totalInFlight.fetch_sub(1, std::memory_order_acq_rel); /// LAST: the drain waits for this to reach zero
    }

    void chunkDone(ThreadStats& ts, Stream& st, const size_t slot, const uint64_t tuples)
    {
        st.slotTuples[slot].v.fetch_add(tuples, std::memory_order_relaxed);
        if (st.remaining[slot].v.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            complete(ts, st, slot);
        }
    }

    /// ----- pooled worker (dpp / gshared / chunker) --------------------------------------------------------
    void worker(const size_t w, const std::vector<int>& cpus, std::latch& ready)
    {
        if (cfg.pin && not cpus.empty())
        {
            pinTo(cpus[w % cpus.size()]);
        }
        ThreadStats& ts = stats[w];
        ready.count_down();
        Task t;
        while (true)
        {
            sem.acquire(cfg.spinNs);
            queue.blockingRead(t);
            if (t.kind == Kind::POISON)
            {
                return;
            }
            Stream& st = *streams[t.stream];
            ts.curStream = t.stream;
            ts.curSlot = t.slot;
            ts.curKind = static_cast<uint8_t>(t.kind);
            ts.curStart = nowNs();
            switch (t.kind)
            {
                case Kind::WHOLE: {
                    const auto n = runStageOnce(st.stage.get(), t.buf, formattedPool, w, st.costMult);
                    if (not n)
                    {
                        push(Task{t}); /// out of the shredder's window: back to the tail, like the engine
                        break;
                    }
                    st.slotTuples[t.slot].v.fetch_add(*n, std::memory_order_relaxed);
                    complete(ts, st, t.slot);
                    break;
                }
                case Kind::FORK: {
                    auto& views = st.views(t.slot);
                    const auto nChunks = static_cast<uint32_t>(views.gangChunks.size());
                    for (uint32_t c = 1; c < nChunks; ++c)
                    {
                        push(Task{.kind = Kind::CHUNK, .stream = t.stream, .slot = t.slot, .chunk = c, .buf = views.gangChunks[c]});
                    }
                    const uint64_t n = runStage(st.stage.get(), views.gangChunks[0], formattedPool, w, st.costMult);
                    chunkDone(ts, st, t.slot, n);
                    break;
                }
                case Kind::CHUNK: {
                    const auto n = runStageOnce(st.stage.get(), t.buf, formattedPool, w, st.costMult);
                    if (not n)
                    {
                        push(Task{t});
                        break;
                    }
                    chunkDone(ts, st, t.slot, *n);
                    break;
                }
                case Kind::SEQCHUNK: {
                    const uint64_t n = runStage(seqStages[w].get(), t.buf, formattedPool, w, st.costMult);
                    chunkDone(ts, st, t.slot, n);
                    break;
                }
                case Kind::SEQWHOLE: /// rr never uses the shared queue
                    std::abort();
                case Kind::POISON:
                    return;
            }
            ts.curStart = 0;
            t.buf = TupleBuffer{}; /// release our reference before blocking on the next task
        }
    }

    /// ----- rr: round-robin pinned workers, one queue each --------------------------------------------------
    void rrWorker(const size_t w, const std::vector<int>& cpus, std::latch& ready)
    {
        if (cfg.pin && not cpus.empty())
        {
            pinTo(cpus[w % cpus.size()]);
        }
        ThreadStats& ts = stats[w];
        ready.count_down();
        uint32_t sid = 0;
        while (true)
        {
            rrSems[w]->acquire(cfg.spinNs);
            rrQueues[w]->blockingRead(sid);
            if (sid == RR_POISON)
            {
                return;
            }
            Stream& st = *streams[sid];
            /// the kick follows the mailbox increment, so takeNext returns at once; it also orders concurrent offers of
            /// one stream (admission hand-offs come from other workers) by buffer index -> this thread, and only this
            /// thread, feeds the stream's sequential stage, in order
            const size_t slot = takeNext(st);
            if (slot == SIZE_MAX)
            {
                return;
            }
            auto& s = st.views(slot);
            stampMeta(s.whole, st, st.seqBase[slot]);
            ts.curStream = sid;
            ts.curSlot = static_cast<uint32_t>(slot);
            ts.curKind = static_cast<uint8_t>(Kind::SEQWHOLE);
            ts.curStart = nowNs();
            const uint64_t n = runStage(st.stage.get(), s.whole, formattedPool, w, st.costMult);
            st.slotTuples[slot].v.fetch_add(n, std::memory_order_relaxed);
            complete(ts, st, slot);
            ts.curStart = 0;
        }
    }

    /// Leader / cutter: take the next offered buffer in order (spin-then-futex on the mailbox count).
    /// Returns the slot, or SIZE_MAX on terminate.
    size_t takeNext(Stream& st)
    {
        uint32_t p = st.pending.load(std::memory_order_acquire);
        while (p == 0)
        {
            p = waitChange(st.pending, 0U, cfg.spinNs);
        }
        if (p == PENDING_TERMINATE)
        {
            return SIZE_MAX;
        }
        st.pending.fetch_sub(1, std::memory_order_acq_rel);
        const uint64_t b = st.takeIdx++;
        const size_t v = b % st.vslots();
        /// the mailbox count may have been raised by a LATER buffer's offer; wait until THIS buffer's offer is done
        while (st.ready[v].v.load(std::memory_order_acquire) != b + 1)
        {
            cpuRelax();
        }
        return v;
    }

    /// ----- gprivate: gang leader + members ---------------------------------------------------------------
    void gangThread(Stream& st, const size_t member, const size_t statsIdx, std::latch& ready)
    {
        /// members take their generation baseline BEFORE signalling readiness: a fork issued between the
        /// two would otherwise be missed and the join would never complete
        uint32_t seenGen = st.gen.load(std::memory_order_acquire);
        ThreadStats& ts = stats[statsIdx];
        ready.count_down();
        const uint64_t threadId = 1000 + st.id * cfg.k + member;
        while (true)
        {
            if (member == 0)
            {
                const size_t slot = takeNext(st);
                if (slot == SIZE_MAX)
                {
                    return;
                }
                auto& s = st.views(slot);
                const auto n = static_cast<uint32_t>(s.gangChunks.size());
                for (uint32_t c = 0; c < n; ++c)
                {
                    stampMeta(s.gangChunks[c], st, st.seqBase[slot] + c);
                }
                st.curSlot = slot;
                st.curChunks = n;
                st.remaining[slot].v.store(n, std::memory_order_relaxed);
                /// fork: publish the generation (release orders curSlot/curChunks/remaining before it)
                seenGen = st.gen.fetch_add(1, std::memory_order_acq_rel) + 1;
                futexWake(st.gen, INT32_MAX);
                const uint64_t t = runStage(st.stage.get(), s.gangChunks[0], formattedPool, threadId, st.costMult);
                chunkDone(ts, st, slot, t);
                /// join: the leader may only take the next buffer once this one is complete (one buffer at a
                /// time per gang = parallel-for semantics). Wait on the slot's remaining counter.
                uint32_t left = st.remaining[slot].v.load(std::memory_order_acquire);
                while (left != 0)
                {
                    left = waitChange(st.remaining[slot].v, left, cfg.spinNs);
                }
            }
            else
            {
                seenGen = waitChange(st.gen, seenGen, cfg.spinNs);
                if (st.terminate.load(std::memory_order_acquire) != 0)
                {
                    return;
                }
                const size_t slot = st.curSlot;
                if (member < st.curChunks)
                {
                    const uint64_t t = runStage(st.stage.get(), st.views(slot).gangChunks[member], formattedPool, threadId, st.costMult);
                    chunkDoneWake(ts, st, slot, t);
                }
            }
        }
    }

    /// chunkDone + wake the leader if it parked on the join counter.
    void chunkDoneWake(ThreadStats& ts, Stream& st, const size_t slot, const uint64_t tuples)
    {
        st.slotTuples[slot].v.fetch_add(tuples, std::memory_order_relaxed);
        if (st.remaining[slot].v.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            complete(ts, st, slot);
            futexWake(st.remaining[slot].v, 1);
        }
    }

    /// ----- chunker: one light cutter thread per stream ---------------------------------------------------
    void chunkerThread(Stream& st, std::latch& ready)
    {
        ready.count_down();
        const size_t k = cfg.k;
        const size_t B = cfg.rawBufBytes;
        while (true)
        {
            const size_t slotIdx = takeNext(st);
            if (slotIdx == SIZE_MAX)
            {
                return;
            }
            auto& slot = st.ring[slotIdx % st.ring.size()];
            const char* const data = reinterpret_cast<const char*>(st.mem) + (slotIdx % st.ring.size()) * B;
            const size_t bytes = slot.bytes;

            size_t headEnd = 0;
            size_t prevCut = 0;
            std::vector<std::pair<size_t, size_t>> ranges;
            ranges.reserve(k);
            if (cfg.cut == "index")
            {
                /// --- the real cutting work: one FULL structural scan of the buffer with the SIMDCSV indexer (the
                /// same quote-aware pass DPP's stage runs), then cut at the first newline at/after each k-th boundary
                /// using the band's newline positions (band[b0 + r*N] is the newline that ends record r-1).
                const RawTupleBuffer raw(st.views(slotIdx).whole);
                st.cutIndexer->indexRawBuffer(st.cutBand, raw, *st.cutMeta);
                const auto firstNl = static_cast<size_t>(st.cutBand.getByteOffsetOfFirstTuple());
                const auto lastNl = static_cast<size_t>(st.cutBand.getByteOffsetOfLastTuple());
                const size_t T = st.cutBand.getTotalNumberOfTuples();
                const FieldIndex* band = st.cutBand.prepareBand(0);
                size_t b0 = 0;
                while (static_cast<size_t>(band[b0]) != firstNl)
                {
                    ++b0;
                }
                headEnd = firstNl;
                prevCut = firstNl;
                const size_t Nf = st.cutFields;
                for (size_t c = 1; c <= k; ++c)
                {
                    size_t cut;
                    if (c == k)
                    {
                        cut = lastNl;
                    }
                    else
                    {
                        const size_t boundary = std::min(bytes, (c * B) / k);
                        /// first record-ending newline >= boundary (binary search over the T+1 newline entries)
                        size_t lo = 0, hi = T + 1;
                        while (lo < hi)
                        {
                            const size_t mid = (lo + hi) / 2;
                            if (static_cast<size_t>(band[b0 + mid * Nf]) < boundary)
                            {
                                lo = mid + 1;
                            }
                            else
                            {
                                hi = mid;
                            }
                        }
                        if (lo > T)
                        {
                            continue;
                        }
                        cut = static_cast<size_t>(band[b0 + lo * Nf]);
                    }
                    if (cut <= prevCut)
                    {
                        continue;
                    }
                    ranges.emplace_back(prevCut + 1, cut - prevCut);
                    prevCut = cut;
                }
            }
            else if (cfg.cut == "scan" || cfg.cut == "quote")
            {
                /// --- the chunker's inherent work: ONE sequential pass over every byte detecting record boundaries
                /// (newlines only, no field offsets), then cut at the first boundary at/after each k-th cut point.
                /// quote mode: the same pass also carries the quote parity (general CSV: a newline inside a quoted
                /// field is not a boundary) -- the work a quote-aware cutter cannot skip.
                if (cfg.cut == "quote")
                {
                    scanNewlinesQuoted(data, bytes, st.cutQuoteCarry, st.cutNewlines);
                }
                else
                {
                    scanNewlines(data, bytes, st.cutNewlines);
                }
                const auto& nls = st.cutNewlines;
                if (nls.empty())
                {
                    std::fprintf(stderr, "chunker: no record boundary in a %zu-byte buffer\n", bytes);
                    std::abort();
                }
                headEnd = nls.front();
                prevCut = headEnd;
                for (size_t c = 1; c <= k; ++c)
                {
                    size_t cut;
                    if (c == k)
                    {
                        cut = nls.back();
                    }
                    else
                    {
                        const size_t boundary = std::min(bytes, (c * B) / k);
                        const auto it = std::lower_bound(nls.begin(), nls.end(), static_cast<uint32_t>(boundary));
                        if (it == nls.end())
                        {
                            continue;
                        }
                        cut = *it;
                    }
                    if (cut <= prevCut)
                    {
                        continue;
                    }
                    ranges.emplace_back(prevCut + 1, cut - prevCut);
                    prevCut = cut;
                }
            }
            else
            {
                /// --- quote-free shortcut: k+1 newline searches from the cut points (+ one from the end) ---
                const auto* first = static_cast<const char*>(std::memchr(data, '\n', bytes));
                headEnd = static_cast<size_t>(first - data);
                prevCut = headEnd;
                for (size_t c = 1; c <= k; ++c)
                {
                    const char* nl = nullptr;
                    if (c == k)
                    {
                        for (size_t i = bytes; i > 0; --i)
                        {
                            if (data[i - 1] == '\n')
                            {
                                nl = data + i - 1;
                                break;
                            }
                        }
                    }
                    else
                    {
                        const size_t from = std::min(bytes, (c * B) / k);
                        if (from < bytes)
                        {
                            nl = static_cast<const char*>(std::memchr(data + from, '\n', bytes - from));
                        }
                    }
                    if (nl == nullptr)
                    {
                        continue;
                    }
                    const auto cut = static_cast<size_t>(nl - data);
                    if (cut <= prevCut)
                    {
                        continue;
                    }
                    ranges.emplace_back(prevCut + 1, cut - prevCut);
                    prevCut = cut;
                }
            }
            const size_t tailStart = prevCut + 1;
            if (headEnd != slot.headEnd || tailStart != slot.tailStart || ranges != slot.cutRanges)
            {
                std::fprintf(stderr, "chunker: runtime cuts differ from setup views (stream %zu slot %zu)\n", st.id, slotIdx);
                std::abort();
            }

            /// --- boundary row = carried tail of the previous buffer + head [0, headEnd] of this one ---
            auto staged = st.stagePool->getBufferBlocking();
            auto stagedMem = staged.getAvailableMemoryArea<char>();
            const size_t rowBytes = st.carry.size() + headEnd + 1;
            if (rowBytes > stagedMem.size())
            {
                std::fprintf(stderr, "chunker: boundary row (%zu B) exceeds staging buffer\n", rowBytes);
                std::abort();
            }
            std::memcpy(stagedMem.data(), st.carry.data(), st.carry.size());
            std::memcpy(stagedMem.data() + st.carry.size(), data, headEnd + 1);
            staged.setNumberOfTuples(rowBytes);
            st.carry.assign(data + tailStart, data + bytes);

            auto& views = st.views(slotIdx);
            const auto n = static_cast<uint32_t>(views.cutChunks.size()) + 1;
            st.remaining[slotIdx].v.store(n, std::memory_order_release);
            push(Task{
                .kind = Kind::SEQCHUNK,
                .stream = static_cast<uint32_t>(st.id),
                .slot = static_cast<uint32_t>(slotIdx),
                .chunk = 0,
                .buf = staged});
            for (uint32_t c = 0; c < views.cutChunks.size(); ++c)
            {
                push(Task{
                    .kind = Kind::SEQCHUNK,
                    .stream = static_cast<uint32_t>(st.id),
                    .slot = static_cast<uint32_t>(slotIdx),
                    .chunk = c + 1,
                    .buf = views.cutChunks[c]});
            }
        }
    }

    /// Sleep `seconds`, aborting with a state dump if no buffer completes for 3 s (deadlock/livelock detector).
    void watchedSleep(const double seconds, const char* phase)
    {
        const uint64_t end = nowNs() + static_cast<uint64_t>(seconds * 1e9);
        uint64_t lastC = g_completions.load();
        uint64_t stallMs = 0;
        while (nowNs() < end)
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            const uint64_t c = g_completions.load();
            stallMs = (c == lastC) ? stallMs + 1 : 0;
            lastC = c;
            if (stallMs > 3000)
            {
                dumpState(phase);
                std::abort();
            }
        }
    }

    void dumpState(const char* phase)
    {
        std::fprintf(
            stderr,
            "\n*** STALL in %s: no completion for 3 s. repeats=%lu completions=%lu idle=%u\n",
            phase,
            static_cast<unsigned long>(g_repeats.load()),
            static_cast<unsigned long>(g_completions.load()),
            idle.load());
        for (auto& st : streams)
        {
            std::string rem;
            for (size_t i = 0; i < st->vslots(); ++i)
            {
                const auto v = st->remaining[i].v.load();
                if (v != 0)
                {
                    rem += " slot" + std::to_string(i) + "=" + std::to_string(v);
                }
            }
            std::fprintf(
                stderr,
                "  stream %zu: inFlight=%u pending=%u nextBuf=%lu takeIdx=%zu gen=%u remaining:%s\n",
                st->id,
                st->inFlight.load(),
                st->pending.load(),
                static_cast<unsigned long>(st->nextBuf.load()),
                st->takeIdx,
                st->gen.load(),
                rem.c_str());
        }
        std::fprintf(
            stderr,
            "  queue size=%zu waiting=%zu freeSlots=%ld timerHeap=%zu\n",
            queue.sizeGuess(),
            waiting.sizeGuess(),
            static_cast<long>(freeSlots.load()),
            timerHeap.size());
        for (size_t w = 0; w < rrQueues.size(); ++w)
        {
            std::fprintf(stderr, "  rr queue %zu size=%zu\n", w, rrQueues[w]->sizeGuess());
        }
        const uint64_t now = nowNs();
        for (size_t w = 0; w < stats.size(); ++w)
        {
            const auto& ts = stats[w];
            const uint64_t start = ts.curStart;
            if (start != 0)
            {
                const double ageMs = (now > start) ? static_cast<double>(now - start) / 1e6 : 0.0;
                std::fprintf(
                    stderr,
                    "  worker %zu: kind=%u stream=%u slot=%u age=%.3f ms%s\n",
                    w,
                    ts.curKind,
                    ts.curStream,
                    ts.curSlot,
                    ageMs,
                    ageMs > 50.0 ? "  <-- STUCK" : "");
            }
        }
    }

    /// ----- one measured run ------------------------------------------------------------------------------
    RunResult execute()
    {
        const size_t N = streams.size();
        const auto cpus = allowedCpus();
        const bool pooled = (cfg.arm == "dpp" || cfg.arm == "gshared" || cfg.arm == "chunker");
        const bool rr = (cfg.arm == "rr");
        const size_t nWorkers = (pooled || rr) ? cfg.workers : 0;
        const size_t nGang = (cfg.arm == "gprivate") ? N * cfg.k : 0;
        const size_t nChunkers = (cfg.arm == "chunker") ? N : 0;

        for (auto& st : streams)
        {
            const size_t R = st->vslots();
            st->enqNs.assign(R, 0);
            st->seqBase.assign(R, 0);
            if (st->remaining.size() != R)
            {
                st->remaining = std::vector<PaddedU32>(R);
                st->slotTuples = std::vector<PaddedU64>(R);
                st->slotBusy = std::vector<PaddedU32>(R);
                st->ready = std::vector<PaddedU64>(R);
            }
            for (size_t i = 0; i < R; ++i)
            {
                st->remaining[i].v.store(0);
                st->slotTuples[i].v.store(0);
                st->slotBusy[i].v.store(0);
                st->ready[i].v.store(0);
            }
            st->pending.store(0);
            st->inFlight.store(0);
            st->queued.store(0);
            st->asleep.store(0);
            st->terminate.store(0);
            st->carry.clear();
            st->cutQuoteCarry = 0;
            /// buffer counter continues across reps (monotone sequence numbers for the shredder); the take
            /// index must stay in step with it
            st->takeIdx = st->nextBuf.load(); /// buffer INDEX (takeNext applies % vslots and matches ready[] = b + 1)
        }
        stop.store(false);
        measuring.store(false);
        idle.store(0);
        totalInFlight.store(0);
        g_slotWaits.store(0);
        g_slotWaitMaxNs.store(0);
        g_repeats.store(0);
        g_timerWakes.store(0);
        g_timerLateMaxNs.store(0);
        freeSlots.store(static_cast<int64_t>(cfg.cap));
        phaseBase = nowNs();
        stats.assign(nWorkers + nGang, ThreadStats{});
        std::thread timer; /// NES_LAT_BURST: started AFTER the seed (see there)

        std::latch ready(static_cast<std::ptrdiff_t>(nWorkers + nGang + nChunkers));
        std::vector<std::thread> threads;
        threads.reserve(nWorkers + nGang + nChunkers);
        for (size_t w = 0; w < nWorkers; ++w)
        {
            if (rr)
            {
                threads.emplace_back([this, w, &cpus, &ready] { rrWorker(w, cpus, ready); });
            }
            else
            {
                threads.emplace_back([this, w, &cpus, &ready] { worker(w, cpus, ready); });
            }
        }
        for (size_t i = 0; i < N && cfg.arm == "gprivate"; ++i)
        {
            for (size_t m = 0; m < cfg.k; ++m)
            {
                const size_t statsIdx = nWorkers + i * cfg.k + m;
                threads.emplace_back([this, i, m, statsIdx, &ready] { gangThread(*streams[i], m, statsIdx, ready); });
            }
        }
        for (size_t i = 0; i < N && cfg.arm == "chunker"; ++i)
        {
            threads.emplace_back([this, i, &ready] { chunkerThread(*streams[i], ready); });
        }
        ready.wait();

        /// seed: D buffers per stream; the first round is staggered over ~100 ms so the shredders do not start in
        /// lock-step, the remaining D-1 rounds are offered immediately (a slow seed would run the closed loop at a
        /// smaller window than D for its whole duration)
        /// NES_LAT_BURST: the timer thread must NOT run during the seed. The seed's first round is staggered over ~100 ms
        /// (longer than a burst period), so a stream that was idle at its seed slot and got its burst (D buffers) from the
        /// timer meanwhile would be admitted AGAIN by the later seed rounds: up to 2D of demand per stream, more entries than
        /// the admission FIFO (N*D) holds -> the seed / the workers / the timer block on a full FIFO (2026-09-23, Pi N=64 and
        /// TR N=512). So: seed first (a stream is seeded only while inFlight + queued < D), then start the timer, which gives
        /// every stream that was idle at the seed its first burst (D - what it has) at its next active phase.
        const auto stagger = std::chrono::microseconds(100'000 / std::max<size_t>(1, N));
        for (size_t d = 0; d < cfg.D; ++d)
        {
            for (auto& st : streams)
            {
                const uint64_t now = nowNs();
                if (not activeAt(*st, now))
                {
                    sleepUntilActive(*st, now); /// idle at the seed -> its first burst comes from the timer (after the seed)
                }
                else if (st->inFlight.load(std::memory_order_acquire) + st->queued.load(std::memory_order_acquire) < cfg.D)
                {
                    admit(*st, now); /// a free slot, or beyond the global cap: wait for admission (FIFO)
                }
                if (d == 0)
                {
                    std::this_thread::sleep_for(stagger);
                }
            }
        }
        if (cfg.bursty())
        {
            timer = std::thread([this] { timerThread(); });
        }

        watchedSleep(cfg.warmupS, "warmup");
        rusage ru0{};
        getrusage(RUSAGE_SELF, &ru0);
        const uint64_t t0 = nowNs();
        g_timerWakes.store(
            0); /// NES_LAT_BURST diagnostics describe the measured window only (the seed's catch-up wakes are late by design)
        g_timerLateMaxNs.store(0);
        measuring.store(true, std::memory_order_release);
        watchedSleep(cfg.windowS, "window");
        measuring.store(false, std::memory_order_release);
        const uint64_t t1 = nowNs();
        rusage ru1{};
        getrusage(RUSAGE_SELF, &ru1);

        /// stop offering; wait for every stream's in-flight buffers to complete
        stop.store(true, std::memory_order_release);
        {
            uint64_t lastC = g_completions.load();
            uint64_t stallMs = 0;
            /// drain = every offered buffer completed (under global admission a waiting query never completes anything,
            /// so per-query idle counting cannot be the condition)
            while (totalInFlight.load(std::memory_order_acquire) > 0)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                const uint64_t c = g_completions.load();
                stallMs = (c == lastC) ? stallMs + 1 : 0;
                lastC = c;
                if (stallMs > 3000)
                {
                    dumpState("drain stall");
                    std::abort();
                }
            }
        }
        /// terminate threads (the timer first: after stop it never offers, so its pending wakes are void)
        if (timer.joinable())
        {
            {
                std::lock_guard lock(timerMu);
                timerStop = true;
            }
            timerCv.notify_all();
            timer.join();
        }
        for (size_t w = 0; w < nWorkers; ++w)
        {
            if (rr)
            {
                rrQueues[w]->blockingWrite(RR_POISON);
                rrSems[w]->release();
            }
            else
            {
                push(Task{});
            }
        }
        for (auto& st : streams)
        {
            st->terminate.store(1, std::memory_order_release);
            st->pending.store(PENDING_TERMINATE, std::memory_order_release);
            futexWake(st->pending, INT32_MAX);
            st->gen.fetch_add(1, std::memory_order_acq_rel);
            futexWake(st->gen, INT32_MAX);
        }
        for (auto& t : threads)
        {
            t.join();
        }

        /// ----- stats -----
        RunResult r;
        r.windowS = static_cast<double>(t1 - t0) / 1e9;
        std::vector<std::pair<uint32_t, uint64_t>> all;
        for (auto& ts : stats)
        {
            r.buffers += ts.buffers;
            r.bytes += ts.bytes;
            r.tuples += ts.tuples;
            all.insert(all.end(), ts.samples.begin(), ts.samples.end());
        }
        if (not all.empty())
        {
            /// per-stream p99: group by stream (sort by stream, then latency)
            std::ranges::sort(all);
            std::vector<double> perStreamP99;
            for (size_t i = 0; i < all.size();)
            {
                size_t j = i;
                while (j < all.size() && all[j].first == all[i].first)
                {
                    ++j;
                }
                const size_t n = j - i;
                perStreamP99.push_back(static_cast<double>(all[i + std::min(n - 1, static_cast<size_t>(n * 0.99))].second) / 1e3);
                if (streams[all[i].first]->costMult == 1)
                {
                    r.lightStreamP99MaxUs = std::max(r.lightStreamP99MaxUs, perStreamP99.back());
                }
                i = j;
            }
            /// per-class pooled percentiles (NES_LAT_HEAVY; with the skew off every stream is light)
            {
                std::vector<uint64_t> latL, latH;
                for (const auto& [sid, l] : all)
                {
                    (streams[sid]->costMult > 1 ? latH : latL).push_back(l);
                }
                const auto pctOf = [](std::vector<uint64_t>& v, const double p)
                {
                    if (v.empty())
                    {
                        return 0.0;
                    }
                    std::ranges::sort(v);
                    return static_cast<double>(v[std::min(v.size() - 1, static_cast<size_t>(static_cast<double>(v.size()) * p))]) / 1e3;
                };
                r.lightBuffers = latL.size();
                r.heavyBuffers = latH.size();
                r.lightP50us = pctOf(latL, 0.50);
                r.lightP99us = pctOf(latL, 0.99);
                r.heavyP50us = pctOf(latH, 0.50);
                r.heavyP99us = pctOf(latH, 0.99);
            }
            std::ranges::sort(perStreamP99);
            r.perStreamP99MedUs = perStreamP99[perStreamP99.size() / 2];
            r.perStreamP99MaxUs = perStreamP99.back();
            /// pooled percentiles
            std::vector<uint64_t> lat;
            lat.reserve(all.size());
            for (const auto& [sid, l] : all)
            {
                lat.push_back(l);
            }
            std::ranges::sort(lat);
            const auto pct = [&](const double p)
            { return static_cast<double>(lat[std::min(lat.size() - 1, static_cast<size_t>(lat.size() * p))]) / 1e3; };
            r.p50us = pct(0.50);
            r.p99us = pct(0.99);
            r.p999us = pct(0.999);
            r.maxus = static_cast<double>(lat.back()) / 1e3;
        }
        const auto tv = [](const timeval& a, const timeval& b) { return (b.tv_sec - a.tv_sec) + (b.tv_usec - a.tv_usec) / 1e6; };
        r.cpuS = tv(ru0.ru_utime, ru1.ru_utime) + tv(ru0.ru_stime, ru1.ru_stime);
        r.vcsw = static_cast<uint64_t>(ru1.ru_nvcsw - ru0.ru_nvcsw);
        r.ivcsw = static_cast<uint64_t>(ru1.ru_nivcsw - ru0.ru_nivcsw);
        r.repeats = g_repeats.load();
        r.timerWakes = g_timerWakes.load();
        r.timerLateMaxNs = g_timerLateMaxNs.load();
        return r;
    }
};

/// ---------------------------------------------------------------------------------------------------------
/// setup: file -> master ring -> per-stream memory + views; stages
/// ---------------------------------------------------------------------------------------------------------
struct Master
{
    std::vector<char> data; /// R * B bytes (last slot trimmed; padding zero)
    std::vector<size_t> slotBytes;
    size_t rows{0};
    size_t totalBytes{0};
};

Master loadMaster(const std::string& path, const size_t B, const size_t R)
{
    Master m;
    const auto fileSize = std::filesystem::file_size(path);
    const size_t want = std::min<size_t>(fileSize, B * R);
    m.data.assign(((want + B - 1) / B) * B + 4096, 0); /// +4 KiB slack: SIMD block over-read past the last chunk stays in bounds
    std::ifstream f(path, std::ios::binary);
    f.read(m.data.data(), static_cast<std::streamsize>(want));
    const size_t got = static_cast<size_t>(f.gcount());
    const size_t nSlots = (got + B - 1) / B;
    m.slotBytes.assign(nSlots, B);
    m.slotBytes.back() = got - (nSlots - 1) * B;
    /// trim the last slot to its last newline so the ring wrap is a record boundary
    {
        const size_t base = (nSlots - 1) * B;
        size_t len = m.slotBytes.back();
        while (len > 0 && m.data[base + len - 1] != '\n')
        {
            --len;
        }
        m.slotBytes.back() = len;
        std::fill(m.data.begin() + static_cast<std::ptrdiff_t>(base + len), m.data.end(), 0);
    }
    for (size_t s = 0; s < nSlots; ++s)
    {
        m.totalBytes += m.slotBytes[s];
        m.rows += static_cast<size_t>(std::count(
            m.data.begin() + static_cast<std::ptrdiff_t>(s * B),
            m.data.begin() + static_cast<std::ptrdiff_t>(s * B + m.slotBytes[s]),
            '\n'));
    }
    return m;
}

/// Compute the chunker's record-aligned cut ranges for one slot (setup-time twin of chunkerThread's loop).
void computeCuts(const char* data, const size_t bytes, const size_t B, const size_t k, Slot& slot)
{
    const auto* first = static_cast<const char*>(std::memchr(data, '\n', bytes));
    slot.headEnd = static_cast<size_t>(first - data);
    size_t prevCut = slot.headEnd;
    slot.cutRanges.clear();
    for (size_t c = 1; c <= k; ++c)
    {
        const char* nl = nullptr;
        if (c == k)
        {
            for (size_t i = bytes; i > 0; --i)
            {
                if (data[i - 1] == '\n')
                {
                    nl = data + i - 1;
                    break;
                }
            }
        }
        else
        {
            const size_t from = std::min(bytes, (c * B) / k);
            if (from < bytes)
            {
                nl = static_cast<const char*>(std::memchr(data + from, '\n', bytes - from));
            }
        }
        if (nl == nullptr)
        {
            continue;
        }
        const auto cut = static_cast<size_t>(nl - data);
        if (cut <= prevCut)
        {
            continue;
        }
        slot.cutRanges.emplace_back(prevCut + 1, cut - prevCut);
        prevCut = cut;
    }
    slot.tailStart = prevCut + 1;
}

std::unique_ptr<Stream> makeStream(
    const size_t id, const Master& m, const Config& cfg, BufferManager& wrapPool, const bool needGang, const bool needCuts, const size_t R)
{
    /// R data slots (>= the master's slot count wraps the file: slot s holds master slot s % n; the master's last
    /// slot is trimmed to a newline, so every wrap is a record boundary). The ring is the stream's private data
    /// working set: with NES_LAT_WS_MIB the caller sizes R so N*R*B stays in one memory regime at every N.
    auto st = std::make_unique<Stream>();
    st->id = id;
    const size_t B = cfg.rawBufBytes;
    const size_t n = m.slotBytes.size();
    st->memBytes = R * B + 4096;
    st->mem = static_cast<uint8_t*>(std::aligned_alloc(4096, st->memBytes));
    for (size_t s = 0; s < R; ++s)
    {
        std::memcpy(st->mem + s * B, m.data.data() + (s % n) * B, B);
    }
    std::memset(st->mem + R * B, 0, 4096);
    st->ring.resize(R);
    const auto noop = [] { };
    for (size_t s = 0; s < st->ring.size(); ++s)
    {
        Slot& slot = st->ring[s];
        slot.bytes = m.slotBytes[s % n];
        uint8_t* base = st->mem + s * B;
        if (needCuts)
        {
            computeCuts(reinterpret_cast<const char*>(base), slot.bytes, B, cfg.k, slot);
        }
        slot.gen.resize(VIEW_GENERATIONS);
        for (auto& g : slot.gen)
        {
            g.whole = wrapPool.wrapExternalMemory(base, static_cast<uint32_t>(B), noop);
            g.whole.setNumberOfTuples(slot.bytes);
            if (needGang)
            {
                for (size_t c = 0; c < cfg.k; ++c)
                {
                    const size_t start = (c * B) / cfg.k;
                    const size_t end = std::min(slot.bytes, ((c + 1) * B) / cfg.k);
                    if (start >= end)
                    {
                        break;
                    }
                    /// view size = chunk + 64 B slack for the SIMD block over-read (stays inside the slot/padding)
                    auto v = wrapPool.wrapExternalMemory(
                        base + start, static_cast<uint32_t>(std::min(end - start + 64, B - start + 4096)), noop);
                    v.setNumberOfTuples(end - start);
                    g.gangChunks.push_back(std::move(v));
                }
            }
            if (needCuts)
            {
                for (const auto& [start, len] : slot.cutRanges)
                {
                    auto v = wrapPool.wrapExternalMemory(base + start, static_cast<uint32_t>(std::min(len + 64, B - start + 4096)), noop);
                    v.setNumberOfTuples(len);
                    g.cutChunks.push_back(std::move(v));
                }
            }
        }
    }
    if (needCuts)
    {
        /// boundary-row staging: 16 KiB holds two rows of any dataset used here; abort-checked at runtime
        st->stagePool = BufferManager::create(16 * 1024, 4 + 2 * cfg.D);
    }
    /// chunk sequence-number layout per ring cycle (only the shredder arms care; chunker uses per-worker counters)
    st->prefixChunks.assign(st->ring.size(), 0);
    uint64_t acc = 0;
    for (size_t s2 = 0; s2 < st->ring.size(); ++s2)
    {
        st->prefixChunks[s2] = acc;
        acc += needGang ? st->ring[s2].gen[0].gangChunks.size() : 1;
    }
    st->chunksPerRing = acc;
    return st;
}

DescriptorConfig::Config validatedConfig(const std::string& type, const std::string& uint64Parser, const std::string& allowCommas)
{
    std::unordered_map<std::string, std::string> idxParams
        = {{"type", type}, {"tuple_delimiter", "\n"}, {"field_delimiter", ","}, {"sequence_shredder_mode", "LOCK_FREE"}};
    if (not allowCommas.empty())
    {
        idxParams["allow_commas_in_strings"] = allowCommas;
    }
    const auto conf = InputFormatterValidationProvider::provide(type, idxParams);
    if (not conf.has_value())
    {
        throw std::runtime_error("Invalid " + type + " indexer config");
    }
    auto formatterConfig = conf.value();
    if (not uint64Parser.empty())
    {
        formatterConfig[InputFormatterDescriptor::UINT64_PARSER.name] = uint64Parser;
    }
    return formatterConfig;
}

std::shared_ptr<CompiledExecutablePipelineStage> makeStage(
    const std::string& type,
    const Schema& schema,
    const std::optional<Schema>& projection,
    const std::string& uint64Parser,
    const std::string& allowCommas,
    const size_t formattedBytes)
{
    return InputFormatterTestUtil::createInputFormatter(
        validatedConfig(type, uint64Parser, allowCommas),
        schema,
        MemoryLayoutType::ROW_LAYOUT,
        formattedBytes,
        /*isCompiled*/ true,
        projection);
}

}

int main(int argc, char** argv)
{
    NES::Logger::setupLogging("formatter-latency-benchmark.log", NES::LogLevel::LOG_ERROR);
    std::signal(SIGSEGV, segvHandler);
    std::signal(SIGABRT, segvHandler);
    std::signal(SIGBUS, segvHandler);

    const std::string filePath = (argc > 1) ? argv[1] : "nes-systests/testdata/large/formatter/bench_5xUINT64_512m.csv";
    Config cfg;
    cfg.rawBufBytes = static_cast<size_t>(((argc > 2) ? std::strtod(argv[2], nullptr) : 128.0) * 1024.0);
    cfg.formattedBytes = cfg.rawBufBytes;
    const size_t numCols = (argc > 3) ? std::strtoull(argv[3], nullptr, 10) : 5;
    const std::string projectField = (argc > 4 && std::string(argv[4]) != "-") ? argv[4] : "f1";
    const std::string fnattrPath = (argc > 5 && std::string(argv[5]) != "-") ? argv[5] : "";
    const std::string uint64Parser = (argc > 6 && std::string(argv[6]) != "-") ? argv[6] : "FastUINT64";
    const std::string allowCommas = envStr("NES_INDEX_ALLOW_COMMAS", "");

    cfg.arm = envStr("NES_LAT_ARM", "dpp");
    cfg.k = envU64("NES_LAT_K", 4);
    cfg.workers = envU64("NES_LAT_WORKERS", 23);
    cfg.pin = envU64("NES_LAT_PIN", 1) != 0;
    cfg.ring = envU64("NES_LAT_RING", 64);
    cfg.D = std::max<size_t>(1, envU64("NES_LAT_D", 1));
    cfg.cut = envStr("NES_LAT_CUT", "scan");
    if (cfg.cut != "scan" && cfg.cut != "quote" && cfg.cut != "index" && cfg.cut != "memchr")
    {
        std::fprintf(stderr, "unknown NES_LAT_CUT=%s (scan|quote|index|memchr)\n", cfg.cut.c_str());
        return 2;
    }
    VIEW_GENERATIONS = std::max<size_t>(2, envU64("NES_LAT_GENS", 64));
    cfg.gensBase = VIEW_GENERATIONS;
    cfg.wsBytes = envU64("NES_LAT_WS_MIB", 0) << 20;
    /// formatted (output) buffer size: the projected single UINT64 column needs ~1/4 of the raw bytes
    cfg.formattedBytes = envU64("NES_LAT_FMT_KIB", cfg.rawBufBytes / 1024) * 1024;
    /// D buffers in flight must fit the data ring; control-block reuse distance is R * VIEW_GENERATIONS
    cfg.ring = std::max(cfg.ring, cfg.D + 8);
    cfg.warmupS = std::strtod(envStr("NES_LAT_WARMUP_S", "2").c_str(), nullptr);
    cfg.windowS = std::strtod(envStr("NES_LAT_WINDOW_S", "10").c_str(), nullptr);
    cfg.reps = static_cast<unsigned>(envU64("NES_LAT_REPS", 3));
    cfg.spinNs = envU64("NES_LAT_SPIN_US", 50) * 1000;
    cfg.cap = envU64("NES_LAT_CAP", 0);
    cfg.outCsv = envStr("NES_LAT_OUT", "");
    if (const auto hv = envStr("NES_LAT_HEAVY", ""); not hv.empty())
    {
        const auto comma = hv.find(',');
        cfg.heavyFrac = std::strtod(hv.c_str(), nullptr);
        cfg.heavyMult = (comma == std::string::npos) ? 0 : static_cast<uint32_t>(std::strtoul(hv.c_str() + comma + 1, nullptr, 10));
        if (cfg.heavyFrac <= 0.0 || cfg.heavyFrac > 1.0 || cfg.heavyMult < 2)
        {
            std::fprintf(stderr, "NES_LAT_HEAVY=%s: want <fraction in (0,1]>,<mult >= 2>\n", hv.c_str());
            return 2;
        }
    }
    if (const auto bv = envStr("NES_LAT_BURST", ""); not bv.empty())
    {
        const auto comma = bv.find(',');
        const double activeMs = std::strtod(bv.c_str(), nullptr);
        const double idleMs = (comma == std::string::npos) ? 0.0 : std::strtod(bv.c_str() + comma + 1, nullptr);
        if (activeMs <= 0.0 || idleMs <= 0.0 || idleMs >= 3000.0)
        {
            std::fprintf(stderr, "NES_LAT_BURST=%s: want <active_ms > 0>,<idle_ms in (0, 3000)>\n", bv.c_str());
            return 2;
        }
        cfg.burstActiveNs = static_cast<uint64_t>(activeMs * 1e6);
        cfg.burstIdleNs = static_cast<uint64_t>(idleMs * 1e6);
    }
    const auto streamCounts = parseList(envStr("NES_LAT_STREAMS", "1,4,16,64"));
    if (cfg.arm != "dpp" && cfg.arm != "gshared" && cfg.arm != "gprivate" && cfg.arm != "chunker" && cfg.arm != "rr")
    {
        std::fprintf(stderr, "unknown NES_LAT_ARM=%s\n", cfg.arm.c_str());
        return 2;
    }
    cfg.label = (cfg.arm == "chunker" && cfg.cut != "scan") ? "chunker_" + cfg.cut : cfg.arm;
    if (cfg.arm == "dpp" || cfg.arm == "rr")
    {
        cfg.k = 1;
    }
    if (not fnattrPath.empty())
    {
        const bool ok = NES::InvokeConfig::instance().loadFromFile(fnattrPath);
        std::printf("fnattr: loadFromFile(%s) -> %s\n", fnattrPath.c_str(), ok ? "OK" : "FAILED");
    }

    std::vector<TestDataTypes> fieldTypes(numCols, TestDataTypes::UINT64);
    std::vector<std::string> fieldNames;
    for (size_t i = 0; i < numCols; ++i)
    {
        fieldNames.push_back("f" + std::to_string(i));
    }
    const auto schema = InputFormatterTestUtil::createSchema(fieldTypes, fieldNames);
    std::optional<Schema> projection;
    if (not projectField.empty())
    {
        projection = InputFormatterTestUtil::createSchema({TestDataTypes::UINT64}, {projectField});
    }

    /// data ring per N-cell: at least cfg.ring (D+8), and large enough that N*ring*B >= NES_LAT_WS_MIB
    const auto ringFor
        = [&](const size_t N) { return std::max(cfg.ring, (cfg.wsBytes + N * cfg.rawBufBytes - 1) / (N * cfg.rawBufBytes)); };
    size_t maxRing = cfg.ring;
    for (const size_t N : streamCounts)
    {
        maxRing = std::max(maxRing, ringFor(N));
    }
    const Master master = loadMaster(filePath, cfg.rawBufBytes, maxRing);
    const auto cpus = allowedCpus();
    std::printf(
        "formatter-latency: arm=%s k=%zu D=%zu gens=%zu fmt=%zuKiB cut=%s W=%zu pin=%d ring=%zu x %zuKiB (%.1f MiB, %zu rows) warmup=%.1fs "
        "window=%.1fs reps=%u spin=%luus cap=%zu cpus=%zu heavy=%.3fx%u burst=%.1f/%.1fms\n",
        cfg.label.c_str(),
        cfg.k,
        cfg.D,
        VIEW_GENERATIONS,
        cfg.formattedBytes / 1024,
        cfg.cut.c_str(),
        cfg.workers,
        cfg.pin ? 1 : 0,
        master.slotBytes.size(),
        cfg.rawBufBytes / 1024,
        static_cast<double>(master.totalBytes) / (1024.0 * 1024.0),
        master.rows,
        cfg.warmupS,
        cfg.windowS,
        cfg.reps,
        static_cast<unsigned long>(cfg.spinNs / 1000),
        cfg.cap,
        cpus.size(),
        cfg.heavyFrac,
        cfg.heavyMult,
        static_cast<double>(cfg.burstActiveNs) / 1e6,
        static_cast<double>(cfg.burstIdleNs) / 1e6);
    std::printf(
        "file=%s cols=%zu project=%s fnattr=%s parser=%s\n",
        std::filesystem::path(filePath).filename().c_str(),
        numCols,
        projectField.c_str(),
        fnattrPath.empty() ? "off" : "on",
        uint64Parser.c_str());
    const double rowsPerByte = static_cast<double>(master.rows) / static_cast<double>(master.totalBytes);

    auto wrapPool = BufferManager::create(4096, 16);
    const bool needGang = (cfg.arm == "gshared" || cfg.arm == "gprivate");
    const bool needCuts = (cfg.arm == "chunker");

    /// chunker: per-worker SEQUENTIAL stages (self-contained chunks, no shredder), compiled once
    std::vector<std::shared_ptr<CompiledExecutablePipelineStage>> seqStages;
    auto resultBuffers = std::make_shared<std::vector<std::vector<TupleBuffer>>>(1);
    auto startStopPool = BufferManager::create(cfg.formattedBytes, 64);
    if (needCuts)
    {
        for (size_t w = 0; w < cfg.workers; ++w)
        {
            auto stage = makeStage("SequentialCSV", schema, projection, uint64Parser, allowCommas, cfg.formattedBytes);
            TestPipelineExecutionContext startPec(startStopPool, WorkerThreadId(0), PipelineId(0), resultBuffers);
            stage->start(startPec);
            seqStages.push_back(std::move(stage));
        }
    }

    std::printf("\n  N | arm      k |   GB/s | bufs/s | p50 us | p99 us | p999 us | max us | strm p99 med/max | cpu s/GB | vcsw/buf | "
                "ivcsw/buf | tuples/row | repeats\n");
    if (not cfg.outCsv.empty() && not std::filesystem::exists(cfg.outCsv))
    {
        std::ofstream o(cfg.outCsv);
        o << "arm,k,N,W,spin_us,cap,rep,window_s,buffers,bytes,GBps,bufs_per_s,p50_us,p99_us,p999_us,max_us,strm_p99_med_us,strm_p99_max_"
             "us,cpu_s_per_GB,vcsw,ivcsw,tuple_ratio,repeats,D,pin,"
             "heavy_frac,heavy_mult,burst_active_ms,burst_idle_ms,light_bufs,heavy_bufs,light_p50_us,light_p99_us,heavy_p50_us,heavy_p99_"
             "us,light_strm_p99_max_us,timer_wakes,timer_late_max_ms\n";
    }

    for (const size_t N : streamCounts)
    {
        /// build N stream contexts: memory + views (+ one compiled stage per stream for the shredder arms)
        std::vector<std::unique_ptr<Stream>> streams;
        streams.reserve(N);
        const uint64_t tBuild0 = nowNs();
        const size_t ringN = ringFor(N);
        /// keep the control-block reuse distance (ring * generations) of the base configuration
        VIEW_GENERATIONS = std::max<size_t>(2, (cfg.ring * cfg.gensBase + ringN - 1) / ringN);
        if (cfg.wsBytes != 0)
        {
            std::printf(
                "  N=%zu: ring=%zu x %zuKiB per stream (working set %.0f MiB), gens=%zu\n",
                N,
                ringN,
                cfg.rawBufBytes / 1024,
                static_cast<double>(N * ringN * cfg.rawBufBytes) / (1024.0 * 1024.0),
                VIEW_GENERATIONS);
        }
        for (size_t i = 0; i < N; ++i)
        {
            auto st = makeStream(i, master, cfg, *wrapPool, needGang, needCuts, ringN);
            if (needCuts && cfg.cut == "index")
            {
                /// the cutter's full structural scan = the real SIMDCSV indexer with DPP's exact configuration
                const InputFormatterDescriptor desc("SIMDCSV", validatedConfig("SIMDCSV", uint64Parser, allowCommas));
                auto tbr = LowerSchemaProvider::lowerSchema(cfg.formattedBytes, schema, MemoryLayoutType::ROW_LAYOUT);
                st->cutIndexer = std::make_unique<SIMDCSVInputFormatIndexer>(desc);
                st->cutMeta = std::make_unique<CSVMetaData>(desc, *tbr);
                st->cutFields = numCols;
            }
            st->costMult = heavyPick(i, cfg.heavyFrac) ? cfg.heavyMult : 1;
            st->phaseOffNs = cfg.bursty() ? splitmix64(0x4255525354ULL ^ i) % cfg.burstPeriodNs() : 0;
            if (not needCuts)
            {
                /// rr: a per-stream SEQUENTIAL stage (no shredder) -- its buffers arrive in order from one thread
                st->stage = makeStage(
                    cfg.arm == "rr" ? "SequentialCSV" : "SIMDCSV", schema, projection, uint64Parser, allowCommas, cfg.formattedBytes);
                TestPipelineExecutionContext startPec(startStopPool, WorkerThreadId(0), PipelineId(0), resultBuffers);
                st->stage->start(startPec);
            }
            streams.push_back(std::move(st));
        }
        const double buildS = static_cast<double>(nowNs() - tBuild0) / 1e9;
        if (cfg.heavyFrac > 0.0)
        {
            /// the heavy set is arm-independent; show the load round-robin pinning (stream mod W) would give each worker:
            /// sum of the cost multipliers of its streams, worst worker over the mean
            std::vector<size_t> heavyPerWorker(cfg.workers, 0);
            std::vector<double> load(cfg.workers, 0.0);
            size_t heavy = 0;
            for (const auto& st : streams)
            {
                heavy += (st->costMult > 1) ? 1 : 0;
                heavyPerWorker[st->id % cfg.workers] += (st->costMult > 1) ? 1 : 0;
                load[st->id % cfg.workers] += st->costMult;
            }
            /// mean over the OCCUPIED workers (N < W leaves workers without a stream)
            const double meanLoad = std::accumulate(load.begin(), load.end(), 0.0) / static_cast<double>(std::min(cfg.workers, N));
            std::printf(
                "  N=%zu: heavy %zu/%zu streams (x%u); rr pinning (id mod W=%zu): heavy per worker max %zu, worst-worker load %.2fx mean\n",
                N,
                heavy,
                N,
                cfg.heavyMult,
                cfg.workers,
                *std::ranges::max_element(heavyPerWorker),
                (meanLoad > 0.0) ? *std::ranges::max_element(load) / meanLoad : 0.0);
        }

        /// formatted-buffer pool: the pec drops output so buffers recycle immediately; size for the number of
        /// concurrent executes (gprivate: N*k threads) + slack, pre-faulted once.
        /// concurrent executes are bounded by THREADS (N*k gang threads or W workers), not by in-flight buffers
        const auto poolBuffers = static_cast<uint32_t>(std::max<size_t>(1024, N * cfg.k * 2 + cfg.workers * 4 + 512));
        auto formattedPool = BufferManager::create(cfg.formattedBytes, poolBuffers);
        {
            std::vector<TupleBuffer> all;
            all.reserve(poolBuffers);
            while (auto buf = formattedPool->getBufferNoBlocking())
            {
                auto mem = buf->getAvailableMemoryArea<char>();
                std::fill(mem.begin(), mem.end(), char{0});
                all.push_back(std::move(*buf));
            }
        }

        for (unsigned rep = 0; rep < cfg.reps; ++rep)
        {
            Run run(cfg, streams, formattedPool, seqStages);
            const RunResult r = run.execute();
            const double gbps = static_cast<double>(r.bytes) / r.windowS / 1e9;
            const double bufsPerS = static_cast<double>(r.buffers) / r.windowS;
            const double cpuPerGB = (r.bytes > 0) ? r.cpuS / (static_cast<double>(r.bytes) / 1e9) : 0.0;
            const double tupleRatio = (r.bytes > 0) ? (static_cast<double>(r.tuples) / static_cast<double>(r.bytes)) / rowsPerByte : 0.0;
            std::printf(
                "%3zu | %-8s %zu | %6.2f | %6.0f | %6.1f | %6.1f | %7.1f | %6.1f | %7.1f / %7.1f | %8.4f | %8.3f | %9.3f | %10.4f | "
                "%lu%s\n",
                N,
                cfg.label.c_str(),
                cfg.k,
                gbps,
                bufsPerS,
                r.p50us,
                r.p99us,
                r.p999us,
                r.maxus,
                r.perStreamP99MedUs,
                r.perStreamP99MaxUs,
                cpuPerGB,
                r.buffers > 0 ? static_cast<double>(r.vcsw) / static_cast<double>(r.buffers) : 0.0,
                r.buffers > 0 ? static_cast<double>(r.ivcsw) / static_cast<double>(r.buffers) : 0.0,
                tupleRatio,
                static_cast<unsigned long>(r.repeats),
                (std::string(rep == 0 ? " (build " + std::to_string(buildS).substr(0, 5) + "s)" : "")
                 + (g_slotWaits.load() != 0 ? " slotwaits=" + std::to_string(g_slotWaits.load())
                            + " max=" + std::to_string(g_slotWaitMaxNs.load() / 1000000) + "ms"
                                            : ""))
                    .c_str());
            if (cfg.heavyFrac > 0.0 || cfg.bursty())
            {
                std::printf(
                    "    | light p50/p99 %.1f/%.1f us (%lu bufs) | heavy p50/p99 %.1f/%.1f us (%lu bufs) | light strm p99 max %.1f us | "
                    "bursts %lu, late max %.2f ms\n",
                    r.lightP50us,
                    r.lightP99us,
                    static_cast<unsigned long>(r.lightBuffers),
                    r.heavyP50us,
                    r.heavyP99us,
                    static_cast<unsigned long>(r.heavyBuffers),
                    r.lightStreamP99MaxUs,
                    static_cast<unsigned long>(r.timerWakes),
                    static_cast<double>(r.timerLateMaxNs) / 1e6);
            }
            std::fflush(stdout);
            if (not cfg.outCsv.empty())
            {
                std::ofstream o(cfg.outCsv, std::ios::app);
                o << cfg.label << ',' << cfg.k << ',' << N << ',' << cfg.workers << ',' << cfg.spinNs / 1000 << ',' << cfg.cap << ',' << rep
                  << ',' << r.windowS << ',' << r.buffers << ',' << r.bytes << ',' << gbps << ',' << bufsPerS << ',' << r.p50us << ','
                  << r.p99us << ',' << r.p999us << ',' << r.maxus << ',' << r.perStreamP99MedUs << ',' << r.perStreamP99MaxUs << ','
                  << cpuPerGB << ',' << r.vcsw << ',' << r.ivcsw << ',' << tupleRatio << ',' << r.repeats << ',' << cfg.D << ','
                  << (cfg.pin ? 1 : 0) << ',' << cfg.heavyFrac << ',' << cfg.heavyMult << ','
                  << static_cast<double>(cfg.burstActiveNs) / 1e6 << ',' << static_cast<double>(cfg.burstIdleNs) / 1e6 << ','
                  << r.lightBuffers << ',' << r.heavyBuffers << ',' << r.lightP50us << ',' << r.lightP99us << ',' << r.heavyP50us << ','
                  << r.heavyP99us << ',' << r.lightStreamP99MaxUs << ',' << r.timerWakes << ','
                  << static_cast<double>(r.timerLateMaxNs) / 1e6 << '\n';
            }
        }

        /// tear down this N cell: stop stages, free stream memory
        for (auto& st : streams)
        {
            if (st->stage)
            {
                TestPipelineExecutionContext stopPec(startStopPool, resultBuffers);
                st->stage->stop(stopPec);
            }
            st->ring.clear();
            st->stagePool.reset();
            std::free(st->mem);
            st->mem = nullptr;
        }
    }
    for (auto& s : seqStages)
    {
        TestPipelineExecutionContext stopPec(startStopPool, resultBuffers);
        s->stop(stopPec);
    }
    return 0;
}
