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

/// Formatter-path worker SCALING benchmark (standalone; isolated from the gtest suite).
///
/// Motivation: in the full query engine, formatter worker scaling is entangled with the source
/// producer threads and the engine's task-admission queue, so the SequenceShredder's own
/// contention is hidden. This benchmark removes both: it pre-fills an MPMC queue
/// (MultiThreadedTestTaskQueue) with formatter tasks over the REAL compiled Scan(InputFormatter)->
/// Emit pipeline, then drains it with N worker threads. There are NO producer threads competing for
/// cores and NO engine machinery -- so the throughput-vs-threads curve isolates how the formatter +
/// SequenceShredder scale, and the LOCK_FREE-vs-LOCKING shredder A/B is directly measurable.
///
/// Strong scaling: a fixed set of raw buffers (one file) is drained by N threads; speedup =
/// time(1) / time(N). Each (mode, threads, rep) gets a FRESH compiled stage so the shredder starts
/// clean (its compile happens in the queue ctor, OUTSIDE the timed drain). The timed window measures
/// the drain ALONE: worker threads are created first and park on a start gate, the clock is armed, then
/// the gate opens -- so thread-creation cost (which grows with N) is excluded, as is the trailing
/// eps->stop(). Each (mode, threads) cell also runs one DISCARDED warmup pass before the timed reps to
/// fault in the pools and warm caches, so a short (large-thread-count) window is not skewed by cold start.
///
/// Build + run in the dev container:
///   cmake --build {build} --target formatter-scaling-benchmark
///   {build}/nes-input-formatters/tests/Benchmark/formatter-scaling-benchmark \
///       [file.csv] [rawBufKiB=128] [maxBuffers=0(all)] [reps=3] [threads=1,2,4,8] [drain=mttq] [cols=5]
///       [projectField=-] [fnattrYaml=-] [shredderMode=both]
///
/// Positional args:
///   1 file.csv     input CSV (N comma-separated UINT64 columns; must match `cols`)
///   2 rawBufKiB    raw buffer size in KiB (shredder cost scales with buffer COUNT, so shrink to stress it)
///   3 maxBuffers   cap on #buffers processed (0 = all)
///   4 reps         repetitions per (mode, threads) cell; median reported
///   5 threads      comma-separated worker counts, e.g. 1,2,4,8,14
///   6 drain        mttq | perthread | lean | leannoemit | static   (see drainMode below)
///   7 cols         number of UINT64 columns (1 = cheap parse exposes the shredder; 5 = parse-heavy)
///   8 projectField  a single column to PROJECT to (e.g. "f1"); formatter still indexes all cols but
///                   only this field is read downstream (- = no projection, parse all cols)
///   9 fnattrYaml    path to the invoke-mode YAML (testing/configurations/invokeModeConfGlobalFnAttr.yaml)
///                   -> stamps parse proxies pure so unread (projected-away) parses DCE (- = off).
///                   NB: projection + fnattr together = engine "SELECT <field>"; either alone parses all.
///  10 shredderMode  which sequence_shredder_mode(s) to sweep: LOCK_FREE | LOCKING | both (default both).
///                   Pass LOCK_FREE to skip the LOCKING A/B when only lock-free throughput is wanted.
/// Each run sweeps the selected sequence_shredder_mode(s) and prints, per thread count: throughput
/// (GB/s of input), speedup vs the first thread count, and the repeat rate.
/// (DIAG_* lines on stderr come from the TMP per-phase timers in the formatter src; grep them out.)

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <latch>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <folly/MPMCQueue.h>

#include <DataTypes/Schema.hpp>
#include <Nautilus/Interface/BufferRef/LowerSchemaProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

#include <InputFormatterTestUtil.hpp>
#include <InputFormatterValidationProvider.hpp>
#include <InvokeConfiguration.hpp>
#include <TestTaskQueue.hpp>

namespace
{
using namespace NES;
using InputFormatterTestUtil::TestDataTypes;

/// Counts buffer repeats (spanning-tuple neighbor not yet processed -> re-index + re-parse). Reset
/// per (mode, threads) row; reveals whether the small-buffer collapse is driven by repeat re-work.
std::atomic<uint64_t> g_repeats{0};

/// A pec that DROPS emitted buffers (returns success without storing -> the output buffer's refcount
/// falls to zero and it recycles immediately). Mirrors the engine's DefaultPEC otherwise. Used to
/// isolate whether the small-buffer scaling collapse lives in the emit/output path (allocation +
/// storage + accumulation) vs the parse/read path. TestPipelineExecutionContext is `final`, so this
/// derives directly from PipelineExecutionContext.
struct DroppingPec final : PipelineExecutionContext
{
    std::shared_ptr<AbstractBufferProvider> bm;
    WorkerThreadId threadId;
    PipelineId pipelineId;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> operatorHandlers;
    std::vector<std::unique_ptr<TupleBuffer>> pinnedBuffers;
    std::function<void()> repeatTaskCallback;

    DroppingPec(std::shared_ptr<AbstractBufferProvider> bufferManager, const WorkerThreadId threadId, const PipelineId pipelineId)
        : bm(std::move(bufferManager)), threadId(threadId), pipelineId(pipelineId)
    {
    }

    bool emitBuffer(const TupleBuffer&, ContinuationPolicy) override { return true; } /// drop

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

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override
    {
        if (repeatTaskCallback)
        {
            repeatTaskCallback();
        }
    }

    void setRepeatTaskCallback(std::function<void()> callback) { repeatTaskCallback = std::move(callback); }
};

/// Load the whole CSV file into raw TupleBuffers via a File source (same path SmallFilesTest uses).
std::vector<TupleBuffer>
loadRawBuffers(SourceCatalog& sourceCatalog, const std::string& filePath, const Schema& schema, size_t sizeOfRawBuffers)
{
    const auto fileSize = std::filesystem::file_size(filePath);
    const auto numExpected = (fileSize / sizeOfRawBuffers) + static_cast<uint64_t>(fileSize % sizeOfRawBuffers != 0);

    auto sourceBufferPool = BufferManager::create(sizeOfRawBuffers, static_cast<uint32_t>(numExpected + 2));
    InputFormatterTestUtil::ThreadSafeVector<TupleBuffer> rawBuffers;
    rawBuffers.reserve(numExpected);

    auto [backpressureController, fileSource]
        = InputFormatterTestUtil::createFileSource(sourceCatalog, filePath, schema, std::move(sourceBufferPool), numExpected + 1);
    fileSource->start(InputFormatterTestUtil::getEmitFunction(rawBuffers));
    rawBuffers.waitForSize(numExpected);
    if (fileSource->tryStop(std::chrono::milliseconds(5000)) != SourceReturnType::TryStopResult::SUCCESS)
    {
        throw std::runtime_error("Failed to stop file source");
    }

    std::vector<TupleBuffer> out;
    rawBuffers.modifyBuffer([&out](const std::vector<TupleBuffer>& v) { out.assign(v.begin(), v.end()); });
    return out;
}

double medianOf(std::vector<double> xs)
{
    std::ranges::sort(xs);
    return xs.at(xs.size() / 2);
}

/// Drain the task queue with N worker threads, each using its OWN formatted-buffer pool (the
/// SequenceShredder inside `eps` is still shared). Isolates shared-buffer-pool allocation contention
/// from the formatter/shredder. Per-thread pools are pre-created OUTSIDE the timed window. Returns
/// wall seconds for the drain. Mirrors MultiThreadedTestTaskQueue::threadFunction otherwise.
double drainPerThreadPools(
    const size_t threads,
    const std::vector<TestPipelineTask>& tasks,
    const std::shared_ptr<ExecutablePipelineStage>& eps,
    const size_t sizeOfFormattedBuffers,
    const uint32_t perThreadBuffers,
    const std::shared_ptr<std::vector<std::vector<TupleBuffer>>>& resultBuffers)
{
    std::vector<std::shared_ptr<BufferManager>> pools;
    pools.reserve(threads);
    for (size_t i = 0; i < threads; ++i)
    {
        pools.push_back(BufferManager::create(sizeOfFormattedBuffers, perThreadBuffers));
    }

    folly::MPMCQueue<TestPipelineTask> queue(tasks.size());
    for (const auto& task : tasks)
    {
        queue.blockingWrite(task);
    }

    {
        auto pec = TestPipelineExecutionContext(pools[0], WorkerThreadId(0), PipelineId(0), resultBuffers);
        eps->start(pec);
    }

    std::latch start(1);
    std::latch done(static_cast<std::ptrdiff_t>(threads));
    std::vector<std::jthread> workers;
    workers.reserve(threads);
    for (size_t i = 0; i < threads; ++i)
    {
        workers.emplace_back(
            [&queue, &pools, &resultBuffers, &start, &done, i]
            {
                start.wait();
                const auto& bufferManager = pools[i];
                TestPipelineTask task{};
                while (queue.readIfNotEmpty(task))
                {
                    auto pec
                        = std::make_shared<TestPipelineExecutionContext>(bufferManager, WorkerThreadId(i), PipelineId(0), resultBuffers);
                    pec->setRepeatTaskCallback([&queue, task] { queue.blockingWrite(task); });
                    task.execute(*pec);
                }
                done.count_down();
            });
    }
    /// Threads exist now; arm the clock and release them so creation cost stays out of the window.
    const auto t0 = std::chrono::steady_clock::now();
    start.count_down();
    done.wait();
    const auto t1 = std::chrono::steady_clock::now();
    {
        auto pec = TestPipelineExecutionContext(pools[0], resultBuffers);
        eps->stop(pec);
    }
    return std::chrono::duration<double>(t1 - t0).count();
}

/// Engine-faithful drain: mirrors ThreadPool::WorkerThread::operator() -- the queue holds plain
/// TupleBuffers (no per-task shared_ptr<eps> copy), the stage is a raw pointer, and the pec is a
/// STACK object constructed per task (like the engine's DefaultPEC), NOT make_shared on the heap.
/// This removes the harness-specific per-task heap alloc + shared-eps refcount churn, so the
/// small-buffer scaling reflects the engine, not the test harness. Shared formatted-buffer pool.
double drainLean(
    const size_t threads,
    const std::vector<TupleBuffer>& buffers,
    ExecutablePipelineStage* eps,
    const size_t sizeOfFormattedBuffers,
    const uint32_t poolBuffers,
    const std::shared_ptr<std::vector<std::vector<TupleBuffer>>>& resultBuffers,
    const bool dropOutput)
{
    auto bufferManager = BufferManager::create(sizeOfFormattedBuffers, poolBuffers);

    folly::MPMCQueue<TupleBuffer> queue(buffers.size());
    for (const auto& buffer : buffers)
    {
        queue.blockingWrite(buffer);
    }

    {
        TestPipelineExecutionContext startPec(bufferManager, WorkerThreadId(0), PipelineId(0), resultBuffers);
        eps->start(startPec);
    }

    std::latch start(1);
    std::latch done(static_cast<std::ptrdiff_t>(threads));
    std::vector<std::jthread> workers;
    workers.reserve(threads);
    for (size_t i = 0; i < threads; ++i)
    {
        workers.emplace_back(
            [&queue, &bufferManager, &resultBuffers, eps, &start, &done, dropOutput, i]
            {
                start.wait();
                TupleBuffer buffer{};
                while (queue.readIfNotEmpty(buffer))
                {
                    const auto repeat = [&queue, &buffer]
                    {
                        g_repeats.fetch_add(1, std::memory_order_relaxed);
                        queue.blockingWrite(buffer);
                    };
                    if (dropOutput)
                    {
                        DroppingPec pec(bufferManager, WorkerThreadId(i), PipelineId(0));
                        pec.setRepeatTaskCallback(repeat);
                        eps->execute(buffer, pec);
                    }
                    else
                    {
                        TestPipelineExecutionContext pec(bufferManager, WorkerThreadId(i), PipelineId(0), resultBuffers);
                        pec.setRepeatTaskCallback(repeat);
                        eps->execute(buffer, pec);
                    }
                }
                done.count_down();
            });
    }
    /// Threads exist now; arm the clock and release them so creation cost stays out of the window.
    const auto t0 = std::chrono::steady_clock::now();
    start.count_down();
    done.wait();
    const auto t1 = std::chrono::steady_clock::now();
    {
        TestPipelineExecutionContext stopPec(bufferManager, resultBuffers);
        eps->stop(stopPec);
    }
    return std::chrono::duration<double>(t1 - t0).count();
}

/// Static-partition drain: NO shared MPMC queue -- each worker owns a CONTIGUOUS block of buffers and
/// processes it in order. Removes both the MPMC dequeue contention AND cross-thread false sharing on
/// adjacent raw-buffer control blocks (adjacent buffers now stay on one thread, except at the few
/// range boundaries). If small-buffer scaling recovers vs `lean`, the collapse was MPMC/false-sharing;
/// if it persists, it is irreducible per-buffer cost (shredder atomics / index setup). Rare repeats
/// (spanning-tuple neighbor not yet processed, only near range boundaries) go to a thread-local retry.
double drainStatic(
    const size_t threads,
    const std::vector<TupleBuffer>& buffers,
    ExecutablePipelineStage* eps,
    const size_t sizeOfFormattedBuffers,
    const uint32_t poolBuffers,
    const std::shared_ptr<std::vector<std::vector<TupleBuffer>>>& resultBuffers)
{
    auto bufferManager = BufferManager::create(sizeOfFormattedBuffers, poolBuffers);
    {
        TestPipelineExecutionContext startPec(bufferManager, WorkerThreadId(0), PipelineId(0), resultBuffers);
        eps->start(startPec);
    }

    const size_t n = buffers.size();
    std::latch start(1);
    std::latch done(static_cast<std::ptrdiff_t>(threads));
    std::vector<std::jthread> workers;
    workers.reserve(threads);
    for (size_t i = 0; i < threads; ++i)
    {
        const size_t begin = (n * i) / threads;
        const size_t finish = (n * (i + 1)) / threads;
        workers.emplace_back(
            [&buffers, &bufferManager, &resultBuffers, eps, &start, &done, i, begin, finish]
            {
                start.wait();
                std::vector<TupleBuffer> pending;
                TupleBuffer current{};
                auto processOne = [&](const TupleBuffer& buffer)
                {
                    current = buffer;
                    TestPipelineExecutionContext pec(bufferManager, WorkerThreadId(i), PipelineId(0), resultBuffers);
                    pec.setRepeatTaskCallback([&pending, &current] { pending.push_back(current); });
                    eps->execute(current, pec);
                };
                for (size_t k = begin; k < finish; ++k)
                {
                    processOne(buffers[k]);
                }
                while (!pending.empty())
                {
                    std::vector<TupleBuffer> again;
                    again.swap(pending);
                    for (const auto& buffer : again)
                    {
                        processOne(buffer);
                    }
                    if (!pending.empty())
                    {
                        std::this_thread::yield();
                    }
                }
                done.count_down();
            });
    }
    /// Threads exist now; arm the clock and release them so creation cost stays out of the window.
    const auto t0 = std::chrono::steady_clock::now();
    start.count_down();
    done.wait();
    const auto t1 = std::chrono::steady_clock::now();
    {
        TestPipelineExecutionContext stopPec(bufferManager, resultBuffers);
        eps->stop(stopPec);
    }
    return std::chrono::duration<double>(t1 - t0).count();
}
}

int main(int argc, char** argv)
{
    NES::Logger::setupLogging("formatter-scaling-benchmark.log", NES::LogLevel::LOG_ERROR);

    const std::string filePath = (argc > 1) ? argv[1] : "nes-systests/testdata/large/formatter/bench_5xUINT64_512m.csv";
    /// rawBuf size in KiB; accepts fractional KiB (strtod) so sub-page sizes work, e.g. 0.5 = 512 bytes.
    const size_t sizeOfRawBuffers = static_cast<size_t>(((argc > 2) ? std::strtod(argv[2], nullptr) : 128.0) * 1024.0);
    const size_t maxBuffers = (argc > 3) ? std::strtoull(argv[3], nullptr, 10) : 0; /// 0 = all
    const unsigned reps = (argc > 4) ? static_cast<unsigned>(std::strtoul(argv[4], nullptr, 10)) : 3U;
    std::vector<size_t> threadCounts;
    if (argc > 5)
    {
        for (std::string s = argv[5];;)
        {
            const auto comma = s.find(',');
            threadCounts.push_back(std::strtoull(s.substr(0, comma).c_str(), nullptr, 10));
            if (comma == std::string::npos)
            {
                break;
            }
            s = s.substr(comma + 1);
        }
    }
    else
    {
        threadCounts = {1, 2, 4, 8};
    }
    /// drainMode: how the MPMC queue is drained --
    ///   "mttq"      (default) MultiThreadedTestTaskQueue: heap make_shared pec + shared_ptr<eps> per task
    ///   "perthread" mttq-style heap pec but each worker has its OWN formatted-buffer pool
    ///   "lean"      engine-faithful: STACK pec per task, plain-buffer queue, raw stage (no heap/eps churn)
    ///   "leannoemit" lean + DROP output buffers (immediate recycle) -- isolates the emit/output path
    ///   "static"    no shared queue: each worker owns a contiguous buffer range (isolates MPMC + false sharing)
    const std::string drainMode = (argc > 6) ? argv[6] : "mttq";
    /// Number of UINT64 columns in the input (must match the CSV). Fewer columns => cheaper parse =>
    /// the SequenceShredder is a larger share of the work, so its lock-free win shows at bigger buffers.
    const size_t numCols = (argc > 7) ? std::strtoull(argv[7], nullptr, 10) : 5;
    /// projectField: a single column name (e.g. "f1") to PROJECT to -- the formatter still indexes all
    /// `numCols`, but only this field is read downstream; combined with fnattr the other parses DCE
    /// (mirrors engine SELECT <field>). "" / "-" = no projection (parse all numCols).
    const std::string projectField = (argc > 8 && std::string(argv[8]) != "-") ? argv[8] : "";
    /// fnattrPath: path to the invoke-mode YAML (e.g. testing/configurations/invokeModeConfGlobalFnAttr.yaml)
    /// that stamps parse proxies readonly/willreturn/nounwind so LLVM can DCE unread parses. "" = off.
    const std::string fnattrPath = (argc > 9 && std::string(argv[9]) != "-") ? argv[9] : "";
    if (not fnattrPath.empty())
    {
        const bool ok = NES::InvokeConfig::instance().loadFromFile(fnattrPath);
        std::printf("fnattr: loadFromFile(%s) -> %s\n", fnattrPath.c_str(), ok ? "OK (global_mode=fnattr)" : "FAILED (default mode)");
    }
    /// shredderMode: which sequence_shredder_mode(s) to sweep -- "LOCK_FREE", "LOCKING", or "both" (default).
    const std::string shredderModeArg = (argc > 10 && std::string(argv[10]) != "-") ? argv[10] : "both";
    const std::vector<std::string> modes = (shredderModeArg == "LOCK_FREE") ? std::vector<std::string>{"LOCK_FREE"}
        : (shredderModeArg == "LOCKING")                                    ? std::vector<std::string>{"LOCKING"}
                                                                            : std::vector<std::string>{"LOCK_FREE", "LOCKING"};
    /// uint64Parser: override the UINT64 field parser via the descriptor (e.g. "FastUINT64") to A/B the
    /// Default vs a fast integer-parser plugin with the producer removed. "" / "-" = DefaultUINT64.
    const std::string uint64Parser = (argc > 11 && std::string(argv[11]) != "-") ? argv[11] : "";

    /// Track the raw buffer size so memory stays ~= input size as we shrink buffers to stress the
    /// shredder (output of the parse is <= input bytes, so it fits a same-size buffer).
    const size_t sizeOfFormattedBuffers = sizeOfRawBuffers;
    std::vector<TestDataTypes> fieldTypes(numCols, TestDataTypes::UINT64);
    std::vector<std::string> fieldNames;
    fieldNames.reserve(numCols);
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

    NES::SourceCatalog sourceCatalog;
    auto rawBuffers = loadRawBuffers(sourceCatalog, filePath, schema, sizeOfRawBuffers);
    if (maxBuffers != 0 && rawBuffers.size() > maxBuffers)
    {
        rawBuffers.resize(maxBuffers);
    }
    size_t totalBytes = 0;
    for (const auto& b : rawBuffers)
    {
        totalBytes += b.getNumberOfTuples(); /// raw buffers store byte count in numberOfTuples
    }

    std::printf(
        "formatter-scaling: file=%s rawBuf=%zuKiB buffers=%zu bytes=%.1fMiB reps=%u\n",
        std::filesystem::path(filePath).filename().c_str(),
        sizeOfRawBuffers / 1024,
        rawBuffers.size(),
        static_cast<double>(totalBytes) / (1024 * 1024),
        reps);
    std::printf(
        "drain mode: %s | project: %s | fnattr: %s | uint64 parser: %s\n",
        drainMode.c_str(),
        projectField.empty() ? "(none, parse all cols)" : projectField.c_str(),
        fnattrPath.empty() ? "off" : "on",
        uint64Parser.empty() ? "DefaultUINT64" : uint64Parser.c_str());

    /// Formatted-buffer pool sized by task count (the emit operator flushes ~one output buffer per
    /// task) with generous headroom for per-thread fragmentation + spanning-tuple buffers.
    const auto numFormattedBuffers = static_cast<uint32_t>(rawBuffers.size() * 2 + 2048);

    for (const auto& mode : modes)
    {
        std::printf(
            "\n=== sequence_shredder_mode = %s (SIMDCSV, %zu-col index, parse=%s) ===\n",
            mode.c_str(),
            numCols,
            projectField.empty() ? "all" : (fnattrPath.empty() ? "all (proj, NO fnattr)" : "1 (projected+fnattr)"));
        std::printf("  threads |   GB/s | speedup\n");

        /// Build a FRESH compiled stage and drain it once with `workerThreads` workers; returns drain
        /// seconds. Stage compile + start() happen here, OUTSIDE every drain's timed window. Shared by
        /// the discarded warmup pass and the measured reps so they are byte-for-byte the same work.
        const auto runOnce = [&](const size_t workerThreads) -> double
        {
            auto resultBuffers = std::make_shared<std::vector<std::vector<TupleBuffer>>>(workerThreads);
            const auto conf = InputFormatterValidationProvider::provide(
                "SIMDCSV", {{"type", "SIMDCSV"}, {"tuple_delimiter", "\n"}, {"field_delimiter", ","}, {"sequence_shredder_mode", mode}});
            if (not conf.has_value())
            {
                throw std::runtime_error("Invalid SIMDCSV indexer config");
            }
            /// SIMDCSV `provide()` validates only the indexer params; the per-field parser is a separate
            /// InputFormatterDescriptor param, so inject the override into the validated config directly.
            auto formatterConfig = conf.value();
            if (not uint64Parser.empty())
            {
                formatterConfig[InputFormatterDescriptor::UINT64_PARSER.name] = uint64Parser;
            }
            auto stage = InputFormatterTestUtil::createInputFormatter(
                formatterConfig, schema, MemoryLayoutType::ROW_LAYOUT, sizeOfFormattedBuffers, /*isCompiled*/ true, projection);

            if (drainMode == "lean" || drainMode == "leannoemit")
            {
                return drainLean(
                    workerThreads,
                    rawBuffers,
                    stage.get(),
                    sizeOfFormattedBuffers,
                    numFormattedBuffers,
                    resultBuffers,
                    /*dropOutput*/ drainMode == "leannoemit");
            }
            if (drainMode == "static")
            {
                return drainStatic(workerThreads, rawBuffers, stage.get(), sizeOfFormattedBuffers, numFormattedBuffers, resultBuffers);
            }
            std::vector<TestPipelineTask> tasks;
            tasks.reserve(rawBuffers.size());
            for (const auto& rawBuffer : rawBuffers)
            {
                tasks.emplace_back(rawBuffer, stage);
            }
            if (drainMode == "perthread")
            {
                const auto perThreadBuffers = static_cast<uint32_t>((tasks.size() / workerThreads) * 3 + 512);
                return drainPerThreadPools(
                    workerThreads, tasks, tasks.front().eps, sizeOfFormattedBuffers, perThreadBuffers, resultBuffers);
            }
            auto formattedBufferManager = BufferManager::create(sizeOfFormattedBuffers, numFormattedBuffers);
            auto queue = std::make_unique<MultiThreadedTestTaskQueue>(workerThreads, tasks, formattedBufferManager, resultBuffers);
            queue->startProcessing();
            queue->waitForCompletion();
            return queue->getElapsedSeconds(); /// drain only -- excludes thread spawn + eps->stop()
        };

        double base = 0.0;
        for (const size_t threads : threadCounts)
        {
            /// One discarded warmup pass: fault in the buffer pools, warm caches, and settle thread
            /// placement so the (now small) timed window is not dominated by first-touch / cold-start costs.
            runOnce(threads);

            std::vector<double> gbs;
            gbs.reserve(reps);
            g_repeats.store(0);
            for (unsigned rep = 0; rep < reps; ++rep)
            {
                gbs.push_back(static_cast<double>(totalBytes) / runOnce(threads) / 1e9);
            }
            const double med = medianOf(gbs);
            if (threads == threadCounts.front())
            {
                base = med;
            }
            const double mn = *std::ranges::min_element(gbs);
            const double mx = *std::ranges::max_element(gbs);
            const double spreadPct = (med > 0.0) ? 100.0 * (mx - mn) / med : 0.0;
            const double repeatsPerPass = static_cast<double>(g_repeats.load()) / reps;
            const double repeatPct = 100.0 * repeatsPerPass / static_cast<double>(rawBuffers.size());
            std::printf(
                "  %7zu | %6.3f | %6.2fx | min..max %6.3f..%6.3f (spread %4.0f%%, n=%u) | repeats/pass=%9.0f (%5.1f%% of buffers)\n",
                threads,
                med,
                med / base,
                mn,
                mx,
                spreadPct,
                reps,
                repeatsPerPass,
                repeatPct);
            std::fflush(stdout);
        }
    }
    return 0;
}
