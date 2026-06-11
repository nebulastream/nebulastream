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

#include <SinkSession.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <filesystem>
#include <latch>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp> /// NOLINT(misc-include-cleaner) Schema's complete type is needed for member calls via getSchema()
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkProvider.hpp>
#include <Util/Strings.hpp>
#include <BackpressureChannel.hpp>
#include <Binder.hpp>
#include <ControlChannel.hpp>
#include <ErrorHandling.hpp>
#include <Formatter.hpp>
#include <HarnessSinkPEC.hpp>
#include <Report.hpp>
#include <SessionCommand.hpp>

/// CPPTRACE_TRY / CPPTRACE_CATCH come from this header but clang-tidy's misc-include-cleaner
/// doesn't see macro-provided symbols, so it flags both the include and the macro usages.
/// NOLINTNEXTLINE(misc-include-cleaner)
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>

/// The session driver uses a few canonical short loop names — `nl` for a newline
/// position (splitRecords), `i` for the batch loop index, and `t` for the
/// per-worker thread index in the drain. Banding the file keeps these idiomatic;
/// renaming each to a 3+-char name fights the reader.
/// NOLINTBEGIN(readability-identifier-length)
namespace NES::ConnTest
{
namespace
{

/// Watchdog poll interval for the write deadline.
constexpr std::chrono::milliseconds WRITE_WATCHDOG_INTERVAL{25};
/// Sleep between repeated `sink->stop()` calls when the sink requests a stop-repeat.
constexpr std::chrono::milliseconds STOP_REPEAT_INTERVAL{10};

/// Split a byte blob into whole records (bytes up to and including each
/// terminating '\n'; the final record may have none). Used to slice the
/// formatted input into the per-record units a WRITE quota counts.
std::vector<std::string> splitRecords(const std::string_view bytes)
{
    std::vector<std::string> records;
    std::size_t pos = 0;
    while (pos < bytes.size())
    {
        const auto nl = bytes.find('\n', pos);
        const std::size_t end = (nl == std::string_view::npos) ? bytes.size() : nl + 1;
        records.emplace_back(bytes.substr(pos, end - pos));
        pos = end;
    }
    return records;
}

/// Holds the state that persists across commands within one sink session:
/// the once-bound descriptor, the pool, the started sink (+ its backpressure
/// listener), the loaded input records, and a cursor into them.
class SinkSessionState
{
public:
    explicit SinkSessionState(const SinkSessionOptions& options) : options(options), numThreads(std::max<std::size_t>(options.threads, 1))
    {
    }

    SinkSessionState(const SinkSessionState&) = delete;
    SinkSessionState& operator=(const SinkSessionState&) = delete;
    SinkSessionState(SinkSessionState&&) = delete;
    SinkSessionState& operator=(SinkSessionState&&) = delete;

    ~SinkSessionState()
    {
        /// Best-effort teardown if the session ended (EOF) without an
        /// explicit STOP: the ExecutablePipelineStage contract requires a
        /// started sink be stopped.
        if (sink && started && !stopped)
        {
            /// NOLINTNEXTLINE(misc-include-cleaner): CPPTRACE_TRY is a macro provided by <cpptrace/from_current.hpp>; clang-tidy doesn't see the include-symbol link.
            CPPTRACE_TRY
            {
                HarnessSinkPEC pec{pool, WorkerThreadId(0), PipelineId(0), numThreads};
                sink->stop(pec);
            }
            /// NOLINTNEXTLINE(misc-include-cleaner): CPPTRACE_CATCH is a macro provided by <cpptrace/from_current.hpp>; clang-tidy doesn't see the include-symbol link.
            CPPTRACE_CATCH(...)
            {
            }
        }
    }

    std::string dispatch(std::string_view line)
    {
        const auto verb = verbOf(line);
        if (verb == "BIND")
        {
            return bind();
        }
        if (verb == "START")
        {
            /// Optional <offset>: resume WRITE at that buffer (a crashed session's
            /// committed prefix). Absent ⇒ 0, the whole input from the start.
            return start(argOf(line).value_or(0));
        }
        if (verb == "WRITE")
        {
            const auto quota = argOf(line);
            if (!quota)
            {
                return errorReplyLine(harnessErrorInfo("write", "HarnessProtocol", fmt::format("WRITE needs a buffer count: {}", line)));
            }
            return write(*quota);
        }
        if (verb == "STOP")
        {
            return stop();
        }
        return errorReplyLine(harnessErrorInfo("dispatch", "HarnessProtocol", fmt::format("unknown command: {}", line)));
    }

private:
    std::string bind()
    {
        if (not ensureBound())
        {
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureBound() returns false only after setting bindError.
            return errorReplyLine(bindError.value());
        }
        /// Report the engine's native row width (fixed region incl. null flags) so the
        /// runner can size its "N buffers" dataset as N * (buffer_size / row_width) without
        /// re-deriving the layout in Python — the one engine-owned layout number it keeps.
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureBound() returning true engaged descriptor.
        const auto rowWidth = descriptor->getSchema()->getSizeOfSchemaInBytes();
        return fmt::format(R"({{"ok":true,"phase":"bind","row_width":{}}})", rowWidth);
    }

    bool ensureBound()
    {
        if (descriptor)
        {
            return true;
        }
        if (bindError)
        {
            return false;
        }
        const auto sql = slurpFile(options.configFile);
        if (not sql)
        {
            bindError = harnessErrorInfo("bind", "HarnessIO", fmt::format("cannot read config file: {}", options.configFile.string()));
            return false;
        }
        Report report;
        auto bound = bindSinkStatements(*sql, report);
        if (!bound)
        {
            bindError = report.error.value_or(harnessErrorInfo("bind", "HarnessProtocol", "bind failed without an error"));
            return false;
        }
        descriptor.emplace(std::move(*bound));
        return true;
    }

    /// START [<offset>]: lower the descriptor to a fresh Sink and start() it.
    /// `startOffset` positions the WRITE cursor past the buffers a prior session
    /// already committed before it died — the sink-side analog of a source
    /// naturally resuming its backend read position. 0 writes the whole input.
    std::string start(std::uint64_t startOffset)
    {
        if (!ensureBound())
        {
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureBound() returns false only after setting bindError.
            return errorReplyLine(bindError.value());
        }
        if (started)
        {
            return errorReplyLine(harnessErrorInfo("start", "HarnessProtocol", "sink already started"));
        }
        if (!pool)
        {
            try
            {
                pool = NES::BufferManager::create(
                    static_cast<std::uint32_t>(options.bufferSize), static_cast<std::uint32_t>(options.bufferCount));
            }
            catch (const std::exception& ex)
            {
                return errorReplyLine(harnessErrorInfo("start", "HarnessIO", ex.what()));
            }
        }
        /// Materialize the input as a sequence of TupleBuffers up front (native:
        /// the `.nes` buffers; formatted: the records batched by buffer size), so
        /// START can report the buffer count and WRITE can step whole buffers.
        if (!ensureLoaded())
        {
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureLoaded() returns false only after setting inputError.
            return errorReplyLine(inputError.value());
        }
        if (startOffset > units.size())
        {
            return errorReplyLine(harnessErrorInfo(
                "start", "HarnessProtocol", fmt::format("START offset {} exceeds input buffer count {}", startOffset, units.size())));
        }
        unitCursor = static_cast<std::size_t>(startOffset);
        try
        {
            auto [controller, channelListener] = createBackpressureChannel();
            listener.emplace(std::move(channelListener));
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureBound() above guarantees descriptor is engaged.
            sink = NES::lower(std::move(controller), descriptor.value());
        }
        catch (const Exception& ex)
        {
            return errorReplyLine(connectorErrorInfo("start", ex));
        }
        catch (const std::exception& ex)
        {
            return errorReplyLine(harnessErrorInfo("start", "HarnessIO", ex.what()));
        }

        HarnessSinkPEC startPec{pool, WorkerThreadId(0), PipelineId(0), numThreads};
        try
        {
            sink->start(startPec);
        }
        catch (const Exception& ex)
        {
            sink.reset();
            return errorReplyLine(connectorErrorInfo("start", ex));
        }
        catch (const std::exception& ex)
        {
            sink.reset();
            return errorReplyLine(harnessErrorInfo("start", "HarnessIO", ex.what()));
        }
        started = true;
        return fmt::format(R"({{"ok":true,"phase":"start","n_buffers":{}}})", units.size());
    }

    /// WRITE <n>: drain the next `n` buffers THROUGH the sink on `numThreads`
    /// concurrent workers (every sink scenario's TSan signal). One TupleBuffer
    /// is the unit — matching the engine's sink interface — so the runner steps
    /// whole buffers (e.g. one per WRITE for the outage scenarios) rather than a
    /// record count. The buffers were materialized at START. Reply carries
    /// `units_written` = records in the delivered buffers (the runner steps WRITEs by the
    /// START `n_buffers`, so it needs no end-of-stream flag).
    std::string write(std::uint64_t numberOfBuffers)
    {
        if (!started || !sink)
        {
            return errorReplyLine(harnessErrorInfo("write", "HarnessProtocol", "START before WRITE"));
        }

        std::vector<TupleBuffer> batch;
        std::uint64_t recordsWritten = 0;
        while (numberOfBuffers > 0 && unitCursor < units.size())
        {
            recordsWritten += unitRecords[unitCursor];
            batch.push_back(std::move(units[unitCursor]));
            ++unitCursor;
            --numberOfBuffers;
        }

        if (auto drainError = drainThroughSink(std::move(batch)))
        {
            return errorReplyLine(*drainError);
        }
        return fmt::format(R"({{"ok":true,"phase":"write","units_written":{}}})", recordsWritten);
    }

    /// Drain `units` THROUGH the sink on `numThreads` concurrent workers, with
    /// a per-step watchdog enforcing options.stepTimeout (every sink scenario's
    /// TSan signal). Returns nullopt on success, or the first connector/harness
    /// error — or a StepTimeout error — on failure. Queue capacity equals the
    /// unit count: a backpressure re-enqueue only follows a dequeue, so the
    /// in-queue count never exceeds capacity and blockingWrite cannot deadlock.
    /// The multi-worker drain loop with per-thread try/catch and a watchdog is inherently branchy; extracting helpers
    /// would scatter the step's control flow across functions for no gain.
    /// NOLINTNEXTLINE(readability-function-cognitive-complexity)
    std::optional<ErrorInfo> drainThroughSink(std::vector<TupleBuffer> units)
    {
        folly::MPMCQueue<TupleBuffer> queue(std::max<std::size_t>(units.size(), 1));
        for (auto& unit : units)
        {
            queue.blockingWrite(std::move(unit));
        }

        std::atomic<bool> failed{false};
        std::mutex errMx;
        std::optional<ErrorInfo> firstErr;
        std::latch done(static_cast<std::ptrdiff_t>(numThreads));
        std::stop_source stop;

        std::vector<std::jthread> workers;
        workers.reserve(numThreads);
        for (std::size_t t = 0; t < numThreads; ++t)
        {
            workers.emplace_back(
                [&, t]
                {
                    HarnessSinkPEC pec{pool, WorkerThreadId(static_cast<std::uint32_t>(t)), PipelineId(0), numThreads};
                    pec.setRepeatTaskCallback([&](const TupleBuffer& buffer) { queue.blockingWrite(TupleBuffer(buffer)); });

                    TupleBuffer buf;
                    while (!stop.get_token().stop_requested() && queue.readIfNotEmpty(buf))
                    {
                        try
                        {
                            sink->execute(buf, pec);
                        }
                        catch (const Exception& ex)
                        {
                            const std::scoped_lock lock(errMx);
                            if (!firstErr)
                            {
                                firstErr = connectorErrorInfo("write", ex);
                            }
                            failed = true;
                            break;
                        }
                        catch (const std::exception& ex)
                        {
                            const std::scoped_lock lock(errMx);
                            if (!firstErr)
                            {
                                firstErr = harnessErrorInfo("write", "HarnessIO", ex.what());
                            }
                            failed = true;
                            break;
                        }
                    }
                    done.count_down();
                });
        }

        std::jthread watchdog(
            /// NOLINTNEXTLINE(performance-unnecessary-value-param): std::jthread invokes the callable with a stop_token by value — the standard's conventional signature; the token is shared_ptr-cheap to copy.
            [&stop, stepTimeout = options.stepTimeout](std::stop_token jthreadStop)
            {
                const auto deadline = std::chrono::steady_clock::now() + stepTimeout;
                while (!jthreadStop.stop_requested() && std::chrono::steady_clock::now() < deadline)
                {
                    std::this_thread::sleep_for(WRITE_WATCHDOG_INTERVAL);
                }
                if (!jthreadStop.stop_requested())
                {
                    stop.request_stop();
                }
            });

        done.wait();
        watchdog.request_stop();
        workers.clear(); /// join all drain threads

        if (failed)
        {
            return firstErr;
        }
        if (stop.get_token().stop_requested())
        {
            return harnessErrorInfo("write", "StepTimeout", fmt::format("write timed out after {} ms", options.stepTimeout.count()));
        }
        return std::nullopt;
    }

    /// STOP: the repeat-aware stop loop
    std::string stop()
    {
        if (!started || !sink)
        {
            return errorReplyLine(harnessErrorInfo("stop", "HarnessProtocol", "STOP without a started sink"));
        }
        HarnessSinkPEC stopPec{pool, WorkerThreadId(0), PipelineId(0), numThreads};
        std::optional<ErrorInfo> err;
        while (true)
        {
            stopPec.clearRepeatRequest();
            try
            {
                sink->stop(stopPec);
            }
            catch (const Exception& ex)
            {
                err = connectorErrorInfo("stop", ex);
                break;
            }
            catch (const std::exception& ex)
            {
                err = harnessErrorInfo("stop", "HarnessIO", ex.what());
                break;
            }
            if (!stopPec.repeatRequested())
            {
                break;
            }
            std::this_thread::sleep_for(STOP_REPEAT_INTERVAL);
        }
        stopped = true;
        sink.reset();
        if (err)
        {
            return errorReplyLine(*err);
        }
        return R"({"ok":true,"phase":"stop"})";
    }

    /// Materialize the input into `units` (the buffer sequence WRITE steps) plus
    /// the per-buffer record count `unitRecords`. Called once at START so it can
    /// report the buffer count. A "Native" sink (e.g. ODBCSink) decodes the
    /// runner-supplied JSONL into native row-layout buffers via the engine's own
    /// JSON input formatter (`Formatter::decodeJsonl`); a formatted sink (e.g.
    /// MQTTSink, JSONL on the wire) batches the records by buffer size and carries
    /// the bytes through unparsed (in the real engine, formatting is lowered out and
    /// query-compiled). An empty inputPath is the `empty` scenario — no buffers.
    bool ensureLoaded()
    {
        if (loaded)
        {
            return !inputError;
        }
        loaded = true;
        if (options.inputPath.empty())
        {
            return true; /// empty scenario: no buffers
        }
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureLoaded() runs only after START's ensureBound() engaged descriptor.
        const SinkDescriptor& boundDescriptor = descriptor.value();
        try
        {
            if (NES::toUpperCase(boundDescriptor.getFormatType()) == "NATIVE")
            {
                const auto jsonl = slurpFile(options.inputPath);
                if (!jsonl)
                {
                    inputError
                        = harnessErrorInfo("start", "HarnessIO", fmt::format("cannot read input file: {}", options.inputPath.string()));
                    return false;
                }
                /// Decode the runner's JSONL into native buffers via the engine's own JSON
                /// input formatter, sized so the emit packs `buffer_size / row_width` tuples
                /// per buffer (exactly the runner's rows_per_buffer) -> the scenario's N
                /// buffers. `decodePool` backs them and is held for the session's lifetime.
                auto decoded = Formatter::decodeJsonl(*boundDescriptor.getSchema(), options.bufferSize, *jsonl);
                decodePool = std::move(decoded.pool);
                units = std::move(decoded.buffers);
                unitRecords.reserve(units.size());
                for (const auto& unit : units)
                {
                    unitRecords.push_back(unit.getNumberOfTuples());
                }
            }
            else
            {
                const auto bytes = slurpFile(options.inputPath);
                if (!bytes)
                {
                    inputError
                        = harnessErrorInfo("start", "HarnessIO", fmt::format("cannot read input file: {}", options.inputPath.string()));
                    return false;
                }
                buildFormattedUnits(splitRecords(*bytes));
            }
        }
        catch (const std::exception& ex)
        {
            inputError = harnessErrorInfo("start", "HarnessIO", ex.what());
            return false;
        }
        return true;
    }

    /// Pack whole records into buffer-sized formatted units. A record is never
    /// torn across a buffer boundary, so each unit is a clean set of records —
    /// the boundary the buffer-granular outage scenarios verify against. The byte
    /// payload is what the sink consumes (numberOfTuples = byte count); the record
    /// count is tracked separately in `unitRecords` for the WRITE reply.
    void buildFormattedUnits(const std::vector<std::string>& records)
    {
        std::string current;
        std::uint64_t currentRecords = 0;
        const auto flush = [&]
        {
            if (current.empty())
            {
                return;
            }
            /// Unpooled, sized exactly to the batch: the formatted units are all held
            /// in `units` at once, so a pooled allocation would deadlock the pool once
            /// the count is reached. Sizing here also lets a single oversized record
            /// pass through as its own unit.
            auto bufOpt = pool->getUnpooledBuffer(current.size());
            if (!bufOpt)
            {
                throw std::runtime_error(fmt::format("failed to allocate an unpooled buffer for a {}-byte record batch", current.size()));
            }
            auto buf = std::move(bufOpt.value());
            std::memcpy(buf.getAvailableMemoryArea<char>().data(), current.data(), current.size());
            buf.setNumberOfTuples(current.size());
            units.push_back(std::move(buf));
            unitRecords.push_back(currentRecords);
            current.clear();
            currentRecords = 0;
        };
        for (const auto& record : records)
        {
            if (!current.empty() && current.size() + record.size() > options.bufferSize)
            {
                flush();
            }
            current.append(record);
            ++currentRecords;
        }
        flush();
    }

    const SinkSessionOptions& options;
    std::size_t numThreads;
    std::optional<SinkDescriptor> descriptor;
    std::optional<ErrorInfo> bindError;
    std::shared_ptr<NES::AbstractBufferProvider> pool;
    /// Backs the native `units` decoded from JSONL (the engine emits into it).
    /// Held for the session's lifetime so those buffers stay valid until WRITE
    /// consumes them; declared before `units` so it outlives them on teardown.
    std::shared_ptr<NES::BufferManager> decodePool;
    /// `listener` is declared before `sink` so `sink` destructs first —
    /// releasing the backpressure controller while the (idle) listener is
    /// still alive matches the channel's "sinks outlive sources" contract.
    std::optional<BackpressureListener> listener;
    std::unique_ptr<NES::Sink> sink;
    /// The input materialized as a TupleBuffer sequence WRITE steps through (G7):
    /// native `.nes` buffers or formatted record-batches. `unitRecords[i]` is the
    /// record count in `units[i]` (= tuples for native; the batched record count
    /// for formatted, where numberOfTuples is the byte payload). `unitCursor` is
    /// the next buffer to deliver.
    std::vector<TupleBuffer> units;
    std::vector<std::uint64_t> unitRecords;
    std::size_t unitCursor{0};
    std::optional<ErrorInfo> inputError;
    bool loaded{false};
    bool started{false};
    bool stopped{false};
};

}

bool runSinkSession(const SinkSessionOptions& options, ControlChannel& channel)
{
    SinkSessionState state(options);
    while (true)
    {
        const auto line = channel.readLine();
        if (not line)
        {
            return true; /// EOF — graceful shutdown (destructor stops the sink).
        }
        if (line->empty())
        {
            continue;
        }
        if (!channel.writeLine(state.dispatch(line.value())))
        {
            return false;
        }
    }
}

}

/// NOLINTEND(readability-identifier-length)
