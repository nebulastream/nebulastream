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
#include <charconv>
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
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkProvider.hpp>
#include <Util/Strings.hpp>
#include <BackpressureChannel.hpp>
#include <Binder.hpp>
#include <ControlChannel.hpp>
#include <ErrorHandling.hpp>
#include <HarnessSinkPEC.hpp>
#include <Report.hpp>
#include <SessionCommand.hpp>

/// CPPTRACE_TRY / CPPTRACE_CATCH come from this header but clang-tidy's misc-include-cleaner
/// doesn't see macro-provided symbols, so it flags both the include and the macro usages.
/// NOLINTNEXTLINE(misc-include-cleaner)
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <folly/MPMCQueue.h>

/// The session driver walks CSV records and tuple buffers with the canonical
/// scanner short names (`i`/`n` for offset/length, `nl`/`sp` for separator
/// positions, `wt` for the watchdog interval capture, `r`/`c`/`t` for
/// row/column/tuple loop indices). Banding the file keeps the parser idiom
/// intact; renaming each to a 3+-char name fights the reader.
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

/// Batch whole records up to `bufferSize`. A record is never torn across a
/// buffer boundary so a row split mid-buffer would corrupt the round-trip.
/// A single record larger than `bufferSize` becomes its own oversized batch;
/// the caller rejects it.
std::vector<std::string> batchRecords(const std::vector<std::string>& records, std::size_t begin, std::size_t end, std::size_t bufferSize)
{
    std::vector<std::string> batches;
    std::string current;
    for (std::size_t i = begin; i < end; ++i)
    {
        const auto& record = records[i];
        if (!current.empty() && current.size() + record.size() > bufferSize)
        {
            batches.push_back(std::move(current));
            current.clear();
        }
        current.append(record);
    }
    if (!current.empty())
    {
        batches.push_back(std::move(current));
    }
    return batches;
}

/// Parse one CSV record (RFC-4180-ish) into fields. A bare empty field is a
/// SQL NULL (std::nullopt); a quoted empty field `""` is an empty string.
/// Embedded commas and doubled quotes inside a quoted field are handled — the
/// whole point of the native path is that a sink no longer naively splits text.
/// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic): the parser walks `record` by index; bounds-checking subscript on string_view is what the loop already does.
std::vector<std::optional<std::string>> parseCsvRecord(std::string_view record)
{
    while (!record.empty() && (record.back() == '\n' || record.back() == '\r'))
    {
        record.remove_suffix(1);
    }
    std::vector<std::optional<std::string>> fields;
    std::size_t i = 0;
    const std::size_t n = record.size();
    while (true)
    {
        if (i < n && record[i] == '"')
        {
            std::string value;
            ++i;
            while (i < n)
            {
                if (record[i] == '"')
                {
                    if (i + 1 < n && record[i + 1] == '"')
                    {
                        value.push_back('"');
                        i += 2;
                        continue;
                    }
                    ++i;
                    break;
                }
                value.push_back(record[i]);
                ++i;
            }
            fields.emplace_back(std::move(value));
        }
        else
        {
            const std::size_t start = i;
            while (i < n && record[i] != ',')
            {
                ++i;
            }
            const auto raw = record.substr(start, i - start);
            if (raw.empty())
            {
                fields.emplace_back(std::nullopt);
            }
            else
            {
                fields.emplace_back(std::string(raw));
            }
        }
        if (i >= n)
        {
            break;
        }
        /// record[i] is the field separator (or a stray char after a closing
        /// quote); step over it. A trailing comma yields one final NULL field.
        ++i;
        if (i == n && record[i - 1] == ',')
        {
            fields.emplace_back(std::nullopt);
            break;
        }
    }
    return fields;
}

/// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

template <typename T>
T parseInteger(const std::string& text)
{
    T value{};
    const auto* first = text.data();
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic): canonical from_chars idiom; the [data, data+size) range is what the API takes.
    const auto* last = text.data() + text.size();
    const auto [ptr, ec] = std::from_chars(first, last, value);
    if (ec != std::errc{} || ptr != last)
    {
        throw std::runtime_error(fmt::format("native sink fixture: '{}' is not an integer", text));
    }
    return value;
}

/// Convert one CSV cell to the native bytes of `dataType` at `dst`. Variable-
/// sized values are copied into a child buffer of `parent`; the 16-byte field
/// slot receives the VariableSizedAccess pointing at it (matching the engine's
/// row layout, so the sink reads it back exactly as production would).
void writeNativeField(
    const NES::DataType& dataType, const std::string& text, std::byte* dst, NES::TupleBuffer& parent, NES::AbstractBufferProvider& pool)
{
    switch (dataType.type)
    {
        case NES::DataType::Type::INT8: {
            const auto value = parseInteger<int8_t>(text);
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::INT16: {
            const auto value = parseInteger<int16_t>(text);
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::INT32: {
            const auto value = parseInteger<int32_t>(text);
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::INT64: {
            const auto value = parseInteger<int64_t>(text);
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::UINT8: {
            const auto value = parseInteger<uint8_t>(text);
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::UINT16: {
            const auto value = parseInteger<uint16_t>(text);
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::UINT32: {
            const auto value = parseInteger<uint32_t>(text);
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::UINT64: {
            const auto value = parseInteger<uint64_t>(text);
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::FLOAT32: {
            const auto value = static_cast<float>(std::stod(text));
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::FLOAT64: {
            const auto value = std::stod(text);
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::BOOLEAN: {
            const std::byte value{static_cast<unsigned char>((text == "true" || text == "1") ? 1 : 0)};
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::CHAR: {
            const std::byte value{static_cast<unsigned char>(text.empty() ? 0 : text.front())};
            std::memcpy(dst, &value, sizeof(value));
            return;
        }
        case NES::DataType::Type::VARSIZED: {
            const std::size_t length = text.size();
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): fail-fast: an empty optional means the buffer manager is exhausted, which is a fixture-builder bug, not a recoverable condition.
            auto child = pool.getUnpooledBuffer(std::max<std::size_t>(length, 1)).value();
            std::memcpy(child.getAvailableMemoryArea<char>().data(), text.data(), length);
            child.setNumberOfTuples(length);
            const auto index = parent.storeChildBuffer(child);
            const NES::VariableSizedAccess access{index, NES::VariableSizedAccess::Size{length}};
            std::memcpy(dst, &access, sizeof(NES::VariableSizedAccess));
            return;
        }
        case NES::DataType::Type::UNDEFINED:
            throw std::runtime_error("native sink fixture: field of UNDEFINED type cannot be encoded");
    }
}

/// Encode `records[begin, end)` as native row-layout TupleBuffers under
/// `schema`. Packs as many whole tuples as fit per pooled buffer; varsized
/// values spill into child buffers. This is what the engine's native emit would
/// hand the sink — the harness reproduces it so the conn-test exercises the
/// real (non-formatted) path. Throws on a malformed fixture row.
/// NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic): tuple/offset walk over the native byte layout; rewriting with span::subspan obscures the per-field stride.
std::vector<TupleBuffer> buildNativeUnits(
    const std::vector<std::string>& records, std::size_t begin, std::size_t end, const Schema& schema, AbstractBufferProvider& pool)
{
    const std::size_t stride = schema.getSizeOfSchemaInBytes();
    const auto& fields = schema.getFields();
    const std::size_t bufferBytes = pool.getBufferSize();
    const std::size_t tuplesPerBuffer = std::max<std::size_t>(1, bufferBytes / std::max<std::size_t>(stride, 1));

    std::vector<TupleBuffer> units;
    TupleBuffer current;
    std::size_t inCurrent = 0;
    const auto flush = [&]
    {
        if (inCurrent > 0)
        {
            current.setNumberOfTuples(inCurrent);
            units.push_back(std::move(current));
            inCurrent = 0;
        }
    };

    for (std::size_t r = begin; r < end; ++r)
    {
        const auto cells = parseCsvRecord(records[r]);
        if (cells.size() != fields.size())
        {
            throw std::runtime_error(fmt::format("native sink fixture: row has {} fields, schema has {}", cells.size(), fields.size()));
        }
        if (inCurrent == 0)
        {
            current = pool.getBufferBlocking();
            std::memset(current.getAvailableMemoryArea<std::byte>().data(), 0, bufferBytes);
        }
        auto* tuple = current.getAvailableMemoryArea<std::byte>().data() + (inCurrent * stride);
        std::size_t offset = 0;
        for (std::size_t c = 0; c < fields.size(); ++c)
        {
            const auto& dataType = fields[c].dataType;
            auto* fieldPtr = tuple + offset;
            const auto& cell = cells[c];
            const bool isNull = !cell.has_value();
            if (dataType.nullable)
            {
                fieldPtr[0] = isNull ? std::byte{1} : std::byte{0};
            }
            else if (isNull)
            {
                throw std::runtime_error(fmt::format("native sink fixture: NULL in non-nullable field `{}`", fields[c].name));
            }
            if (!isNull)
            {
                writeNativeField(dataType, *cell, fieldPtr + (dataType.nullable ? 1 : 0), current, pool);
            }
            offset += dataType.getSizeInBytesWithNull();
        }
        ++inCurrent;
        if (inCurrent == tuplesPerBuffer)
        {
            flush();
        }
    }
    flush();
    return units;
}

/// NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

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
        if (verb == "VALIDATE")
        {
            return validate();
        }
        if (verb == "START")
        {
            return start();
        }
        if (verb == "WRITE")
        {
            const auto quota = argOf(line);
            if (!quota)
            {
                return errorReplyLine(harnessErrorInfo("write", "HarnessProtocol", fmt::format("WRITE needs a record count: {}", line)));
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
    std::string validate()
    {
        if (not ensureBound())
        {
            return errorReplyLine(bindError.value());
        }
        return R"({"ok":true,"phase":"validate"})";
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
            bindError = harnessErrorInfo("validate", "HarnessIO", fmt::format("cannot read config file: {}", options.configFile.string()));
            return false;
        }
        Report report;
        auto bound = bindSinkStatements(*sql, report);
        if (!bound)
        {
            bindError = report.error.value_or(harnessErrorInfo("validate", "HarnessProtocol", "bind failed without an error"));
            return false;
        }
        descriptor.emplace(std::move(*bound));
        return true;
    }

    /// START: lower the descriptor to a fresh Sink and start() it.
    std::string start()
    {
        if (!ensureBound())
        {
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
        try
        {
            auto [controller, channelListener] = createBackpressureChannel();
            listener.emplace(std::move(channelListener));
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
        return R"({"ok":true,"phase":"start"})";
    }

    /// WRITE <n>: write the next n input records THROUGH the sink, packed
    /// into buffer-sized batches and drained by `numThreads` concurrent
    /// workers (the concurrent scenario's TSan signal). Reply carries
    /// units_written = records actually written.
    std::string write(std::uint64_t numberOfRecords)
    {
        if (!started || !sink)
        {
            return errorReplyLine(harnessErrorInfo("write", "HarnessProtocol", "START before WRITE"));
        }
        if (!ensureInputLoaded())
        {
            return errorReplyLine(inputError.value());
        }

        /// `started` implies ensureBound() succeeded, so the descriptor is set;
        /// bind it once and use the reference for the rest of WRITE.
        const SinkDescriptor& boundDescriptor = descriptor.value();

        const std::size_t begin = cursor;
        const std::size_t end = std::min<std::size_t>(records.size(), begin + numberOfRecords);
        cursor = end;
        const std::uint64_t recordsTaken = end - begin;

        std::vector<TupleBuffer> units;
        /// A sink with no `output_format` (the default "Native") receives the
        /// engine's native row layout, not formatted bytes — so build native
        /// tuples from the CSV fixture under the bound schema. A non-Native sink
        /// (e.g. CSV/JSON, as MQTTSink uses) still gets the formatted bytes
        /// packed whole-record into buffers, the original behaviour.
        /// G7 replaces this CSV→native build with native .nes input via the copied loader.
        if (NES::toUpperCase(boundDescriptor.getFormatType()) == "NATIVE")
        {
            try
            {
                units = buildNativeUnits(records, begin, end, *boundDescriptor.getSchema(), *pool);
            }
            catch (const std::exception& ex)
            {
                return errorReplyLine(harnessErrorInfo("write", "HarnessIO", ex.what()));
            }
        }
        else
        {
            for (auto& chunk : batchRecords(records, begin, end, options.bufferSize))
            {
                if (chunk.size() > pool->getBufferSize())
                {
                    return errorReplyLine(harnessErrorInfo(
                        "write",
                        "HarnessIO",
                        fmt::format("record of {} bytes exceeds buffer-size {}", chunk.size(), pool->getBufferSize())));
                }
                auto buf = pool->getBufferBlocking();
                std::memcpy(buf.getAvailableMemoryArea<char>().data(), chunk.data(), chunk.size());
                buf.setNumberOfTuples(chunk.size());
                units.push_back(std::move(buf));
            }
        }

        if (auto drainError = drainThroughSink(std::move(units)))
        {
            return errorReplyLine(*drainError);
        }
        return fmt::format(R"({{"ok":true,"phase":"write","units_written":{}}})", recordsTaken);
    }

    /// Drain `units` THROUGH the sink on `numThreads` concurrent workers, with
    /// a per-step watchdog enforcing options.stepTimeout (the concurrent scenario's
    /// TSan signal). Returns nullopt on success, or the first connector/harness
    /// error — or a StepTimeout error — on failure. Queue capacity equals the
    /// unit count: a backpressure re-enqueue only follows a dequeue, so the
    /// in-queue count never exceeds capacity and blockingWrite cannot deadlock.
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

    bool ensureInputLoaded()
    {
        if (inputLoaded)
        {
            return !inputError;
        }
        inputLoaded = true;
        if (options.inputPath.empty())
        {
            records.clear(); /// empty scenario: nothing to write
            return true;
        }
        const auto bytes = slurpFile(options.inputPath);
        if (!bytes)
        {
            inputError = harnessErrorInfo("write", "HarnessIO", fmt::format("cannot read input file: {}", options.inputPath.string()));
            return false;
        }
        records = splitRecords(*bytes);
        return true;
    }

    const SinkSessionOptions& options;
    std::size_t numThreads;
    std::optional<SinkDescriptor> descriptor;
    std::optional<ErrorInfo> bindError;
    std::shared_ptr<NES::AbstractBufferProvider> pool;
    /// `listener` is declared before `sink` so `sink` destructs first —
    /// releasing the backpressure controller while the (idle) listener is
    /// still alive matches the channel's "sinks outlive sources" contract.
    std::optional<BackpressureListener> listener;
    std::unique_ptr<NES::Sink> sink;
    std::vector<std::string> records;
    std::size_t cursor{0};
    std::optional<ErrorInfo> inputError;
    bool inputLoaded{false};
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
