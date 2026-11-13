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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <expected>
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
#include <variant>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkProvider.hpp>
#include <Util/Strings.hpp>
#include <BackpressureChannel.hpp>
#include <Binder.hpp>
#include <BufferPool.hpp>
#include <ControlChannel.hpp>
#include <ErrorHandling.hpp>
#include <HarnessSinkPEC.hpp>
#include <Reply.hpp>
#include <SessionCommand.hpp>

#include <fmt/format.h>
#include <folly/MPMCQueue.h>

namespace NES::ConnTest
{
namespace
{

constexpr std::chrono::milliseconds WRITE_POLL_INTERVAL{25};

/// Records end with a newline, except possibly the last one.
std::vector<std::string_view> splitRecords(const std::string_view bytes)
{
    std::vector<std::string_view> records;
    std::size_t begin = 0;
    while (begin < bytes.size())
    {
        const auto newline = bytes.find('\n', begin);
        const auto end = newline == std::string_view::npos ? bytes.size() : newline + 1;
        records.push_back(bytes.substr(begin, end - begin));
        begin = end;
    }
    return records;
}

/// The state that persists across the commands of one session.
class SinkSessionState
{
public:
    explicit SinkSessionState(const SinkSessionOptions& options) : options(options), threads(std::max<std::size_t>(options.threads, 1)) { }

    SinkSessionState(const SinkSessionState&) = delete;
    SinkSessionState& operator=(const SinkSessionState&) = delete;
    SinkSessionState(SinkSessionState&&) = delete;
    SinkSessionState& operator=(SinkSessionState&&) = delete;

    /// A session that ends without STOP destroys the sink without stopping it.
    /// The runner ends a session that way only after the scenario failed, and a sink that failed may not be able to stop.
    ~SinkSessionState() = default;

    std::string dispatch(std::string_view line)
    {
        const auto verb = verbOf(line);
        if (verb == "BIND")
        {
            return bind();
        }
        if (verb == "START")
        {
            return start();
        }
        if (verb == "WRITE")
        {
            const auto count = argOf(line);
            if (!count)
            {
                return errorReply(harnessErrorInfo("write", "HarnessProtocol", fmt::format("WRITE needs <n_buffers>: {}", line)));
            }
            return write(*count);
        }
        if (verb == "STOP")
        {
            return stop();
        }
        return errorReply(harnessErrorInfo("dispatch", "HarnessProtocol", fmt::format("unknown command: {}", line)));
    }

private:
    std::string bind()
    {
        const auto& descriptor = ensureBound();
        if (!descriptor)
        {
            return errorReply(descriptor.error());
        }
        /// The config's CREATE SINK statement binds to a named sink, whose schema holds the declared columns in order.
        const auto* named = std::get_if<NamedSinkDescriptor>(&descriptor->getUnderlying());
        INVARIANT(named != nullptr, "a CREATE SINK statement binds to a named sink");
        return bindReply(*named->getSchema());
    }

    /// Binds on the first call only, so a repeated BIND or START returns the same descriptor or the same error.
    const std::expected<SinkDescriptor, ErrorInfo>& ensureBound()
    {
        if (!bound)
        {
            bound.emplace(bindConfig());
        }
        return *bound;
    }

    [[nodiscard]] std::expected<SinkDescriptor, ErrorInfo> bindConfig() const
    {
        const auto sql = slurpFile(options.configFile);
        if (!sql)
        {
            return std::unexpected{
                harnessErrorInfo("bind", "HarnessIO", fmt::format("cannot read config file: {}", options.configFile.string()))};
        }
        auto descriptor = bindSinkStatements(*sql);
        /// The session writes the runner's JSONL, so a config that declares another format would describe bytes that the sink never gets.
        if (descriptor && toUpperCase(descriptor->getFormatType()) != "JSON")
        {
            return std::unexpected{harnessErrorInfo(
                "bind",
                "InvalidConfig",
                fmt::format(
                    "the sink receives JSONL, so its config must declare 'JSON' as OUTPUT_FORMAT, not {}", descriptor->getFormatType()))};
        }
        return descriptor;
    }

    /// Every START builds a new sink from the once-bound descriptor, as the engine does.
    std::string start()
    {
        const auto& descriptor = ensureBound();
        if (!descriptor)
        {
            return errorReply(descriptor.error());
        }
        if (sink)
        {
            return errorReply(harnessErrorInfo("start", "HarnessProtocol", "START while the sink is started"));
        }
        if (!pool)
        {
            try
            {
                pool = makeBufferPool(static_cast<std::uint32_t>(options.bufferSize), static_cast<std::uint32_t>(options.bufferCount));
            }
            catch (const std::exception& ex)
            {
                return errorReply(harnessErrorInfo("start", "HarnessIO", ex.what()));
            }
        }
        if (auto inputError = loadInput())
        {
            return errorReply(*inputError);
        }

        try
        {
            auto [controller, channelListener] = createBackpressureChannel();
            listener.emplace(std::move(channelListener));
            sink = NES::lower(std::move(controller), *descriptor);
            HarnessSinkPEC pec{pool, WorkerThreadId(0), threads, {}};
            sink->start(pec);
        }
        catch (const Exception& ex)
        {
            resetSink();
            return errorReply(connectorErrorInfo("start", ex));
        }
        catch (const std::exception& ex)
        {
            resetSink();
            return errorReply(harnessErrorInfo("start", "HarnessIO", ex.what()));
        }
        return startReply(buffers.size());
    }

    /// WRITE hands the buffers to the sink, so each START makes them again.
    std::optional<ErrorInfo> loadInput()
    {
        buffers.clear();
        nextBuffer = 0;
        if (!options.inputPath)
        {
            return std::nullopt;
        }
        const auto bytes = slurpFile(*options.inputPath);
        if (!bytes)
        {
            return harnessErrorInfo("start", "HarnessIO", fmt::format("cannot read input file: {}", options.inputPath->string()));
        }
        try
        {
            packRecords(splitRecords(*bytes));
        }
        catch (const std::exception& ex)
        {
            return harnessErrorInfo("start", "HarnessIO", ex.what());
        }
        return std::nullopt;
    }

    /// Packs whole records into buffers of at most the buffer size, so that no record is split between two buffers.
    /// A record that is larger than a buffer gets a buffer of its own.
    void packRecords(const std::vector<std::string_view>& records)
    {
        std::string batch;
        const auto flush = [&]
        {
            if (batch.empty())
            {
                return;
            }
            /// Unpooled, because the session holds every buffer of the input at once.
            auto buffer = pool->getUnpooledBuffer(batch.size());
            if (!buffer)
            {
                throw std::runtime_error(fmt::format("cannot allocate a buffer for {} bytes of records", batch.size()));
            }
            std::ranges::copy(batch, buffer->getAvailableMemoryArea<char>().begin());
            /// A sink receives formatted bytes, and the engine stores their count as the number of tuples.
            buffer->setNumberOfTuples(batch.size());
            /// A sink's backpressure handling tells buffers apart by origin, sequence and chunk number, so each buffer has its own.
            buffer->setOriginId(INITIAL_ORIGIN_ID);
            buffer->setSequenceNumber(SequenceNumber(INITIAL_SEQ_NUMBER.getRawValue() + buffers.size()));
            buffer->setChunkNumber(INITIAL_CHUNK_NUMBER);
            buffer->setLastChunk(true);
            buffers.push_back(std::move(*buffer));
            batch.clear();
        };
        for (const auto record : records)
        {
            if (!batch.empty() && batch.size() + record.size() > options.bufferSize)
            {
                flush();
            }
            batch.append(record);
        }
        flush();
    }

    /// Writes the next buffers that START made, or as many as are left.
    std::string write(const std::uint64_t numberOfBuffers)
    {
        if (!sink)
        {
            return errorReply(harnessErrorInfo("write", "HarnessProtocol", "WRITE without a started sink"));
        }
        const auto count = std::min<std::uint64_t>(numberOfBuffers, buffers.size() - nextBuffer);
        std::vector<TupleBuffer> step;
        step.reserve(count);
        for (std::uint64_t written = 0; written < count; ++written)
        {
            step.push_back(std::move(buffers.at(nextBuffer++)));
        }
        if (auto error = writeThroughSink(std::move(step)))
        {
            return errorReply(*error);
        }
        return okReply("write");
    }

    /// The threads take buffers from one queue until it is empty, and a buffer that the sink repeats goes back into the queue.
    /// The queue has room for every buffer of the step, and a buffer only goes back after one was taken, so putting it back never blocks.
    /// The step timeout stops the threads after the buffer that each is writing, so a sink that never accepts a buffer fails the step.
    std::optional<ErrorInfo> writeThroughSink(std::vector<TupleBuffer> step)
    {
        auto queue = std::make_unique<folly::MPMCQueue<TupleBuffer>>(std::max<std::size_t>(step.size(), 1));
        for (auto& buffer : step)
        {
            queue->blockingWrite(std::move(buffer));
        }

        std::stop_source halt;
        std::mutex errorMutex;
        std::optional<ErrorInfo> firstError;
        std::latch done(static_cast<std::ptrdiff_t>(threads));
        bool timedOut = false;
        {
            std::vector<std::jthread> workers;
            workers.reserve(threads);
            for (std::size_t thread = 0; thread < threads; ++thread)
            {
                workers.emplace_back(
                    [&, thread]
                    {
                        HarnessSinkPEC pec{
                            pool,
                            WorkerThreadId(static_cast<std::uint32_t>(thread)),
                            threads,
                            [&queue](const TupleBuffer& buffer) { queue->blockingWrite(TupleBuffer(buffer)); }};
                        TupleBuffer buffer;
                        while (!halt.stop_requested() && queue->readIfNotEmpty(buffer))
                        {
                            if (auto error = execute(buffer, pec))
                            {
                                const std::scoped_lock lock(errorMutex);
                                if (!firstError)
                                {
                                    firstError = std::move(error);
                                }
                                halt.request_stop();
                            }
                        }
                        done.count_down();
                    });
            }
            const auto deadline = std::chrono::steady_clock::now() + SINK_STEP_TIMEOUT;
            while (!done.try_wait())
            {
                if (!timedOut && std::chrono::steady_clock::now() >= deadline)
                {
                    timedOut = true;
                    halt.request_stop();
                }
                std::this_thread::sleep_for(WRITE_POLL_INTERVAL);
            }
        }

        if (firstError)
        {
            return firstError;
        }
        if (timedOut)
        {
            /// A harness error, so that no scenario can list it as an expected connector error.
            return harnessErrorInfo("write", "StepTimeout", fmt::format("write timed out after {} ms", SINK_STEP_TIMEOUT.count()));
        }
        return std::nullopt;
    }

    std::optional<ErrorInfo> execute(const TupleBuffer& buffer, HarnessSinkPEC& pec)
    {
        try
        {
            sink->execute(buffer, pec);
            return std::nullopt;
        }
        catch (const Exception& ex)
        {
            return connectorErrorInfo("write", ex);
        }
        catch (const std::exception& ex)
        {
            return harnessErrorInfo("write", "HarnessIO", ex.what());
        }
    }

    /// A sink with data left to deliver requests that its stop be called again after an interval, as in the engine.
    /// The step timeout bounds the repeats, so a sink that never finishes fails the step instead of hanging the session.
    std::string stop()
    {
        if (!sink)
        {
            return errorReply(harnessErrorInfo("stop", "HarnessProtocol", "STOP without a started sink"));
        }
        HarnessSinkPEC pec{pool, WorkerThreadId(0), threads, {}};
        const auto deadline = std::chrono::steady_clock::now() + SINK_STEP_TIMEOUT;
        std::optional<ErrorInfo> error;
        while (true)
        {
            pec.clearRepeatRequest();
            try
            {
                sink->stop(pec);
            }
            catch (const Exception& ex)
            {
                error = connectorErrorInfo("stop", ex);
                break;
            }
            catch (const std::exception& ex)
            {
                error = harnessErrorInfo("stop", "HarnessIO", ex.what());
                break;
            }
            if (!pec.repeatRequested())
            {
                break;
            }
            if (std::chrono::steady_clock::now() >= deadline)
            {
                error = harnessErrorInfo(
                    "stop", "StepTimeout", fmt::format("the sink did not finish stopping within {} ms", SINK_STEP_TIMEOUT.count()));
                break;
            }
            std::this_thread::sleep_for(pec.repeatInterval());
        }
        resetSink();
        if (error)
        {
            return errorReply(*error);
        }
        return okReply("stop");
    }

    void resetSink()
    {
        sink.reset();
        listener.reset();
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): non-owning view into runSinkSession's options; the state's lifetime is strictly contained by the loop's.
    const SinkSessionOptions& options;
    std::size_t threads;
    std::optional<std::expected<SinkDescriptor, ErrorInfo>> bound;
    std::shared_ptr<AbstractBufferProvider> pool;
    /// The buffers that START made from the input, and the next one that WRITE hands to the sink.
    std::vector<TupleBuffer> buffers;
    std::size_t nextBuffer = 0;
    /// The harness holds the listener in place of a source.
    /// Declared before the sink, so that the sink, which holds the controller, is destroyed first.
    std::optional<BackpressureListener> listener;
    std::unique_ptr<Sink> sink;
};

}

bool runSinkSession(const SinkSessionOptions& options, ControlChannel& channel)
{
    SinkSessionState state(options);
    while (true)
    {
        const auto line = channel.readLine();
        if (!line)
        {
            return true;
        }
        if (line->empty())
        {
            continue; /// Blank keep-alive line.
        }
        if (!channel.writeLine(state.dispatch(*line)))
        {
            return false;
        }
    }
}

}
