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

#include <SourceSession.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
#include <future>
#include <ios>
#include <iostream>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <BackpressureChannel.hpp>
#include <Binder.hpp>
#include <ControlChannel.hpp>
#include <ErrorHandling.hpp>
#include <InterceptorSource.hpp>
#include <Reply.hpp>
#include <SessionCommand.hpp>
#include <SourceRegistry.hpp>

#include <fmt/format.h>
#include <folly/MPMCQueue.h>

namespace NES::ConnTest
{
namespace
{

constexpr std::chrono::milliseconds INGEST_POLL_INTERVAL{25};

/// A write into a full queue is sliced so that the source thread keeps noticing a stop request, as in the engine.
constexpr std::chrono::milliseconds QUEUE_WRITE_POLL{100};

/// A limit on the unpooled buffers that a connector may request, not a reservation.
constexpr std::size_t UNPOOLED_MEMORY_BUDGET_IN_BYTES = 256UZ * 1024 * 1024;

/// The buffer manager is sized by a memory budget, not a buffer count.
/// Its rounding only moves bytes from the unpooled to the pooled share, so the pool holds at least the requested count.
std::shared_ptr<BufferManager> makeBufferPool(const std::uint32_t bufferSize, const std::uint32_t bufferCount)
{
    constexpr BufferAlignment alignment{64};
    const auto totalMemory = (static_cast<std::size_t>(bufferSize) * bufferCount) + UNPOOLED_MEMORY_BUDGET_IN_BYTES;
    return BufferManager::create(
        totalMemory,
        static_cast<double>(UNPOOLED_MEMORY_BUDGET_IN_BYTES) / static_cast<double>(totalMemory),
        alignment,
        bufferSize,
        std::make_shared<NesDefaultMemoryAllocator>());
}

/// The state that persists across the commands of one session.
/// Each OPEN starts a source thread whose emit pushes buffers into a bounded queue, and FILL reads from that queue.
class SourceSessionState
{
public:
    explicit SourceSessionState(const SourceSessionOptions& options) : options(options) { }

    SourceSessionState(const SourceSessionState&) = delete;
    SourceSessionState& operator=(const SourceSessionState&) = delete;
    SourceSessionState(SourceSessionState&&) = delete;
    SourceSessionState& operator=(SourceSessionState&&) = delete;

    ~SourceSessionState()
    {
        /// Covers an EOF that arrives without a CLOSE.
        resetSource();
        if (stuck)
        {
            exitStuck();
        }
    }

    /// The source thread did not stop within the step timeout and still uses the session's state.
    [[nodiscard]] bool isStuck() const { return stuck; }

    /// Nothing that the stuck source thread uses can be torn down safely, so the harness exits without unwinding.
    [[noreturn]] static void exitStuck()
    {
        std::cerr << "conntest-harness: the source did not stop within the step timeout, exiting\n";
        std::_Exit(SOURCE_STUCK_EXIT_CODE);
    }

    std::string dispatch(std::string_view line)
    {
        const auto verb = verbOf(line);
        if (verb == "BIND")
        {
            return bind();
        }
        if (verb == "OPEN")
        {
            return open();
        }
        if (verb == "FILL")
        {
            const auto counts = twoArgsOf(line);
            if (!counts)
            {
                return errorReply(harnessErrorInfo("fill", "HarnessProtocol", fmt::format("FILL needs <n_rows> <n_bytes>: {}", line)));
            }
            /// A byte source counts what it delivers in bytes, so the byte count is the quota and the row count is unused.
            return ingest("fill", counts->second);
        }
        if (verb == "CLOSE")
        {
            return close();
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
        return okReply("bind");
    }

    /// Binds on the first call only, so a repeated BIND or OPEN returns the same descriptor or the same error.
    const std::expected<SourceDescriptor, ErrorInfo>& ensureBound()
    {
        if (!bound)
        {
            if (const auto sql = slurpFile(options.configFile))
            {
                bound.emplace(bindSourceStatements(*sql));
            }
            else
            {
                bound.emplace(
                    std::unexpected{
                        harnessErrorInfo("bind", "HarnessIO", fmt::format("cannot read config file: {}", options.configFile.string()))});
            }
        }
        return *bound;
    }

    std::string open()
    {
        const auto& descriptor = ensureBound();
        if (!descriptor)
        {
            return errorReply(descriptor.error());
        }

        resetSource();
        if (stuck)
        {
            return errorReply(harnessErrorInfo("open", "StepTimeout", stopTimeoutMessage()));
        }

        if (!pool)
        {
            try
            {
                pool = makeBufferPool(static_cast<std::uint32_t>(options.bufferSize), static_cast<std::uint32_t>(options.bufferCount));
            }
            catch (const std::exception& ex)
            {
                return errorReply(harnessErrorInfo("open", "HarnessIO", ex.what()));
            }
        }

        queue = std::make_unique<folly::MPMCQueue<TupleBuffer>>(std::max<std::size_t>(options.bufferCount, 1));
        eos.store(false);

        auto [channelController, listener] = createBackpressureChannel();
        controller = std::move(channelController);
        /// The source blocks after open() until the first FILL, which comes after the runner has seeded the backend.
        controller->applyPressure();

        std::unique_ptr<NES::Source> inner;
        try
        {
            const auto sourceFactory = NES::SourceRegistry::instance().find(descriptor->getSourceType());
            if (!sourceFactory)
            {
                throw UnknownSourceType("unknown source descriptor type: {}", descriptor->getSourceType());
            }
            inner = (*sourceFactory)(NES::SourceRegistryArguments{*descriptor});
        }
        catch (const Exception& ex)
        {
            resetSource();
            return errorReply(connectorErrorInfo("open", ex));
        }
        catch (const std::exception& ex)
        {
            resetSource();
            return errorReply(harnessErrorInfo("open", "HarnessIO", ex.what()));
        }

        /// open() runs on the source thread, so the wrapper fulfils this promise to give OPEN a synchronous result.
        /// The wrapper also records a read error with its real code before the source thread wraps it.
        errorSlot = std::make_shared<ConnectorErrorSlot>();
        reading = std::make_shared<std::atomic<bool>>(false);
        partialReads = std::make_shared<PartialReadLog>();
        auto openedPromise = std::make_shared<std::promise<void>>();
        auto openedFuture = openedPromise->get_future();
        auto wrapped = std::make_unique<InterceptorSource>(std::move(inner), std::move(openedPromise), errorSlot, reading, partialReads);

        /// The handle is built here, not by the source provider, so that the wrapper can be injected.
        /// The in-flight limit is not enforced here; the bounded queue applies the backpressure.
        handle = std::make_unique<SourceHandle>(
            std::move(listener), NES::OriginId(1), NES::SourceRuntimeConfiguration{options.bufferCount}, pool, std::move(wrapped));
        handle->start(makeEmitFunction());

        if (openedFuture.wait_for(options.stepTimeout) != std::future_status::ready)
        {
            resetSource();
            return errorReply(
                harnessErrorInfo("open", "StepTimeout", fmt::format("open timed out after {} ms", options.stepTimeout.count())));
        }
        try
        {
            openedFuture.get();
        }
        catch (const Exception& ex)
        {
            resetSource();
            return errorReply(connectorErrorInfo("open", ex));
        }
        catch (const std::exception& ex)
        {
            resetSource();
            return errorReply(harnessErrorInfo("open", "HarnessIO", ex.what()));
        }

        return okReply("open");
    }

    /// Data buffers go into the bounded queue with a blocking but cancellable write, which keeps real backpressure.
    /// EoS and errors bypass the queue, because the error alternative is not default-constructible and cannot be a queue element.
    SourceReturnType::EmitFunction makeEmitFunction()
    {
        return
            [this](NES::OriginId, SourceReturnType::SourceReturnType ret, const std::stop_token& stopToken) -> SourceReturnType::EmitResult
        {
            if (auto* data = std::get_if<SourceReturnType::Data>(&ret))
            {
                auto buffer = std::move(data->buffer);
                /// The write consumes the buffer only when it succeeds, so moving it again after a timeout is safe.
                /// NOLINTNEXTLINE(bugprone-use-after-move)
                while (!queue->tryWriteUntil(std::chrono::steady_clock::now() + QUEUE_WRITE_POLL, std::move(buffer)))
                {
                    if (stopToken.stop_requested())
                    {
                        return SourceReturnType::EmitResult::STOP_REQUESTED;
                    }
                }
                return SourceReturnType::EmitResult::SUCCESS;
            }
            if (std::holds_alternative<SourceReturnType::EoS>(ret))
            {
                eos.store(true);
                return SourceReturnType::EmitResult::SUCCESS;
            }
            if (auto* error = std::get_if<SourceReturnType::Error>(&ret))
            {
                /// Covers errors that the wrapper did not record.
                /// The slot keeps its first value, so an error that the wrapper recorded with its real code takes precedence.
                errorSlot->set(std::move(error->ex));
                return SourceReturnType::EmitResult::SUCCESS;
            }
            return SourceReturnType::EmitResult::SUCCESS;
        };
    }

    /// The observed bytes are the runner's only check of source correctness.
    static void processObservedBuffer(const TupleBuffer& buffer, std::optional<std::ofstream>& observedOut, std::uint64_t& bytesObserved)
    {
        /// For a byte source the source thread stores the byte count as the tuple count.
        const auto area = buffer.getAvailableMemoryArea<char>();
        const auto count = std::min<std::size_t>(buffer.getNumberOfTuples(), area.size());
        if (observedOut)
        {
            observedOut->write(area.data(), static_cast<std::streamsize>(count));
        }
        bytesObserved += count;
    }

    /// A source may legitimately end before the quota is met, which is reported with eos set, not as an error.
    /// A quota of 0 returns once the connector has entered a read, so that a scenario can stop a connector that waits for data.
    /// The reply lists how long each read that returned a partly filled buffer took, so the runner can check the flush interval.
    /// NOLINTNEXTLINE(readability-function-cognitive-complexity): one state machine, splitting it would spread the FILL logic over several functions
    std::string ingest(std::string_view phase, std::uint64_t quota)
    {
        if (!handle)
        {
            return errorReply(harnessErrorInfo(phase, "HarnessProtocol", "OPEN before FILL"));
        }

        /// The runner has seeded the backend by now, so the source may start reading.
        /// Releasing again on a later FILL has no effect.
        if (controller)
        {
            controller->releasePressure();
        }

        std::optional<std::ofstream> observedOut;
        if (options.observedPath)
        {
            observedOut.emplace(*options.observedPath, std::ios::binary | std::ios::trunc);
            if (!observedOut->good())
            {
                return errorReply(
                    harnessErrorInfo(phase, "HarnessIO", fmt::format("cannot open observed-path: {}", options.observedPath->string())));
            }
        }

        std::uint64_t bytesObserved = 0;
        bool sawEoS = false;
        bool timedOut = false;
        std::optional<ErrorInfo> err;

        const auto deadline = std::chrono::steady_clock::now() + options.stepTimeout;
        while (true)
        {
            if (bytesObserved >= quota && (quota > 0 || reading->load()))
            {
                break;
            }
            if (auto connectorError = errorSlot->peek())
            {
                err = connectorErrorInfo(phase, *connectorError);
                break;
            }
            if (std::chrono::steady_clock::now() >= deadline)
            {
                timedOut = true;
                break;
            }

            TupleBuffer buf;
            if (!queue->tryReadUntil(std::chrono::steady_clock::now() + INGEST_POLL_INTERVAL, buf))
            {
                /// EoS is emitted after the last data buffer, so once it is set the queue holds everything that is left.
                if (eos.load())
                {
                    while (queue->read(buf))
                    {
                        processObservedBuffer(buf, observedOut, bytesObserved);
                    }
                    sawEoS = true;
                    break;
                }
                continue;
            }
            processObservedBuffer(buf, observedOut, bytesObserved);
        }

        if (observedOut)
        {
            observedOut->flush();
        }
        if (err)
        {
            return errorReply(*err);
        }
        /// A timeout is reported as a harness error so that no scenario can list it as an expected connector error.
        const bool quotaMet = bytesObserved >= quota && (quota > 0 || reading->load());
        if (timedOut && !quotaMet && !sawEoS)
        {
            return errorReply(harnessErrorInfo(
                phase,
                "StepTimeout",
                fmt::format("{} timed out after {} ms ({} of quota observed)", phase, options.stepTimeout.count(), bytesObserved)));
        }
        return fillReply(phase, sawEoS, partialReads->take());
    }

    std::string close()
    {
        if (!handle)
        {
            return errorReply(harnessErrorInfo("close", "HarnessProtocol", "CLOSE without an open source"));
        }
        /// The connector's close() runs on the source thread while it stops.
        /// An error from that close() is not reported, because no source scenario expects one.
        resetSource();
        if (stuck)
        {
            /// A harness error, so that a connector that ignores the stop request fails every scenario that stops it.
            return errorReply(harnessErrorInfo("close", "StepTimeout", stopTimeoutMessage()));
        }
        return okReply("close");
    }

    [[nodiscard]] std::string stopTimeoutMessage() const
    {
        return fmt::format("the source did not stop within {} ms; its connector ignores the stop request", options.stepTimeout.count());
    }

    /// The stop has the step timeout, so a connector that ignores the stop request fails the step instead of hanging the session.
    /// When it times out, the source thread keeps running and uses the members below, so they stay alive and `stuck` is set.
    /// The controller must outlive the source's listener, so the source stops before the controller is dropped.
    void resetSource()
    {
        if (handle)
        {
            if (handle->tryStop(options.stepTimeout) == SourceReturnType::TryStopResult::TIMEOUT)
            {
                stuck = true;
                return;
            }
            handle.reset();
        }
        controller.reset();
        queue.reset();
        eos.store(false);
        errorSlot.reset();
        reading.reset();
        partialReads.reset();
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): non-owning view into runSourceSession's options; the state's lifetime is strictly contained by the loop's.
    const SourceSessionOptions& options;
    std::optional<std::expected<SourceDescriptor, ErrorInfo>> bound;
    std::shared_ptr<NES::AbstractBufferProvider> pool;

    /// Written by the emit on the source thread and read by FILL on the command thread.
    std::unique_ptr<folly::MPMCQueue<TupleBuffer>> queue;
    std::atomic<bool> eos{false};
    std::shared_ptr<ConnectorErrorSlot> errorSlot;
    /// Set by the wrapper when the connector enters fillTupleBuffer.
    std::shared_ptr<std::atomic<bool>> reading;
    std::shared_ptr<PartialReadLog> partialReads;
    bool stuck = false;

    /// The harness holds the controller in place of a sink.
    /// Declared before the handle so that it outlives the source's listener.
    std::optional<BackpressureController> controller;

    /// Declared last so that it is destroyed first: the source thread stops before the members it uses go away.
    std::unique_ptr<SourceHandle> handle;
};

}

bool runSourceSession(const SourceSessionOptions& options, ControlChannel& channel)
{
    SourceSessionState state(options);
    while (true)
    {
        const auto line = channel.readLine();
        if (!line)
        {
            return true; /// The destructor stops the source.
        }
        if (line->empty())
        {
            continue; /// Blank keep-alive line.
        }
        const bool written = channel.writeLine(state.dispatch(*line));
        if (state.isStuck())
        {
            SourceSessionState::exitStuck();
        }
        if (!written)
        {
            return false;
        }
    }
}

}
