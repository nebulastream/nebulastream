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
#include <exception>
#include <filesystem>
#include <fstream>
#include <future>
#include <ios>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <utility>
#include <variant>

#include <DataTypes/Schema.hpp> /// NOLINT(misc-include-cleaner) Schema's complete type is needed for member calls via getSchema()
#include <Identifiers/Identifiers.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceReturnType.hpp>
#include <Util/Strings.hpp>
#include <BackpressureChannel.hpp>
#include <Binder.hpp>
#include <ControlChannel.hpp>
#include <ErrorHandling.hpp>
#include <Formatter.hpp>
#include <InterceptorSource.hpp>
#include <Report.hpp>
#include <SessionCommand.hpp>
#include <SourceRegistry.hpp>

#include <fmt/format.h>
#include <folly/MPMCQueue.h>

namespace NES::ConnTest
{
namespace
{

/// Poll granularity for draining the admission queue in FILL.
constexpr std::chrono::milliseconds INGEST_POLL_INTERVAL{25};

/// Retry granularity for the emit's blocking enqueue: when the admission queue is full
/// the source thread re-attempts on this cadence, checking its stop token in between
/// (mirrors the engine's TaskQueue::addAdmissionTaskBlocking).
constexpr std::chrono::milliseconds QUEUE_WRITE_POLL{100};

/// Buffers for the native-source encode pool. The encode of one native buffer emits a
/// parent JSONL buffer plus a few continuation child buffers; the Formatter releases the
/// prior call's before each encode, so this is a small per-call high-water mark.
constexpr std::size_t FORMAT_POOL_BUFFERS = 64;

/// Holds the state that persists across commands within one source session: the
/// once-bound descriptor, the buffer pool, and — per OPEN — a live SourceHandle whose
/// SourceThread drives the (InterceptorSource-wrapped) connector and pushes filled buffers
/// through a custom emit into a bounded MPMC "admission" queue, which FILL drains.
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
        /// Stop the source thread before the queue / signals / controller it touches go
        /// away (best-effort teardown for an EOF without an explicit CLOSE).
        resetSource();
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
                return errorReplyLine(harnessErrorInfo("fill", "HarnessProtocol", fmt::format("FILL needs <n_rows> <n_bytes>: {}", line)));
            }
            /// The runner sends both measures of the seeded data, kind-blind; the source
            /// counts in its own native unit, so pick the matching target: the tuple count
            /// for a NATIVE source, the JSONL byte length for an opaque one.
            return ingest("fill", nativeSource ? counts->first : counts->second);
        }
        if (verb == "CLOSE")
        {
            return close();
        }
        return errorReplyLine(harnessErrorInfo("dispatch", "HarnessProtocol", fmt::format("unknown command: {}", line)));
    }

private:
    std::string bind()
    {
        if (!ensureBound())
        {
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureBound() returns false only after populating bindError.
            return errorReplyLine(*bindError);
        }
        /// Report the engine's native row width (fixed region incl. null flags) so the
        /// runner can size its "N buffers" dataset as N * (buffer_size / row_width) without
        /// re-deriving the layout in Python — the one engine-owned layout number it keeps.
        /// The connector kind never crosses the wire: FILL carries both a row count and a
        /// byte length, and the harness picks the one matching how it counts (see dispatch).
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureBound() returning true engaged descriptor.
        const auto rowWidth = descriptor->getLogicalSource().getSchema()->getSizeOfSchemaInBytes();
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
        if (!sql)
        {
            bindError = harnessErrorInfo("bind", "HarnessIO", fmt::format("cannot read config file: {}", options.configFile.string()));
            return false;
        }
        Report report;
        auto bound = bindSourceStatements(*sql, report);
        if (!bound)
        {
            bindError = report.error.value_or(harnessErrorInfo("bind", "HarnessProtocol", "bind failed without an error"));
            return false;
        }
        descriptor.emplace(std::move(*bound));
        return true;
    }

    /// OPEN: build a *fresh* connector from the cached descriptor, wrap it in an
    /// InterceptorSource (for the synchronous open-completion signal), hand it to a
    /// SourceHandle, and start its SourceThread. The source is gated (backpressure
    /// applied) so it parks right after open() until the first FILL releases it (the
    /// GO that lets it read the now-seeded backend). A fresh instance every call so an
    /// in-process CLOSE->OPEN is a new connector + thread (mirrors the real engine).
    std::string open()
    {
        if (!ensureBound())
        {
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureBound() returns false only after populating bindError.
            return errorReplyLine(*bindError);
        }

        /// Fresh connector + thread per OPEN.
        resetSource();

        if (!pool)
        {
            try
            {
                pool = NES::BufferManager::create(
                    static_cast<std::uint32_t>(options.bufferSize), static_cast<std::uint32_t>(options.bufferCount));
            }
            catch (const std::exception& ex)
            {
                return errorReplyLine(harnessErrorInfo("open", "HarnessIO", ex.what()));
            }
        }

        /// A NATIVE source emits engine tuples (re-encoded to JSONL in FILL); a byte
        /// source carries the runner's JSONL through unchanged.
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): open() ran ensureBound(), engaging descriptor.
        nativeSource = toUpperCase(descriptor->getInputFormatterDescriptor().getInputFormatterType()) == "NATIVE";

        queue = std::make_unique<folly::MPMCQueue<TupleBuffer>>(std::max<std::size_t>(options.bufferCount, 1));
        eos.store(false);

        auto [channelController, listener] = createBackpressureChannel();
        controller = std::move(channelController);
        controller->applyPressure(); /// gate: the source parks after open() until the first FILL (the GO)

        std::unique_ptr<NES::Source> inner;
        try
        {
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): descriptor engaged on this path.
            auto created = NES::SourceRegistry::instance().create(descriptor->getSourceType(), NES::SourceRegistryArguments{*descriptor});
            if (!created)
            {
                /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): same — descriptor engaged.
                throw UnknownSourceType("unknown source descriptor type: {}", descriptor->getSourceType());
            }
            inner = std::move(created.value());
        }
        catch (const Exception& ex)
        {
            resetSource();
            return errorReplyLine(connectorErrorInfo("open", ex));
        }
        catch (const std::exception& ex)
        {
            resetSource();
            return errorReplyLine(harnessErrorInfo("open", "HarnessIO", ex.what()));
        }

        /// The InterceptorSource fulfils this promise the moment inner->open() returns (value)
        /// or throws (exception), giving OPEN a synchronous open-completion signal even
        /// though open() runs on the SourceThread; and it captures a read-time connector
        /// error (with its real code) into errorSlot before SourceThread can wrap it.
        errorSlot = std::make_shared<ConnectorErrorSlot>();
        auto openedPromise = std::make_shared<std::promise<void>>();
        auto openedFuture = openedPromise->get_future();
        auto wrapped = std::make_unique<InterceptorSource>(std::move(inner), std::move(openedPromise), errorSlot);

        /// Inline SourceProvider::lower's body so we can inject the InterceptorSource wrapper:
        /// a SourceHandle owns the SourceThread that drives open() + ingest. inflightBufferLimit
        /// is unused here (our emit, not the engine's RunningSource, throttles via the queue).
        handle = std::make_unique<SourceHandle>(
            std::move(listener), NES::OriginId(1), NES::SourceRuntimeConfiguration{options.bufferCount}, pool, std::move(wrapped));
        handle->start(makeEmitFunction());

        if (openedFuture.wait_for(options.stepTimeout) != std::future_status::ready)
        {
            resetSource();
            return errorReplyLine(
                harnessErrorInfo("open", "StepTimeout", fmt::format("open timed out after {} ms", options.stepTimeout.count())));
        }
        try
        {
            openedFuture.get();
        }
        catch (const Exception& ex)
        {
            resetSource();
            return errorReplyLine(connectorErrorInfo("open", ex));
        }
        catch (const std::exception& ex)
        {
            resetSource();
            return errorReplyLine(harnessErrorInfo("open", "HarnessIO", ex.what()));
        }

        return R"({"ok":true,"phase":"open"})";
    }

    /// The custom emit handed to the SourceThread: Data buffers go into the bounded
    /// admission queue (blocking-but-cancellable, to preserve real backpressure); EoS and
    /// Error travel out-of-band (the variant cannot be a folly queue element — its first
    /// alternative Error{Exception} is not default-constructible).
    SourceReturnType::EmitFunction makeEmitFunction()
    {
        return
            [this](NES::OriginId, SourceReturnType::SourceReturnType ret, const std::stop_token& stopToken) -> SourceReturnType::EmitResult
        {
            if (auto* data = std::get_if<SourceReturnType::Data>(&ret))
            {
                auto buffer = std::move(data->buffer);
                /// folly's tryWriteUntil consumes the value only on a successful enqueue; on a timed-out
                /// retry `buffer` is left intact, so re-moving it on the next round is safe.
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
                /// Fallback for an error InterceptorSource didn't already capture (e.g. a
                /// non-NES exception SourceThread wrapped). First-write-wins, so a
                /// connector error InterceptorSource recorded with its real code takes priority.
                errorSlot->set(std::move(error->ex));
                return SourceReturnType::EmitResult::SUCCESS;
            }
            return SourceReturnType::EmitResult::SUCCESS;
        };
    }

    /// Encode one observed buffer to its JSONL wire form, write it to the observed-path
    /// (the runner's sole source-correctness oracle), and add its native-unit count to
    /// `unitsObserved` (which bounds the FILL quota). Returns an ErrorInfo if the encode
    /// itself fails.
    std::optional<ErrorInfo> processObservedBuffer(
        TupleBuffer buffer, const std::string_view phase, std::optional<std::ofstream>& observedOut, std::uint64_t& unitsObserved) const
    {
        /// getNumberOfTuples() carries the FILL quota unit in both shapes: SourceThread set
        /// it from withBytes() — the tuple count for a NATIVE source, the byte count for a
        /// byte source.
        const std::size_t count = buffer.getNumberOfTuples();
        std::string observed;
        try
        {
            if (nativeSource)
            {
                /// Re-encode engine tuples to JSONL through the engine's own JSON output
                /// formatter; the buffer arrives fully stamped by SourceThread.
                observed = formatter->encodeToText(buffer);
            }
            else
            {
                const auto area = buffer.getAvailableMemoryArea<char>();
                observed.assign(area.data(), std::min<std::size_t>(count, area.size()));
            }
        }
        catch (const Exception& ex)
        {
            return connectorErrorInfo(phase, ex);
        }
        catch (const std::exception& ex)
        {
            return harnessErrorInfo(phase, "HarnessIO", ex.what());
        }
        if (observedOut)
        {
            observedOut->write(observed.data(), static_cast<std::streamsize>(observed.size()));
        }
        unitsObserved += count;
        return std::nullopt;
    }

    /// FILL <n>: drain the admission queue the SourceThread fills in the background
    /// until either a quota of `n` native units is observed or the source signals EoS
    /// with the queue drained (a source that legitimately ends before the quota). The
    /// first call releases the backpressure gate (the GO). A per-step deadline bounds the
    /// wait; a connector error surfaced through the emit is reported as soon as the queue
    /// is exhausted.
    /// NOLINTNEXTLINE(readability-function-cognitive-complexity) one deliberate drain state machine; splitting it would scatter the FILL contract
    std::string ingest(std::string_view phase, std::uint64_t quota)
    {
        if (!handle)
        {
            return errorReplyLine(harnessErrorInfo(phase, "HarnessProtocol", "OPEN before FILL"));
        }

        /// The GO: let the (already-open) source start reading the now-seeded backend.
        /// Idempotent — only the first FILL actually releases.
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
                return errorReplyLine(
                    harnessErrorInfo(phase, "HarnessIO", fmt::format("cannot open observed-path: {}", options.observedPath->string())));
            }
        }

        if (nativeSource && !formatter)
        {
            formatPool = NES::BufferManager::create(
                static_cast<std::uint32_t>(options.bufferSize), static_cast<std::uint32_t>(FORMAT_POOL_BUFFERS));
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): a live handle implies a successful open(), which engaged descriptor.
            formatter = std::make_unique<Formatter>(*descriptor->getLogicalSource().getSchema(), formatPool);
        }

        std::uint64_t unitsObserved = 0; ///< tuples (NATIVE) or bytes (byte source) — bounds the FILL quota
        bool sawEoS = false;
        bool timedOut = false;
        std::optional<ErrorInfo> err;

        const auto deadline = std::chrono::steady_clock::now() + options.stepTimeout;
        while (true)
        {
            if (unitsObserved >= quota)
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
                /// No buffer this round. EoS is emitted only after the last Data, so once it
                /// is set every produced buffer is already enqueued: drain the remainder.
                if (eos.load())
                {
                    while (!err && queue->read(buf))
                    {
                        err = processObservedBuffer(std::move(buf), phase, observedOut, unitsObserved);
                    }
                    sawEoS = !err;
                    break;
                }
                continue;
            }
            err = processObservedBuffer(std::move(buf), phase, observedOut, unitsObserved);
            if (err)
            {
                break;
            }
        }

        if (observedOut)
        {
            observedOut->flush();
        }
        if (err)
        {
            return errorReplyLine(*err);
        }
        /// A timed-out step is always a failure (design §5): the connector did not deliver
        /// the quota / reach EoS in time. Surface it as a harness-origin error so the
        /// generic runner never "expects" it.
        const bool quotaMet = unitsObserved >= quota;
        if (timedOut && !quotaMet && !sawEoS)
        {
            return errorReplyLine(harnessErrorInfo(
                phase,
                "StepTimeout",
                fmt::format("{} timed out after {} ms ({} of quota observed)", phase, options.stepTimeout.count(), unitsObserved)));
        }
        return fmt::format(R"({{"ok":true,"phase":"{}","eos":{}}})", phase, sawEoS ? "true" : "false");
    }

    std::string close()
    {
        if (!handle)
        {
            return errorReplyLine(harnessErrorInfo("close", "HarnessProtocol", "CLOSE without an open source"));
        }
        /// stop() joins the SourceThread; the connector's close() runs in the thread's
        /// SCOPE_EXIT. (A connector close-error there is not separately reportable — no
        /// source scenario expects one.)
        resetSource();
        return R"({"ok":true,"phase":"close"})";
    }

    /// Stop the SourceThread (if any) and tear down the per-OPEN state, in an order that
    /// respects the backpressure channel's "controller outlives listener" contract: stop
    /// the source first, then drop the controller, then the queue / signals.
    void resetSource()
    {
        if (handle)
        {
            handle->stop();
            handle.reset();
        }
        controller.reset();
        queue.reset();
        eos.store(false);
        errorSlot.reset();
        formatter.reset();
        formatPool.reset();
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): non-owning view into runSourceSession's options; the state's lifetime is strictly contained by the loop's.
    const SourceSessionOptions& options;
    std::optional<SourceDescriptor> descriptor;
    std::optional<ErrorInfo> bindError;
    bool nativeSource{false};
    std::shared_ptr<NES::AbstractBufferProvider> pool;

    /// The admission queue + out-of-band signals the custom emit writes from the
    /// SourceThread and FILL reads from the command thread.
    std::unique_ptr<folly::MPMCQueue<TupleBuffer>> queue;
    std::atomic<bool> eos{false};
    /// First connector error (with its real code, captured by InterceptorSource before
    /// SourceThread wraps it). Shared with the InterceptorSource; rebuilt per OPEN.
    std::shared_ptr<ConnectorErrorSlot> errorSlot;

    /// Native-source encode: the JSON output-formatter pipeline + its dedicated pool,
    /// rebuilt per OPEN so its sequence tracking aligns with the new thread's INITIAL.
    std::shared_ptr<NES::BufferManager> formatPool;
    std::unique_ptr<Formatter> formatter;

    /// The harness holds the backpressure controller (the sink's role); it must outlive
    /// the source's listener, so it is declared before `handle` (destructs after it).
    std::optional<BackpressureController> controller;

    /// The live source: a SourceHandle owning a SourceThread driving the InterceptorSource.
    /// Declared LAST so it destructs FIRST — the thread stops before the queue / signals /
    /// controller it uses are torn down.
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
            return true; /// EOF on the command channel — graceful shutdown (destructor stops the source).
        }
        if (line->empty())
        {
            continue; /// tolerate blank keep-alive lines
        }
        if (!channel.writeLine(state.dispatch(*line)))
        {
            return false; /// hard channel error
        }
    }
}

}
