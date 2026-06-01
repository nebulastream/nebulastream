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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <fstream>
#include <ios>
#include <memory>
#include <optional>
#include <stop_token>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Binder.hpp>
#include <ControlChannel.hpp>
#include <ErrorHandling.hpp>
#include <Report.hpp>
#include <SessionCommand.hpp>
#include <SourceRegistry.hpp>

#include <fmt/format.h>

namespace NES::ConnTest
{
namespace
{

/// Watchdog poll interval and pool-wait timeout for the ingest loop.
constexpr std::chrono::milliseconds INGEST_POLL_INTERVAL{25};

/// Holds the state that persists across commands within one source
/// session: the once-bound descriptor, the buffer pool, and the current
/// connector (a fresh instance per OPEN).
class SourceSessionState
{
public:
    explicit SourceSessionState(const SourceSessionOptions& options) : options(options) { }

    std::string dispatch(std::string_view line)
    {
        const auto verb = verbOf(line);
        if (verb == "VALIDATE")
        {
            return validate();
        }
        if (verb == "OPEN")
        {
            return open();
        }
        if (verb == "FILL")
        {
            const auto quota = argOf(line);
            if (!quota)
            {
                return errorReplyLine(harnessErrorInfo("fill", "HarnessProtocol", fmt::format("FILL needs a unit count: {}", line)));
            }
            return ingest("fill", quota);
        }
        if (verb == "DRAIN")
        {
            return ingest("drain", std::nullopt);
        }
        if (verb == "CLOSE")
        {
            return close();
        }
        return errorReplyLine(harnessErrorInfo("dispatch", "HarnessProtocol", fmt::format("unknown command: {}", line)));
    }

private:
    std::string validate()
    {
        if (!ensureBound())
        {
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureBound() returns false only after populating bindError.
            return errorReplyLine(*bindError);
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
        if (!sql)
        {
            bindError = harnessErrorInfo("validate", "HarnessIO", fmt::format("cannot read config file: {}", options.configFile.string()));
            return false;
        }
        Report report;
        auto bound = bindSourceStatements(*sql, report);
        if (!bound)
        {
            bindError = report.error.value_or(harnessErrorInfo("validate", "HarnessProtocol", "bind failed without an error"));
            return false;
        }
        descriptor.emplace(std::move(*bound));
        return true;
    }

    /// OPEN: build a *fresh* connector from the cached descriptor and open
    /// it. A new instance every call so an in-process CLOSE→OPEN is a new
    /// connector on the same descriptor (mirrors a SourceThread).
    std::string open()
    {
        if (!ensureBound())
        {
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): ensureBound() returns false only after populating bindError.
            return errorReplyLine(*bindError);
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
                return errorReplyLine(harnessErrorInfo("open", "HarnessIO", ex.what()));
            }
        }
        try
        {
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): descriptor is set when ensureBound() returns true (entered this branch).
            auto created = NES::SourceRegistry::instance().create(descriptor->getSourceType(), NES::SourceRegistryArguments{*descriptor});
            if (!created)
            {
                /// NOLINTNEXTLINE(bugprone-unchecked-optional-access): same — descriptor is set on this path.
                throw UnknownSourceType("unknown source descriptor type: {}", descriptor->getSourceType());
            }
            source = std::move(created.value());
            source->open(pool);
        }
        catch (const Exception& ex)
        {
            source.reset();
            return errorReplyLine(connectorErrorInfo("open", ex));
        }
        catch (const std::exception& ex)
        {
            source.reset();
            return errorReplyLine(harnessErrorInfo("open", "HarnessIO", ex.what()));
        }
        return R"({"ok":true,"phase":"open"})";
    }

    /// FILL <n> / DRAIN: run the ingest loop until either a quota of `n`
    /// native units is observed (FILL) or the connector signals EoS
    /// (DRAIN), whichever the caller asked for. EoS always stops the loop;
    /// the per-step watchdog stops it cooperatively on timeout. Streams
    /// SHA-256 over the observed bytes and, when --observed-path is set,
    /// (over)writes them so RowModel can decode rows for a multiset compare.
    std::string ingest(std::string_view phase, std::optional<std::uint64_t> quota)
    {
        if (!source)
        {
            return errorReplyLine(harnessErrorInfo(phase, "HarnessProtocol", "OPEN before FILL/DRAIN"));
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

        Sha256Digest sha;
        std::uint64_t unitsObserved = 0; ///< native units (bytes, or tuples for NATIVE sources)
        std::uint64_t bytesObserved = 0; ///< bytes actually hashed / written
        bool sawEoS = false;

        std::stop_source stop;
        std::jthread watchdog(
            /// NOLINTNEXTLINE(performance-unnecessary-value-param): std::jthread invokes the callable with a stop_token by value — the standard's conventional signature; the token is shared_ptr-cheap to copy.
            [&stop, stepTimeout = options.stepTimeout](std::stop_token jthreadStop)
            {
                const auto deadline = std::chrono::steady_clock::now() + stepTimeout;
                while (!jthreadStop.stop_requested() && std::chrono::steady_clock::now() < deadline)
                {
                    std::this_thread::sleep_for(INGEST_POLL_INTERVAL);
                }
                if (!jthreadStop.stop_requested())
                {
                    stop.request_stop();
                }
            });
        const auto stopToken = stop.get_token();

        std::optional<ErrorInfo> err;
        while (!stopToken.stop_requested())
        {
            if (quota && unitsObserved >= *quota)
            {
                break;
            }
            auto buf = pool->getBufferWithTimeout(INGEST_POLL_INTERVAL);
            if (!buf.has_value())
            {
                continue;
            }
            auto fillResult = NES::Source::FillTupleBufferResult::eos();
            try
            {
                fillResult = source->fillTupleBuffer(*buf, stopToken);
            }
            catch (const Exception& ex)
            {
                err = connectorErrorInfo(phase, ex);
                break;
            }
            catch (const std::exception& ex)
            {
                err = harnessErrorInfo(phase, "HarnessIO", ex.what());
                break;
            }
            if (fillResult.isEoS())
            {
                sawEoS = true;
                break;
            }

            const auto area = buf->getAvailableMemoryArea<std::byte>();
            /// withBytes() is a byte count for byte-formatter sources and a
            /// tuple count for NATIVE-formatter sources. The quota counts in
            /// that native unit; the observed byte slice is tuples * width
            /// when a native row width was declared.
            const std::size_t units = fillResult.getNumberOfBytes();
            const std::size_t byteLen = options.nativeRowWidth ? std::min<std::size_t>(units * *options.nativeRowWidth, area.size())
                                                               : std::min<std::size_t>(units, area.size());
            sha.update(area.data(), byteLen);
            if (observedOut)
            {
                /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast): ostream::write takes `const char*`; the underlying bytes are `std::byte`, so the bit-pattern reinterpret is unavoidable at the iostream boundary.
                observedOut->write(reinterpret_cast<const char*>(area.data()), static_cast<std::streamsize>(byteLen));
            }
            unitsObserved += units;
            bytesObserved += byteLen;
        }
        watchdog.request_stop();
        if (observedOut)
        {
            observedOut->flush();
        }

        if (err)
        {
            return errorReplyLine(*err);
        }
        /// A timed-out step is always a failure (design §5): the connector
        /// did not deliver the quota / reach EoS in time. Surface it as a
        /// harness-origin error so the generic runner never "expects" it.
        const bool quotaMet = quota && unitsObserved >= *quota;
        if (stopToken.stop_requested() && !quotaMet && !sawEoS)
        {
            return errorReplyLine(harnessErrorInfo(
                phase,
                "StepTimeout",
                fmt::format("{} timed out after {} ms ({} of quota observed)", phase, options.stepTimeout.count(), unitsObserved)));
        }
        return fmt::format(
            R"({{"ok":true,"phase":"{}","count":{},"bytes":{},"sha":"{}","eos":{}}})",
            phase,
            unitsObserved,
            bytesObserved,
            sha.finalizeHex(),
            sawEoS ? "true" : "false");
    }

    std::string close()
    {
        if (!source)
        {
            return errorReplyLine(harnessErrorInfo("close", "HarnessProtocol", "CLOSE without an open source"));
        }
        try
        {
            source->close();
        }
        catch (const Exception& ex)
        {
            source.reset();
            return errorReplyLine(connectorErrorInfo("close", ex));
        }
        catch (const std::exception& ex)
        {
            source.reset();
            return errorReplyLine(harnessErrorInfo("close", "HarnessIO", ex.what()));
        }
        source.reset();
        return R"({"ok":true,"phase":"close"})";
    }

    /// NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members): non-owning view into runSourceSession's options; the state's lifetime is strictly contained by the loop's.
    const SourceSessionOptions& options;
    std::optional<SourceDescriptor> descriptor;
    std::optional<ErrorInfo> bindError;
    std::shared_ptr<NES::AbstractBufferProvider> pool;
    std::unique_ptr<NES::Source> source;
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
            return true; /// EOF on the command channel — graceful shutdown.
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
