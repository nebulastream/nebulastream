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

#include <TCPSource.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <tuple>
#include <variant>

#include <boost/asio/buffer.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/system/detail/error_code.hpp>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Variant.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// While the buffer is empty the source can wait for data indefinitely, but it must keep noticing a stop request,
/// so each blocking wait is sliced.
constexpr auto STOP_POLL_INTERVAL = std::chrono::milliseconds(100);

/// Message plus category and value (e.g. "Connection refused [system:111]"), so logs show which errno or resolver code it was.
std::string describe(const boost::system::error_code& error)
{
    return error.message() + " [" + error.category().name() + ":" + std::to_string(error.value()) + "]";
}

/// NOLINTBEGIN(bugprone-throwing-static-initialization,cert-err58-cpp)
const ConfigField<std::optional<std::string>> SOCKET_HOST{
    Identifier::parse("SOCKET_HOST"),
    "The host from which to read.",
    [](const ConfigLiteral& literal) -> std::expected<std::optional<std::string>, Exception>
    {
        if (std::holds_alternative<std::monostate>(literal))
        {
            return std::nullopt;
        }
        return NES::tryGetOr<std::string>(literal, expectedType<std::string>());
    }};

const ConfigField<std::optional<uint32_t>> SOCKET_PORT{
    Identifier::parse("SOCKET_PORT"),
    "The port of the host from which to read, any number from 1 to including 65535",
    [](const ConfigLiteral& literal) -> std::expected<std::optional<uint32_t>, Exception>
    {
        if (std::holds_alternative<std::monostate>(literal))
        {
            return std::nullopt;
        }
        return NES::tryGetOr<int64_t>(literal, expectedType<int64_t>())
            .and_then(narrowConfigValue<int64_t, uint32_t, 65535, 1>)
            .transform([](const uint32_t value) { return std::optional{value}; });
    }};

/// Upper bound on how long a partial buffer is held back. Zero flushes as soon as bytes arrive.
/// An empty buffer waits for data, peer close, or stop independently of this interval.
const ConfigField<uint32_t> FLUSH_INTERVAL_MS{
    Identifier::parse("FLUSH_INTERVAL_MS"),
    "How long to hold a partially filled buffer, in whole milliseconds.",
    [](const ConfigLiteral& literal)
    { return NES::tryGetOr<int64_t>(literal, expectedType<int64_t>()).and_then(narrowConfigValue<int64_t, uint32_t>); },
    100};

const ConfigField<uint32_t> CONNECT_TIMEOUT_MS{
    Identifier::parse("CONNECT_TIMEOUT_MS"),
    "How long to wait for the TCP connection, in whole milliseconds. DNS resolution is not included.",
    [](const ConfigLiteral& literal)
    { return NES::tryGetOr<int64_t>(literal, expectedType<int64_t>()).and_then(narrowConfigValue<int64_t, uint32_t>); },
    5000};

/// Additional safety marker to prevent people from not setting host and port by accident outside of the systests
const ConfigField<bool> OVERWRITEABLE_HOST_AND_PORT{
    Identifier::parse("OVERWRITEABLE_HOST_AND_PORT"), "Internal marker, do NOT overwrite this", false};
/// NOLINTEND(bugprone-throwing-static-initialization,cert-err58-cpp)

}

Schema<QualifiedErasedConfigField, Ordered> TCPSource::getConfigSchema()
{
    return createConfigSchema(
        Identifier::parse("TCP_SOURCE"), SOCKET_HOST, SOCKET_PORT, FLUSH_INTERVAL_MS, CONNECT_TIMEOUT_MS, OVERWRITEABLE_HOST_AND_PORT);
}

std::expected<TCPSourceConfig, Exception> TCPSourceConfig::fromConfig(const InstantiatedConfig& config)
{
    bool overwriteableHostAndPort = config.get(OVERWRITEABLE_HOST_AND_PORT);
    if (!overwriteableHostAndPort)
    {
        /// HOST and PORT are optional config fields so that the systests can set them, but at the end of the day they still need to be set.
        if (!config.get(SOCKET_HOST).has_value())
        {
            return std::unexpected{InvalidConfigParameter("TCPSource: Missing required parameter: SOCKET_HOST")};
        }
        if (!config.get(SOCKET_PORT).has_value())
        {
            return std::unexpected{InvalidConfigParameter("TCPSource: Missing required parameter: SOCKET_PORT")};
        }
    }
    else
    {
        if (config.get(SOCKET_HOST).has_value() and config.get(SOCKET_PORT).has_value())
        {
            overwriteableHostAndPort = false;
        }
        if (config.get(SOCKET_HOST).has_value() != config.get(SOCKET_PORT).has_value())
        {
            return std::unexpected{InvalidConfigParameter("TCPSource: HOST and PORT must be set together or left out in systests")};
        }
    }

    /// In the overwriteable (systest) case host/port may be absent; the systest data adaptors
    /// (provideInlineData/provideFileData) fill in the mock server's host and port later.
    return TCPSourceConfig{
        .socketDestination = overwriteableHostAndPort
            ? std::optional<SocketDestination>{}
            /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) - non-overwriteable configs always carry host and port
            : SocketDestination{.socketHost = config.get(SOCKET_HOST).value(), .socketPort = config.get(SOCKET_PORT).value()},
        .flushIntervalMs = config.get(FLUSH_INTERVAL_MS),
        .connectTimeoutMs = config.get(CONNECT_TIMEOUT_MS)};
}

TCPSource::TCPSource(const TCPSourceConfig& config)
    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) - the endpoint is supplied before construction
    : host(config.socketDestination.value().socketHost)
    , port(config.socketDestination.value().socketPort) /// NOLINT(bugprone-unchecked-optional-access)
    , connectTimeout(config.connectTimeoutMs)
    , flushInterval(config.flushIntervalMs)
{
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    str << "\nTCPSource(";
    str << "\n  host: " << host;
    str << "\n  port: " << port;
    str << "\n  connectTimeoutMs: " << connectTimeout.count();
    str << "\n  flushIntervalMs: " << flushInterval.count();
    str << ")\n";
    return str;
}

void TCPSource::open(std::shared_ptr<AbstractBufferProvider>)
{
    boost::system::error_code resolveError;
    /// The analyzer flags a virtual call in the destructor of Asio's resolver thread pool, inside Boost and not in this code.
    boost::asio::ip::tcp::resolver resolver{ioContext}; /// NOLINT(clang-analyzer-optin.cplusplus.VirtualCall)
    const auto endpoints = resolver.resolve(host, std::to_string(port), resolveError);
    if (resolveError.failed())
    {
        throw CannotOpenSource("TCPSource cannot resolve {}:{}: {}", host, port, describe(resolveError));
    }

    /// Asio's synchronous connect has no deadline, so connect asynchronously and
    /// pump the io_context for at most `connectTimeout`.
    std::optional<boost::system::error_code> connectResult;
    boost::asio::async_connect(
        socket,
        endpoints,
        [&connectResult](const boost::system::error_code& error, const boost::asio::ip::tcp::endpoint&) { connectResult = error; });
    ioContext.restart();
    ioContext.run_for(connectTimeout);
    if (!connectResult.has_value())
    {
        /// Deadline hit with the connect still pending: closing the socket aborts
        /// it; drain the io_context so the handler has run before `connectResult`
        /// goes out of scope.
        boost::system::error_code closeError;
        std::ignore = socket.close(closeError);
        ioContext.run();
        throw CannotOpenSource("TCPSource connect to {}:{} timed out after {}ms", host, port, connectTimeout.count());
    }
    if (connectResult->failed())
    {
        throw CannotOpenSource("TCPSource cannot connect to {}:{}: {}", host, port, describe(*connectResult));
    }
    NES_DEBUG("TCPSource connected to {}:{}", host, port);
}

Source::FillTupleBufferResult TCPSource::fillTupleBuffer(NES::TupleBuffer& tupleBuffer, const std::stop_token& stopToken)
{
    if (peerClosed)
    {
        return FillTupleBufferResult::eos();
    }

    size_t tbOffset = 0;
    const auto tbSize = tupleBuffer.getBufferSize();
    const auto memoryArea = tupleBuffer.getAvailableMemoryArea();
    const auto flushDeadline = std::chrono::steady_clock::now() + flushInterval;

    while (tbOffset < tbSize && !stopToken.stop_requested())
    {
        /// The flush interval only bounds how long bytes already in the buffer
        /// are held back; an empty buffer keeps waiting (TCP silence is not EoS).
        const auto now = std::chrono::steady_clock::now();
        if (tbOffset > 0 && now >= flushDeadline)
        {
            break;
        }
        auto waitUntil = now + STOP_POLL_INTERVAL;
        if (tbOffset > 0)
        {
            waitUntil = std::min(waitUntil, flushDeadline);
        }

        /// Asio's synchronous read has no deadline, so read asynchronously and
        /// pump the io_context until the read completes or the slice elapses.
        boost::system::error_code readError;
        size_t bytesRead = 0;
        bool completed = false;
        socket.async_read_some(
            boost::asio::buffer(memoryArea.subspan(tbOffset).data(), tbSize - tbOffset),
            [&completed, &readError, &bytesRead](const boost::system::error_code& error, const size_t numBytes)
            {
                completed = true;
                readError = error;
                bytesRead = numBytes;
            });
        ioContext.restart();
        ioContext.run_until(waitUntil);
        if (!completed)
        {
            /// Slice elapsed with no data: cancel and drain the io_context so the
            /// (aborted) handler has run before its captures go out of scope.
            boost::system::error_code cancelError;
            std::ignore = socket.cancel(cancelError);
            ioContext.run();
        }

        tbOffset += bytesRead;
        if (readError == boost::asio::error::operation_aborted)
        {
            continue;
        }
        if (readError == boost::asio::error::eof)
        {
            peerClosed = true;
            break;
        }
        if (readError.failed())
        {
            throw CannotReadFromSource("TCPSource read from {}:{} failed: {}", host, port, describe(readError));
        }
    }

    if (tbOffset == 0 && peerClosed)
    {
        return FillTupleBufferResult::eos();
    }
    return FillTupleBufferResult::withBytes(std::min(tbOffset, tbSize));
}

void TCPSource::close()
{
    /// Best-effort teardown: the peer may already have closed or reset the
    /// connection, and that must not fail the query.
    boost::system::error_code shutdownError;
    std::ignore = socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, shutdownError);
    boost::system::error_code closeError;
    std::ignore = socket.close(closeError);
    NES_DEBUG("TCPSource to {}:{} closed", host, port);
}

}
