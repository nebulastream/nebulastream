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
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>

#include <boost/asio/buffer.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/error.hpp>
#include <boost/system/error_code.hpp>

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

namespace
{
/// While the buffer is empty the source can wait for data indefinitely, but it
/// must keep noticing a stop request — so each blocking wait is sliced.
constexpr auto STOP_POLL_INTERVAL = std::chrono::milliseconds(100);
}

TCPSource::TCPSource(const SourceDescriptor& sourceDescriptor)
    : host(sourceDescriptor.getFromConfig(ConfigParametersTCPSource::SOCKET_HOST))
    , port(sourceDescriptor.getFromConfig(ConfigParametersTCPSource::SOCKET_PORT))
    , connectTimeout(sourceDescriptor.getFromConfig(ConfigParametersTCPSource::CONNECT_TIMEOUT_MS))
    , flushInterval(sourceDescriptor.getFromConfig(ConfigParametersTCPSource::FLUSH_INTERVAL_MS))
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
    boost::asio::ip::tcp::resolver resolver{ioContext};
    const auto endpoints = resolver.resolve(host, std::to_string(port), resolveError);
    if (resolveError)
    {
        throw CannotOpenSource("TCPSource cannot resolve {}:{}: {}", host, port, resolveError.message());
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
    if (!connectResult)
    {
        /// Deadline hit with the connect still pending: closing the socket aborts
        /// it; drain the io_context so the handler has run before `connectResult`
        /// goes out of scope.
        boost::system::error_code closeError;
        socket.close(closeError);
        ioContext.run();
        throw CannotOpenSource("TCPSource connect to {}:{} timed out after {}ms", host, port, connectTimeout.count());
    }
    if (*connectResult)
    {
        throw CannotOpenSource("TCPSource cannot connect to {}:{}: {}", host, port, connectResult->message());
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
    auto* const data = tupleBuffer.getAvailableMemoryArea().data();
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
            boost::asio::buffer(data + tbOffset, tbSize - tbOffset),
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
            socket.cancel(cancelError);
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
        if (readError)
        {
            throw CannotOpenSource("TCPSource read from {}:{} failed: {}", host, port, readError.message());
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
    socket.shutdown(boost::asio::ip::tcp::socket::shutdown_both, shutdownError);
    boost::system::error_code closeError;
    socket.close(closeError);
    NES_DEBUG("TCPSource to {}:{} closed", host, port);
}

DescriptorConfig::Config TCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCPSource>(std::move(config), NAME);
}

SourceValidationRegistryReturnType RegisterTCPSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return TCPSource::validateAndFormat(std::move(sourceConfig.config));
}

/// The matching declaration in the auto-generated SourceGeneratedRegistrar.inc takes SourceRegistryArguments by value; a
/// const-ref definition here would not match (compile error). The generator owns the calling convention. The function is
/// also reached only by name from the registrar; `static` / anonymous-namespace would hide it from the registrar resolver.
/// NOLINTBEGIN(performance-unnecessary-value-param, misc-use-internal-linkage)
SourceRegistryReturnType SourceGeneratedRegistrar::RegisterTCPSource(SourceRegistryArguments sourceRegistryArguments)
/// NOLINTEND(performance-unnecessary-value-param, misc-use-internal-linkage)
{
    return std::make_unique<TCPSource>(sourceRegistryArguments.sourceDescriptor);
}

}
