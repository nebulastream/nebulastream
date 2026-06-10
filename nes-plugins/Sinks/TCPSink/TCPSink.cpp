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

#include <TCPSink.hpp>

#include <cerrno>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <sys/socket.h>

#include <boost/asio/buffer.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/error_code.hpp>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/VariableSizedAccess.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

TCPSink::TCPSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , host(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::SOCKET_HOST))
    , port(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::SOCKET_PORT))
    , connectTimeout(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::CONNECT_TIMEOUT_MS))
    , writeTimeout(sinkDescriptor.getFromConfig(ConfigParametersTCPSink::WRITE_TIMEOUT_MS))
{
}

std::ostream& TCPSink::toString(std::ostream& os) const
{
    os << "\nTCPSink(";
    os << "\n  host: " << host;
    os << "\n  port: " << port;
    os << "\n  connectTimeoutMs: " << connectTimeout.count();
    os << "\n  writeTimeoutMs: " << writeTimeout.count();
    os << ")\n";
    return os;
}

void TCPSink::start(PipelineExecutionContext&)
{
    NES_INFO("Opening TCPSink to {}:{}.", host, port);

    boost::system::error_code resolveError;
    boost::asio::ip::tcp::resolver resolver{ioContext};
    const auto endpoints = resolver.resolve(host, std::to_string(port), resolveError);
    if (resolveError)
    {
        throw CannotOpenSink("TCPSink cannot resolve {}:{}: {}", host, port, resolveError.message());
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
        throw CannotOpenSink("TCPSink connect to {}:{} timed out after {}ms", host, port, connectTimeout.count());
    }
    if (*connectResult)
    {
        throw CannotOpenSink("TCPSink cannot connect to {}:{}: {}", host, port, connectResult->message());
    }
    NES_DEBUG("TCPSink connected to {}:{}", host, port);
}

void TCPSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in TCPSink.");

    /// One socket, one writer at a time: concurrent writes would interleave and
    /// corrupt the byte stream.
    const std::scoped_lock lock(writeMutex);
    if (!socket.is_open())
    {
        throw CannotOpenSink("TCPSink to {}:{} is not connected", host, port);
    }

    /// Probe for a peer that closed the connection. A write into a dead socket
    /// "succeeds" into the kernel send buffer and the bytes silently vanish —
    /// TCP only surfaces the error on the write *after* the peer's RST came
    /// back. A sink never expects inbound bytes, so a readable end-of-stream
    /// here means the peer is gone: fail before losing this buffer.
    /// Trade-off: a peer that half-closes its write direction while still
    /// reading is treated as gone — the sink's contract is a one-way byte
    /// stream to a consuming server.
    char probe = 0;
    const auto probeResult = ::recv(socket.native_handle(), &probe, 1, MSG_PEEK | MSG_DONTWAIT);
    if (probeResult == 0)
    {
        throw CannotOpenSink("TCPSink peer {}:{} closed the connection", host, port);
    }
    if (probeResult < 0 && errno != EAGAIN && errno != EWOULDBLOCK)
    {
        const auto probeError = boost::system::error_code(errno, boost::system::system_category());
        throw CannotOpenSink("TCPSink connection to {}:{} failed: {}", host, port, probeError.message());
    }

    /// Gather the already-formatted bytes: the main buffer first, then the
    /// variable-sized child buffers (same layout MQTTSink publishes). The
    /// `getNumberOfTuples()` of a formatted buffer is its byte count. The child
    /// TupleBuffers are kept alive until the write completed — the const_buffers
    /// reference their memory.
    std::vector<TupleBuffer> childBuffers;
    childBuffers.reserve(inputTupleBuffer.getNumberOfChildBuffers());
    std::vector<boost::asio::const_buffer> payload;
    payload.reserve(1 + inputTupleBuffer.getNumberOfChildBuffers());
    const auto data = inputTupleBuffer.getAvailableMemoryArea<char>().first(inputTupleBuffer.getNumberOfTuples());
    payload.emplace_back(data.data(), data.size());
    for (size_t index = 0; index < inputTupleBuffer.getNumberOfChildBuffers(); index++)
    {
        childBuffers.push_back(inputTupleBuffer.loadChildBuffer(VariableSizedAccess::Index(index)));
        const auto childData = childBuffers.back().getAvailableMemoryArea<char>().first(childBuffers.back().getNumberOfTuples());
        payload.emplace_back(childData.data(), childData.size());
    }

    /// Asio's synchronous write has no deadline, so write asynchronously and
    /// pump the io_context for at most `writeTimeout`.
    std::optional<boost::system::error_code> writeResult;
    boost::asio::async_write(
        socket, payload, [&writeResult](const boost::system::error_code& error, const size_t) { writeResult = error; });
    ioContext.restart();
    ioContext.run_for(writeTimeout);
    if (!writeResult)
    {
        /// Deadline hit with the write still pending: the peer stopped draining
        /// the stream. Close to abort the write and drain the io_context so the
        /// handler has run before the payload buffers go out of scope.
        boost::system::error_code closeError;
        socket.close(closeError);
        ioContext.run();
        throw CannotOpenSink("TCPSink write to {}:{} timed out after {}ms", host, port, writeTimeout.count());
    }
    if (*writeResult)
    {
        throw CannotOpenSink("TCPSink write to {}:{} failed: {}", host, port, writeResult->message());
    }
}

void TCPSink::stop(PipelineExecutionContext&)
{
    const std::scoped_lock lock(writeMutex);
    /// Best-effort teardown: shutdown sends FIN (the peer sees a clean
    /// end-of-stream); errors are ignored — the peer may already be gone, and
    /// tearing down a dead connection is not a query failure.
    boost::system::error_code shutdownError;
    socket.shutdown(boost::asio::ip::tcp::socket::shutdown_send, shutdownError);
    boost::system::error_code closeError;
    socket.close(closeError);
    NES_INFO("TCPSink to {}:{} completed.", host, port);
}

DescriptorConfig::Config TCPSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersTCPSink>(std::move(config), NAME);
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkValidationRegistryReturnType RegisterTCPSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return TCPSink::validateAndFormat(std::move(sinkConfig.config));
}

/// NOLINTNEXTLINE(performance-unnecessary-value-param): registry signature fixed by framework.
SinkRegistryReturnType RegisterTCPSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<TCPSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
