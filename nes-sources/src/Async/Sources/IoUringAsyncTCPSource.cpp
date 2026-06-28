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

#include <Async/Sources/IoUringAsyncTCPSource.hpp>

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <format>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include <liburing.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/deferred.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/system/system_error.hpp>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceUtility.hpp>
#include <Sources/TCPDataServer.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES
{

IoUringAsyncTCPSource::IoUringAsyncTCPSource(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersIoUringTCP::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersIoUringTCP::PORT)))
{
}

asio::awaitable<void, Executor> IoUringAsyncTCPSource::onOpen()
try
{
    /// Connect with asio (resolve + connect), then drive recv on the native fd through io_uring. Keep the
    /// asio socket alive so it owns the fd; we only borrow its handle for the ring.
    NES_TRACE("IoUringTCP: opening connection to {}:{}", socketHost, socketPort);
    const auto executor = co_await asio::this_coro::executor;
    socket.emplace(executor);
    const auto endpoints = co_await asio::ip::tcp::resolver{executor}.async_resolve(socketHost, socketPort, asio::deferred);
    co_await asio::async_connect(socket.value(), endpoints, asio::deferred);
    socketFd = socket->native_handle();
    NES_DEBUG("IoUringTCP: connected to {}:{}.", socketHost, socketPort);

    co_await initRing();

    /// Acquire the first buffer and prime the pipeline with a recv into it.
    auto buffer = co_await acquireBuffer();
    if (not buffer.has_value())
    {
        co_return; /// stop requested during open
    }
    fillBuffer = std::move(*buffer);
    fillOffset = 0;
    fillStartMicros = ingestNowMicros();
    submitRecv();
    io_uring_submit(&ring);
}
catch (const boost::system::system_error& error)
{
    throw CannotOpenSource("IoUringTCP: failed to connect to {}:{} with error {}", socketHost, socketPort, error.what());
}

void IoUringAsyncTCPSource::submitRecv()
{
    io_uring_sqe* const sqe = io_uring_get_sqe(&ring);
    INVARIANT(sqe != nullptr, "io_uring SQ unexpectedly full when issuing recv");
    char* const dest = fillBuffer.getAvailableMemoryArea<char>().data() + fillOffset;
    io_uring_prep_recv(sqe, socketFd, dest, static_cast<unsigned>(bufferSize - fillOffset), 0);
}

asio::awaitable<std::optional<TupleBuffer>, Executor> IoUringAsyncTCPSource::takePreFilledBuffer()
{
    if (streamClosed)
    {
        co_return std::nullopt; /// peer already closed and the tail was emitted -> end of stream
    }

    while (true)
    {
        io_uring_cqe* cqe = nullptr;
        if (io_uring_peek_cqe(&ring, &cqe) != 0 || cqe == nullptr)
        {
            if (not co_await waitForCompletion())
            {
                co_return std::nullopt; /// cancelled (query stop)
            }
            continue;
        }
        const int res = cqe->res;
        io_uring_cqe_seen(&ring, cqe);

        if (res > 0)
        {
            fillOffset += static_cast<std::size_t>(res);
            if (fillOffset < bufferSize)
            {
                /// Partial recv -- keep filling the same buffer at the new offset (emit only full buffers, so
                /// the in-flight buffer count tracks bytes/bufferSize, not the number of TCP segments).
                submitRecv();
                io_uring_submit(&ring);
                continue;
            }
            /// Buffer full: stamp it, hand it to the runner, and prime the next recv before returning so the
            /// kernel fills buffer N+1 while the engine processes buffer N.
            TupleBuffer full = std::move(fillBuffer);
            full.setNumberOfTuples(bufferSize);
            full.setSourceCreationTimestampInMS(Timestamp(fillStartMicros));

            auto next = co_await acquireBuffer();
            if (not next.has_value())
            {
                co_return std::nullopt; /// cancelled while acquiring the next buffer
            }
            fillBuffer = std::move(*next);
            fillOffset = 0;
            fillStartMicros = ingestNowMicros();
            submitRecv();
            io_uring_submit(&ring);
            co_return full;
        }
        if (res == 0 || res == -ECONNRESET || res == -EPIPE || res == -ENOTCONN)
        {
            /// Peer closed: emit the partially filled tail buffer (if any), then signal EoS on the next call.
            streamClosed = true;
            if (fillOffset > 0)
            {
                TupleBuffer tail = std::move(fillBuffer);
                tail.setNumberOfTuples(fillOffset);
                tail.setSourceCreationTimestampInMS(Timestamp(fillStartMicros));
                co_return tail;
            }
            co_return std::nullopt;
        }
        if (res == -EINTR || res == -EAGAIN)
        {
            submitRecv(); /// transient -- retry the recv
            io_uring_submit(&ring);
            continue;
        }
        throw RunningRoutineFailure("IoUringTCP: recv failed: {}", std::strerror(-res));
    }
}

void IoUringAsyncTCPSource::onClose()
{
    fillBuffer = TupleBuffer();
    if (socket.has_value())
    {
        boost::system::error_code ignored;
        socket->close(ignored);
        socket.reset();
    }
    socketFd = -1;
}

DescriptorConfig::Config IoUringAsyncTCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersIoUringTCP>(std::move(config), std::string(NAME));
}

std::ostream& IoUringAsyncTCPSource::toString(std::ostream& str) const
{
    str << std::format("\nIoUringAsyncTCPSource(host: {}, port: {})", socketHost, socketPort);
    return str;
}

SourceValidationRegistryReturnType RegisterIoUringTCPSourceValidation(SourceValidationRegistryArguments arguments)
{
    return IoUringAsyncTCPSource::validateAndFormat(std::move(arguments.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterIoUringTCPSource(SourceRegistryArguments arguments)
{
    return std::make_unique<IoUringAsyncTCPSource>(arguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterIoUringTCPInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCP::PORT))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCP::HOST))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.tuples));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersIoUringTCP::PORT, std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersIoUringTCP::HOST, "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterIoUringTCPFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCP::PORT))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCP::HOST))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(systestAdaptorArguments.testFilePath);
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersIoUringTCP::PORT, std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersIoUringTCP::HOST, "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

}
