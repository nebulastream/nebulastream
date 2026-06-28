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

#include <Blocking/Sources/IoUringTCPSource.hpp>

#include <netdb.h>
#include <unistd.h>
#include <sys/socket.h>

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

#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/BlockingSource.hpp>
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

namespace
{
/// Ring depth. A single ordered stream only ever has one recv in flight (offset-fill), so the ring is tiny;
/// a couple of slots is plenty of headroom (recv SQE + the internal timeout SQE).
constexpr unsigned RING_DEPTH = 8;
/// Granularity at which a blocked recv wakes to re-check the stop token (query stop responsiveness).
constexpr __kernel_timespec WAIT_TIMEOUT{.tv_sec = 0, .tv_nsec = 100'000'000};
/// user_data tag on our recv SQE, so we can tell it apart from the timeout completion that
/// io_uring_submit_and_wait_timeout posts when it expires.
constexpr std::uint64_t RECV_USER_DATA = 1;
}

IoUringTCPSource::IoUringTCPSource(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersIoUringTCPBlocking::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersIoUringTCPBlocking::PORT)))
{
}

IoUringTCPSource::~IoUringTCPSource()
{
    close();
}

void IoUringTCPSource::connectSocket()
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    if (const int rc = ::getaddrinfo(socketHost.c_str(), socketPort.c_str(), &hints, &result); rc != 0)
    {
        throw CannotOpenSource("IoUringTCPBlocking: getaddrinfo({}:{}) failed: {}", socketHost, socketPort, gai_strerror(rc));
    }
    for (const addrinfo* ai = result; ai != nullptr; ai = ai->ai_next)
    {
        const int fd = ::socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd == -1)
        {
            continue;
        }
        if (::connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
        {
            sockfd = fd;
            break;
        }
        ::close(fd);
    }
    ::freeaddrinfo(result);
    if (sockfd == -1)
    {
        throw CannotOpenSource("IoUringTCPBlocking: could not connect to {}:{} - {}", socketHost, socketPort, std::strerror(errno));
    }
}

void IoUringTCPSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    bufferSize = bufferProvider->getBufferSize();

    connectSocket();
    NES_DEBUG("IoUringTCPBlocking: connected to {}:{}.", socketHost, socketPort);

    if (const int rc = io_uring_queue_init(RING_DEPTH, &ring, 0); rc < 0)
    {
        throw CannotOpenSource("IoUringTCPBlocking: io_uring_queue_init failed: {}", std::strerror(-rc));
    }
    ringInitialized = true;

    /// Register the socket as fixed file index 0 so each recv skips the per-op fd table lookup. Best-effort.
    if (io_uring_register_files(&ring, &sockfd, 1) == 0)
    {
        socketRegistered = true;
    }

    /// Acquire the first buffer; recv()s are issued lazily in takePreFilledBuffer().
    fillBuffer = bufferProvider->getBufferBlocking();
    fillOffset = 0;
    fillStartMicros = ingestNowMicros();
}

void IoUringTCPSource::submitRecv()
{
    io_uring_sqe* const sqe = io_uring_get_sqe(&ring);
    INVARIANT(sqe != nullptr, "io_uring SQ unexpectedly full when issuing recv");
    char* const dest = fillBuffer.getAvailableMemoryArea<char>().data() + fillOffset;
    const auto length = static_cast<unsigned>(bufferSize - fillOffset);
    if (socketRegistered)
    {
        io_uring_prep_recv(sqe, 0, dest, length, 0);
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    else
    {
        io_uring_prep_recv(sqe, sockfd, dest, length, 0);
    }
    io_uring_sqe_set_data64(sqe, RECV_USER_DATA);
    /// Not submitted here: io_uring_submit_and_wait_timeout in the reap loop submits this SQE AND waits for its
    /// completion in a SINGLE io_uring_enter -- one syscall per recv instead of submit + wait (two).
    recvInFlight = true;
}

std::optional<TupleBuffer> IoUringTCPSource::takePreFilledBuffer(const std::stop_token& stopToken)
{
    if (streamClosed)
    {
        return std::nullopt; /// peer already closed and the tail was emitted -> end of stream
    }

    while (true)
    {
        if (stopToken.stop_requested())
        {
            return std::nullopt;
        }
        if (not recvInFlight)
        {
            submitRecv(); /// preps the SQE; the submit happens in the combined submit-and-wait below
        }
        /// Submit the pending recv AND wait for its completion in one io_uring_enter (no eventfd/epoll). The
        /// timeout bounds the wait so a query stop is noticed promptly; on expiry it posts its own completion.
        io_uring_cqe* cqe = nullptr;
        auto timeout = WAIT_TIMEOUT;
        if (const int rc = io_uring_submit_and_wait_timeout(&ring, &cqe, 1, &timeout, nullptr);
            rc < 0 && rc != -ETIME && rc != -EINTR)
        {
            throw RunningRoutineFailure("IoUringTCPBlocking: io_uring_submit_and_wait_timeout failed: {}", std::strerror(-rc));
        }
        if (io_uring_peek_cqe(&ring, &cqe) != 0 || cqe == nullptr)
        {
            continue; /// nothing ready yet (timed out) -- re-check stop, the recv is still in flight
        }
        if (io_uring_cqe_get_data64(cqe) != RECV_USER_DATA)
        {
            io_uring_cqe_seen(&ring, cqe); /// the internal wait-timeout completion -- ignore it
            continue;
        }
        const int res = cqe->res;
        io_uring_cqe_seen(&ring, cqe);
        recvInFlight = false;

        if (res > 0)
        {
            fillOffset += static_cast<std::size_t>(res);
            if (fillOffset < bufferSize)
            {
                continue; /// partial -- keep filling the same buffer (emit only full buffers)
            }
            /// Buffer full: stamp it and hand it to the runner. The next recv is issued on the next call.
            TupleBuffer full = std::move(fillBuffer);
            full.setNumberOfTuples(bufferSize);
            full.setSourceCreationTimestampInMS(Timestamp(fillStartMicros));

            fillBuffer = bufferProvider->getBufferBlocking();
            fillOffset = 0;
            fillStartMicros = ingestNowMicros();
            return full;
        }
        if (res == 0 || res == -ECONNRESET || res == -EPIPE || res == -ENOTCONN)
        {
            streamClosed = true; /// peer closed: emit the partially filled tail (if any), EoS on the next call
            if (fillOffset > 0)
            {
                TupleBuffer tail = std::move(fillBuffer);
                tail.setNumberOfTuples(fillOffset);
                tail.setSourceCreationTimestampInMS(Timestamp(fillStartMicros));
                return tail;
            }
            return std::nullopt;
        }
        if (res == -EAGAIN || res == -EINTR)
        {
            continue; /// transient -- re-issue the recv
        }
        throw RunningRoutineFailure("IoUringTCPBlocking: recv failed: {}", std::strerror(-res));
    }
}

BlockingSource::FillTupleBufferResult IoUringTCPSource::fillTupleBuffer(TupleBuffer&, const std::stop_token&, std::size_t)
{
    /// Unreachable: preFillsBuffers() == true, so the runner uses takePreFilledBuffer() exclusively.
    return BlockingSource::FillTupleBufferResult::eos();
}

void IoUringTCPSource::close()
{
    fillBuffer = TupleBuffer();
    if (ringInitialized)
    {
        io_uring_queue_exit(&ring);
        ringInitialized = false;
    }
    if (sockfd != -1)
    {
        ::close(sockfd);
        sockfd = -1;
    }
}

DescriptorConfig::Config IoUringTCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersIoUringTCPBlocking>(std::move(config), std::string(NAME));
}

std::ostream& IoUringTCPSource::toString(std::ostream& str) const
{
    str << std::format("\nIoUringTCPSource(host: {}, port: {}, registeredSocket: {})", socketHost, socketPort, socketRegistered);
    return str;
}

SourceValidationRegistryReturnType RegisterIoUringTCPBlockingSourceValidation(SourceValidationRegistryArguments arguments)
{
    return IoUringTCPSource::validateAndFormat(std::move(arguments.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterIoUringTCPBlockingSource(SourceRegistryArguments arguments)
{
    return std::make_unique<IoUringTCPSource>(arguments.sourceDescriptor);
}

InlineDataRegistryReturnType
InlineDataGeneratedRegistrar::RegisterIoUringTCPBlockingInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCPBlocking::PORT))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCPBlocking::HOST))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.tuples));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        ConfigParametersIoUringTCPBlocking::PORT, std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersIoUringTCPBlocking::HOST, "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterIoUringTCPBlockingFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCPBlocking::PORT))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCPBlocking::HOST))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(systestAdaptorArguments.testFilePath);
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        ConfigParametersIoUringTCPBlocking::PORT, std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersIoUringTCPBlocking::HOST, "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

}
