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

#include <Blocking/Sources/IoUringTCPMultishotSource.hpp>

#include <netdb.h>
#include <unistd.h>
#include <sys/socket.h>

#include <algorithm>
#include <bit>
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
/// Buffer group id for the provided buffer ring (a single group is enough for one socket).
constexpr int BUFFER_GROUP_ID = 0;
/// Provided-ring size is a SMALL fraction of the source pool. This is critical: once a multishot recv is
/// armed, the KERNEL autonomously fills provided buffers as data arrives -- so the ring size bounds how far
/// the recv can run ahead of the engine. If the ring is a large fraction of the pool, the kernel fills the
/// whole ring before the engine (still compiling the query) drains anything, and replenishment exhausts the
/// pool -> getBufferBlocking times out -> query fails. Keeping the ring at ~pool/8 leaves ample in-flight
/// margin so replenishment never starves and backpressure is a graceful block, not a failure.
constexpr unsigned MIN_BUFFERS = 4;
constexpr unsigned MAX_BUFFERS = 64;
constexpr unsigned POOL_RING_DIVISOR = 8;
/// Granularity at which a blocked reap wakes to re-check the stop token (query-stop responsiveness).
constexpr __kernel_timespec WAIT_TIMEOUT{.tv_sec = 0, .tv_nsec = 100'000'000};
/// user_data tag on our multishot recv SQE, to tell it apart from the internal wait-timeout completion.
constexpr std::uint64_t RECV_USER_DATA = 1;
}

IoUringTCPMultishotSource::IoUringTCPMultishotSource(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersIoUringTCPMultishot::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersIoUringTCPMultishot::PORT)))
{
}

IoUringTCPMultishotSource::~IoUringTCPMultishotSource()
{
    close();
}

void IoUringTCPMultishotSource::connectSocket()
{
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* result = nullptr;
    if (const int rc = ::getaddrinfo(socketHost.c_str(), socketPort.c_str(), &hints, &result); rc != 0)
    {
        throw CannotOpenSource("IoUringTCPMultishot: getaddrinfo({}:{}) failed: {}", socketHost, socketPort, gai_strerror(rc));
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
        throw CannotOpenSource("IoUringTCPMultishot: could not connect to {}:{} - {}", socketHost, socketPort, std::strerror(errno));
    }
}

void IoUringTCPMultishotSource::open(std::shared_ptr<AbstractBufferProvider> provider)
{
    bufferProvider = std::move(provider);
    bufferSize = bufferProvider->getBufferSize();

    /// Size the provided buffer ring to ~half the pool (power of two, clamped) so the ring stays full of
    /// in-flight recv targets while leaving pool headroom to replenish consumed slots without stalling.
    const auto pooled = bufferProvider->getNumOfPooledBuffers();
    const auto target = static_cast<unsigned>(std::clamp<std::size_t>(pooled / POOL_RING_DIVISOR, MIN_BUFFERS, MAX_BUFFERS));
    numBuffers = std::bit_floor(target);

    connectSocket();
    NES_DEBUG("IoUringTCPMultishot: connected to {}:{} (ring buffers={}).", socketHost, socketPort, numBuffers);

    if (const int rc = io_uring_queue_init(numBuffers, &ring, 0); rc < 0)
    {
        throw CannotOpenSource("IoUringTCPMultishot: io_uring_queue_init failed: {}", std::strerror(-rc));
    }
    ringInitialized = true;

    /// Register the socket as fixed file index 0 so each recv skips the per-op fd table lookup. Best-effort.
    if (io_uring_register_files(&ring, &sockfd, 1) == 0)
    {
        socketRegistered = true;
    }

    /// Set up the provided buffer ring and back every slot with a pool buffer.
    int ret = 0;
    bufRing = io_uring_setup_buf_ring(&ring, numBuffers, BUFFER_GROUP_ID, 0, &ret);
    if (bufRing == nullptr)
    {
        throw CannotOpenSource("IoUringTCPMultishot: io_uring_setup_buf_ring failed: {}", std::strerror(-ret));
    }
    slotBuffers.resize(numBuffers);
    const int mask = io_uring_buf_ring_mask(numBuffers);
    for (std::uint16_t bid = 0; bid < numBuffers; ++bid)
    {
        slotBuffers[bid] = bufferProvider->getBufferBlocking();
        io_uring_buf_ring_add(
            bufRing, slotBuffers[bid].getAvailableMemoryArea<char>().data(), static_cast<unsigned>(bufferSize), bid, mask, bid);
    }
    io_uring_buf_ring_advance(bufRing, static_cast<int>(numBuffers));
}

void IoUringTCPMultishotSource::armMultishotRecv()
{
    io_uring_sqe* const sqe = io_uring_get_sqe(&ring);
    INVARIANT(sqe != nullptr, "io_uring SQ unexpectedly full when arming multishot recv");
    io_uring_prep_recv_multishot(sqe, socketRegistered ? 0 : sockfd, nullptr, 0, 0);
    sqe->flags |= IOSQE_BUFFER_SELECT;
    if (socketRegistered)
    {
        sqe->flags |= IOSQE_FIXED_FILE;
    }
    sqe->buf_group = BUFFER_GROUP_ID;
    io_uring_sqe_set_data64(sqe, RECV_USER_DATA);
    multishotArmed = true;
}

void IoUringTCPMultishotSource::replenishBuffer(const std::uint16_t bid)
{
    /// Acquire a fresh pool buffer for the just-consumed slot. Blocking here IS the backpressure: if the
    /// engine has not released buffers yet, the producer waits instead of overrunning the pool.
    slotBuffers[bid] = bufferProvider->getBufferBlocking();
    const int mask = io_uring_buf_ring_mask(numBuffers);
    io_uring_buf_ring_add(bufRing, slotBuffers[bid].getAvailableMemoryArea<char>().data(), static_cast<unsigned>(bufferSize), bid, mask, 0);
    io_uring_buf_ring_advance(bufRing, 1);
}

std::optional<TupleBuffer> IoUringTCPMultishotSource::takePreFilledBuffer(const std::stop_token& stopToken)
{
    /// Hand back already-reaped buffers first (a single multishot reap can yield many).
    if (not ready.empty())
    {
        TupleBuffer buffer = std::move(ready.front());
        ready.pop_front();
        return buffer;
    }
    if (streamClosed)
    {
        return std::nullopt; /// peer closed and the backlog is drained -> end of stream
    }

    while (true)
    {
        if (stopToken.stop_requested())
        {
            return std::nullopt;
        }
        if (not multishotArmed)
        {
            armMultishotRecv(); /// preps the SQE; the submit happens in the combined submit-and-wait below
        }

        /// Submit the armed recv (if any) AND wait for >=1 completion in one io_uring_enter (no eventfd/epoll).
        /// The timeout bounds the wait so a query stop is noticed promptly.
        io_uring_cqe* firstCqe = nullptr;
        auto timeout = WAIT_TIMEOUT;
        if (const int rc = io_uring_submit_and_wait_timeout(&ring, &firstCqe, 1, &timeout, nullptr); rc < 0 && rc != -ETIME && rc != -EINTR)
        {
            throw RunningRoutineFailure("IoUringTCPMultishot: io_uring_submit_and_wait_timeout failed: {}", std::strerror(-rc));
        }

        /// Drain every completion the kernel posted (multishot posts one per arriving segment). Collect the
        /// consumed buffer ids and replenish AFTER advancing the CQ, so the CQ frees up promptly.
        std::uint16_t consumed[MAX_BUFFERS];
        unsigned consumedCount = 0;
        const auto reapMicros = ingestNowMicros();
        bool fatalError = false;
        int fatalRes = 0;
        unsigned head = 0;
        unsigned seen = 0;
        io_uring_cqe* cqe = nullptr;
        io_uring_for_each_cqe(&ring, head, cqe)
        {
            ++seen;
            if (io_uring_cqe_get_data64(cqe) != RECV_USER_DATA)
            {
                continue; /// internal wait-timeout completion -- ignore
            }
            const int res = cqe->res;
            const unsigned flags = cqe->flags;
            if (res > 0)
            {
                INVARIANT(flags & IORING_CQE_F_BUFFER, "multishot recv completion without a selected buffer");
                const auto bid = static_cast<std::uint16_t>(flags >> IORING_CQE_BUFFER_SHIFT);
                TupleBuffer buffer = std::move(slotBuffers[bid]);
                buffer.setNumberOfTuples(static_cast<std::size_t>(res));
                buffer.setSourceCreationTimestampInMS(Timestamp(reapMicros));
                ready.push_back(std::move(buffer));
                consumed[consumedCount++] = bid;
            }
            else if (res == 0 || res == -ECONNRESET || res == -EPIPE || res == -ENOTCONN)
            {
                streamClosed = true; /// peer closed -- drain `ready`, then signal EoS
            }
            else if (res == -ENOBUFS)
            {
                /// Ran out of provided buffers (engine slower than the link). Re-arm after replenishing.
            }
            else if (res != -EINTR && res != -EAGAIN)
            {
                fatalError = true;
                fatalRes = res;
            }
            if (not(flags & IORING_CQE_F_MORE))
            {
                multishotArmed = false; /// multishot terminated -- must re-arm to keep receiving
            }
        }
        io_uring_cq_advance(&ring, seen);

        if (fatalError)
        {
            throw RunningRoutineFailure("IoUringTCPMultishot: recv failed: {}", std::strerror(-fatalRes));
        }
        for (unsigned i = 0; i < consumedCount; ++i)
        {
            replenishBuffer(consumed[i]);
        }

        if (not ready.empty())
        {
            TupleBuffer buffer = std::move(ready.front());
            ready.pop_front();
            return buffer;
        }
        if (streamClosed)
        {
            return std::nullopt; /// closed with nothing buffered
        }
        /// else: timed out with no data (or only -ENOBUFS/transient) -- loop, re-check stop, re-arm.
    }
}

BlockingSource::FillTupleBufferResult IoUringTCPMultishotSource::fillTupleBuffer(TupleBuffer&, const std::stop_token&, std::size_t)
{
    /// Unreachable: preFillsBuffers() == true, so the runner uses takePreFilledBuffer() exclusively.
    return BlockingSource::FillTupleBufferResult::eos();
}

void IoUringTCPMultishotSource::close()
{
    ready.clear();
    slotBuffers.clear();
    if (bufRing != nullptr && ringInitialized)
    {
        io_uring_free_buf_ring(&ring, bufRing, numBuffers, BUFFER_GROUP_ID);
        bufRing = nullptr;
    }
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

DescriptorConfig::Config IoUringTCPMultishotSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersIoUringTCPMultishot>(std::move(config), std::string(NAME));
}

std::ostream& IoUringTCPMultishotSource::toString(std::ostream& str) const
{
    str << std::format(
        "\nIoUringTCPMultishotSource(host: {}, port: {}, registeredSocket: {}, ringBuffers: {})",
        socketHost,
        socketPort,
        socketRegistered,
        numBuffers);
    return str;
}

SourceValidationRegistryReturnType RegisterIoUringTCPMultishotSourceValidation(SourceValidationRegistryArguments arguments)
{
    return IoUringTCPMultishotSource::validateAndFormat(std::move(arguments.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterIoUringTCPMultishotSource(SourceRegistryArguments arguments)
{
    return std::make_unique<IoUringTCPMultishotSource>(arguments.sourceDescriptor);
}

InlineDataRegistryReturnType
InlineDataGeneratedRegistrar::RegisterIoUringTCPMultishotInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCPMultishot::PORT))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCPMultishot::HOST))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.tuples));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        ConfigParametersIoUringTCPMultishot::PORT, std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersIoUringTCPMultishot::HOST, "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

FileDataRegistryReturnType
FileDataGeneratedRegistrar::RegisterIoUringTCPMultishotFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCPMultishot::PORT))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a port");
    }
    if (systestAdaptorArguments.physicalSourceConfig.sourceConfig.contains(ConfigParametersIoUringTCPMultishot::HOST))
    {
        throw InvalidConfigParameter("Cannot use mock implementation if config already contains a host");
    }

    auto mockTCPServer = std::make_unique<TCPDataServer>(systestAdaptorArguments.testFilePath);
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(
        ConfigParametersIoUringTCPMultishot::PORT, std::to_string(mockTCPServer->getPort()));
    systestAdaptorArguments.physicalSourceConfig.sourceConfig.emplace(ConfigParametersIoUringTCPMultishot::HOST, "localhost");

    auto serverThread = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
    systestAdaptorArguments.serverThreads->push_back(std::move(serverThread));

    return systestAdaptorArguments.physicalSourceConfig;
}

}
