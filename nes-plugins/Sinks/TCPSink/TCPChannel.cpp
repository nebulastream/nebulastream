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

#include <TCPChannel.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <ctime>
#include <exception>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <ares.h>
#include <netdb.h>
#include <poll.h>
#include <unistd.h>
#include <Runtime/TupleBuffer.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Files.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Wait.hpp>
#include <boost/scope/unique_resource.hpp>
#include <cpptrace/from_current.hpp> /// NOLINT(misc-include-cleaner)
#include <cpptrace/from_current_macros.hpp> ///NOLINT(misc-include-cleaner)
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include <scope_guard.hpp>

namespace NES
{
namespace
{
constexpr std::chrono::milliseconds::rep IO_POLL_INTERVAL_MS = 10;
constexpr int MILLISECONDS_PER_SECOND = 1'000;

template <typename State>
void updateResolverSocket(void* data, const ares_socket_t descriptor, const int readable, const int writable)
{
    auto& state = *static_cast<State*>(data);
    const auto match = std::ranges::find(state.sockets, descriptor, &pollfd::fd);
    if (readable == 0 && writable == 0)
    {
        if (match != state.sockets.end())
        {
            state.sockets.erase(match);
        }
        return;
    }

    const auto events = static_cast<decltype(pollfd{}.events)>(
        (readable != 0 ? static_cast<unsigned int>(POLLIN) : 0U) | (writable != 0 ? static_cast<unsigned int>(POLLOUT) : 0U));
    if (match == state.sockets.end())
    {
        state.sockets.push_back({.fd = descriptor, .events = events, .revents = 0});
    }
    else
    {
        match->events = events;
    }
}

template <typename State>
void collectResolvedAddresses(void* data, const int status, int, ares_addrinfo* result)
{
    auto& state = *static_cast<State*>(data);
    state.status = status;
    state.complete = true;
    if (status == ARES_SUCCESS && result != nullptr)
    {
        for (auto* address = result->nodes; address != nullptr; address = address->ai_next)
        {
            if (address->ai_addrlen > sizeof(sockaddr_storage))
            {
                continue;
            }
            auto& resolvedAddress = state.addresses.emplace_back();
            std::memcpy(&resolvedAddress.storage, address->ai_addr, address->ai_addrlen);
            resolvedAddress.size = address->ai_addrlen;
            resolvedAddress.family = address->ai_family;
            resolvedAddress.type = address->ai_socktype;
            resolvedAddress.protocol = address->ai_protocol;
        }
    }
    if (result != nullptr)
    {
        ares_freeaddrinfo(result);
    }
}

std::vector<ares_fd_events_t> toAresEvents(const std::vector<pollfd>& descriptors)
{
    std::vector<ares_fd_events_t> events;
    for (const auto& descriptor : descriptors)
    {
        unsigned int flags = ARES_FD_EVENT_NONE;
        const auto returnedEvents = static_cast<uint32_t>(descriptor.revents);
        const auto readableEvents = static_cast<uint32_t>(POLLIN) | static_cast<uint32_t>(POLLERR) | static_cast<uint32_t>(POLLHUP);
        if ((returnedEvents & readableEvents) != 0)
        {
            flags |= ARES_FD_EVENT_READ;
        }
        if ((returnedEvents & static_cast<uint32_t>(POLLOUT)) != 0)
        {
            flags |= ARES_FD_EVENT_WRITE;
        }
        if (flags != ARES_FD_EVENT_NONE)
        {
            events.push_back({.fd = descriptor.fd, .events = flags});
        }
    }
    return events;
}
}

struct TCPChannel::ResolvedAddress
{
    [[nodiscard]] std::string toString() const
    {
        std::array<char, NI_MAXHOST> host{};
        std::array<char, NI_MAXSERV> service{};
        /// POSIX address lookup accepts sockaddr even when the storage owns a sockaddr_storage.
        const auto* socketAddress = reinterpret_cast<const sockaddr*>(&storage); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        const auto result
            = getnameinfo(socketAddress, size, host.data(), host.size(), service.data(), service.size(), NI_NUMERICHOST | NI_NUMERICSERV);
        if (result != 0)
        {
            return fmt::format("unprintable address (family {}: {})", family, gai_strerror(result));
        }
        return family == AF_INET6 ? fmt::format("[{}]:{}", host.data(), service.data()) : fmt::format("{}:{}", host.data(), service.data());
    }

    sockaddr_storage storage{};
    socklen_t size = 0;
    int family = 0;
    int type = 0;
    int protocol = 0;
};

class TCPChannel::Socket
{
public:
    explicit Socket(const int descriptor) noexcept : descriptor(descriptor) { }

    [[nodiscard]] int get() const noexcept { return descriptor.get(); }

private:
    struct Closer
    {
        void operator()(const int descriptor) const noexcept
        {
            if (::close(descriptor) != 0)
            {
                NES_WARNING("Failed to close TCP socket: {}", getErrorMessageFromERRNO());
            }
        }
    };

    boost::scope::unique_resource<int, Closer> descriptor;
};

struct TCPChannel::Ok
{
    Socket socket;
};

TCPChannel::TCPChannel(
    std::string host,
    const uint32_t port,
    const std::chrono::milliseconds connectionTimeout,
    const std::chrono::milliseconds closeTimeout,
    const size_t maxQueuedBuffers)
    : queue(maxQueuedBuffers), connectionTimeout(connectionTimeout), closeTimeout(closeTimeout), host(std::move(host)), port(port)
{
}

TCPChannel::~TCPChannel()
{
    if (ioThread.joinable())
    {
        NES_WARNING(
            "Aborting TCPChannel for {}:{} with {} pending buffers during destruction",
            host,
            port,
            pendingBuffers.load(std::memory_order_acquire));
        stop();
    }
}

void TCPChannel::start()
{
    PRECONDITION(!ioThread.joinable(), "TCPChannel is already started");
    ioThread = Thread("tcp-sink", [this](const std::stop_token& stopToken) { run(stopToken); });
}

TCPChannel::SendResult TCPChannel::trySend(const TupleBuffer& buffer)
{
    PRECONDITION(ioThread.joinable(), "TCPChannel is not started");
    rethrowError();

    pendingBuffers.fetch_add(1, std::memory_order_acq_rel);
    if (queue.writeIfNotFull(buffer))
    {
        return SendResult::Accepted;
    }
    pendingBuffers.fetch_sub(1, std::memory_order_release);
    return SendResult::Full;
}

/// Attempts a connection to one address using a nonblocking socket.
/// If the connection is in progress, polls within the supplied timeout
/// and checks SO_ERROR to determine whether the connection succeeded.
/// Returns Ok with the socket, Error with a failure or timeout message, or Stopped on cancellation.
TCPChannel::ConnectResult
TCPChannel::tryConnect(const ResolvedAddress& address, const std::chrono::milliseconds timeout, const std::stop_token& stopToken)
{
    const auto deadline = std::chrono::steady_clock::now() + timeout;

    if (stopToken.stop_requested())
    {
        return Stopped{};
    }

    const auto candidateFd = socket(
        address.family,
        static_cast<int>(
            static_cast<unsigned int>(address.type) | static_cast<unsigned int>(SOCK_NONBLOCK) | static_cast<unsigned int>(SOCK_CLOEXEC)),
        address.protocol);
    if (candidateFd < 0)
    {
        return Error{getErrorMessageFromERRNO()};
    }
    Socket candidate{candidateFd};

    /// POSIX connect accepts sockaddr while the resolved address uses sockaddr_storage.
    const auto* socketAddress = reinterpret_cast<const sockaddr*>(&address.storage); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    if (::connect(candidate.get(), socketAddress, address.size) == 0)
    {
        return Ok{std::move(candidate)};
    }
    if (errno != EINPROGRESS)
    {
        return Error{getErrorMessageFromERRNO()};
    }

    const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
    if (remaining <= std::chrono::milliseconds::zero())
    {
        return Error{"connection timed out"};
    }

    pollfd descriptor{.fd = candidate.get(), .events = POLLOUT, .revents = 0};
    const auto pollResult = pollWithStop(std::span{&descriptor, 1}, deadline, stopToken);

    if (pollResult == PollResult::Stopped)
    {
        return Stopped{};
    }
    if (pollResult == PollResult::TimedOut)
    {
        return Error{"connection timed out"};
    }

    int socketError = 0;
    socklen_t socketErrorSize = sizeof(socketError);
    if (getsockopt(candidate.get(), SOL_SOCKET, SO_ERROR, &socketError, &socketErrorSize) != 0) /// NOLINT(misc-include-cleaner)
    {
        return Error{getErrorMessageFromERRNO()};
    }
    if (socketError != 0)
    {
        return Error{getErrorMessage(socketError)};
    }

    return Ok{std::move(candidate)};
}

/// Resolves the host and port to IPv4 and IPv6 TCP addresses.
/// Returns the addresses, or std::nullopt on cancellation.
/// Throws CannotOpenSink on resolution failure, timeout, or an empty address list.
/// c-ares may report a DNS timeout after exhausting its own retries, independently of
/// the configured connection timeout and before the supplied overall deadline expires.
std::optional<std::vector<TCPChannel::ResolvedAddress>> TCPChannel::resolveAddresses(
    const std::string& host, const uint32_t port, const std::chrono::milliseconds timeout, const std::stop_token& stopToken)
{
    struct LookupState
    {
        std::vector<pollfd> sockets;
        std::vector<ResolvedAddress> addresses;
        int status = ARES_SUCCESS;
        bool complete = false;
    } state;

    ares_channel_t* channel = nullptr;

    if (const auto libraryStatus = ares_library_initialized(); libraryStatus != ARES_SUCCESS)
    {
        throw CannotOpenSink("DNS resolver library is not initialized: {}", ares_strerror(libraryStatus));
    }

    ares_options options{};
    /// Track the resolver's sockets and the events needed to make progress.
    options.sock_state_cb = updateResolverSocket<LookupState>;
    options.sock_state_cb_data = &state;
    if (const auto result = ares_init_options(&channel, &options, ARES_OPT_SOCK_STATE_CB); result != ARES_SUCCESS)
    {
        throw CannotOpenSink("Failed to create DNS resolver for {}: {}", host, ares_strerror(result));
    }
    /// Cancel outstanding requests and destroy the channel on every exit path.
    SCOPE_EXIT
    {
        ares_cancel(channel);
        ares_destroy(channel);
    };

    ares_addrinfo_hints hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    const auto portString = std::to_string(port);
    ares_getaddrinfo(channel, host.c_str(), portString.c_str(), &hints, collectResolvedAddresses<LookupState>, &state);

    constexpr int microsecondsPerMillisecond = 1'000;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!state.complete)
    {
        if (stopToken.stop_requested())
        {
            return std::nullopt;
        }

        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        if (remaining <= std::chrono::milliseconds::zero())
        {
            throw CannotOpenSink("Failed to resolve TCPChannel host {} within {} milliseconds", host, timeout.count());
        }

        auto descriptors = state.sockets;
        timeval maximumTimeout{
            .tv_sec = static_cast<time_t>(remaining.count() / MILLISECONDS_PER_SECOND),
            .tv_usec = static_cast<suseconds_t>((remaining.count() % MILLISECONDS_PER_SECOND) * microsecondsPerMillisecond),
        };
        timeval resolverTimeout{};
        /// Poll using c-ares' retry timing, bounded by the remaining overall timeout.
        /// This does not change c-ares' own timeout or retry limit, which may cause an earlier failure.
        const auto* selectedTimeout = ares_timeout(channel, &maximumTimeout, &resolverTimeout);
        const auto pollTimeout = static_cast<int>(
            (selectedTimeout->tv_sec * MILLISECONDS_PER_SECOND)
            + ((selectedTimeout->tv_usec + microsecondsPerMillisecond - 1) / microsecondsPerMillisecond));

        const auto pollResult
            = pollWithStop(descriptors, std::chrono::steady_clock::now() + std::chrono::milliseconds(pollTimeout), stopToken);
        if (pollResult == PollResult::Stopped)
        {
            return std::nullopt;
        }

        auto events = toAresEvents(descriptors);
        if (const auto result = ares_process_fds(channel, events.empty() ? nullptr : events.data(), events.size(), ARES_PROCESS_FLAG_NONE);
            result != ARES_SUCCESS)
        {
            throw CannotOpenSink("Failed while resolving TCPChannel host {}: {}", host, ares_strerror(result));
        }
    }

    if (state.status != ARES_SUCCESS)
    {
        throw CannotOpenSink("Failed to resolve TCPChannel host {}: {}", host, ares_strerror(state.status));
    }
    if (state.addresses.empty())
    {
        throw CannotOpenSink("Failed to resolve TCPChannel host {}: no addresses returned", host);
    }
    return std::move(state.addresses);
}

/// Resolves the server addresses once, sharing one deadline between DNS and connection attempts.
/// Each pass divides the remaining connection budget evenly among the resolved addresses.
/// Retries unsuccessful passes after a 10 ms wait, capped by the deadline.
/// Returns the connected socket, or std::nullopt if cancellation is observed.
/// Throws CannotOpenSink if resolution fails or the connection deadline expires.
std::optional<TCPChannel::Socket> TCPChannel::connectUntil(const std::chrono::milliseconds timeout, const std::stop_token& stopToken) const
{
    std::unordered_map<std::string, std::vector<std::string>> connectionErrors;
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    auto addresses = resolveAddresses(
        host, port, std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now()), stopToken);
    if (!addresses)
    {
        return std::nullopt;
    }
    while (std::chrono::steady_clock::now() < deadline)
    {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
        const auto addressBudget
            = std::max(remaining / static_cast<std::chrono::milliseconds::rep>(addresses->size()), std::chrono::milliseconds(1));
        for (const auto& address : *addresses)
        {
            const auto timeLeft = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now());
            if (timeLeft <= std::chrono::milliseconds::zero())
            {
                break;
            }
            auto result = tryConnect(address, std::min(addressBudget, timeLeft), stopToken);
            if (auto* connected = std::get_if<Ok>(&result))
            {
                return std::move(connected->socket);
            }
            if (std::holds_alternative<Stopped>(result))
            {
                return std::nullopt;
            }
            connectionErrors[address.toString()].push_back(std::move(std::get<Error>(result).message));
        }

        if (!waitUntil(std::min(deadline, std::chrono::steady_clock::now() + std::chrono::milliseconds{IO_POLL_INTERVAL_MS}), stopToken))
        {
            return std::nullopt;
        }
    }

    std::vector<std::string> addressErrors;
    for (auto& [address, errors] : connectionErrors)
    {
        std::ranges::sort(errors);
        errors.erase(std::ranges::unique(errors).begin(), errors.end());
        addressErrors.push_back(fmt::format("{}: {}", address, fmt::join(errors, ", ")));
    }
    std::ranges::sort(addressErrors);
    auto details = fmt::format("{}", fmt::join(addressErrors, "; "));
    if (details.empty())
    {
        details = "deadline expired before any connection attempt";
    }

    NES_WARNING("Failed to connect TCPChannel to {}:{} within {} milliseconds: {}", host, port, timeout.count(), details);
    throw CannotOpenSink("Failed to connect TCPChannel to {}:{} within {} milliseconds: {}", host, port, timeout.count(), details);
}

TCPChannel::IOResult TCPChannel::writeData(const std::span<const char> data, Socket& socket, const std::stop_token& stopToken)
{
    size_t offset = 0;
    while (offset < data.size())
    {
        if (stopToken.stop_requested())
        {
            return IOResult::Stopped;
        }

        const auto written = send(socket.get(), data.data() + offset, data.size() - offset, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (written > 0)
        {
            offset += static_cast<size_t>(written);
            continue;
        }
        if (written < 0 && errno == EINTR)
        {
            continue;
        }
        if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            pollfd descriptor{.fd = socket.get(), .events = POLLOUT, .revents = 0};
            const auto pollResult = pollWithStop(std::span{&descriptor, 1}, std::nullopt, stopToken);
            if (pollResult == PollResult::Stopped)
            {
                return IOResult::Stopped;
            }
            if ((static_cast<uint32_t>(descriptor.revents) & static_cast<uint32_t>(POLLOUT)) != 0)
            {
                continue;
            }
        }
        return IOResult::Disconnected;
    }
    return IOResult::Complete;
}

/// Collects the buffer's elements and sends their content bytes in order.
/// Handles partial writes and retries interrupted sends.
/// Waits for socket writability when a send would block.
/// On disconnection, reconnects and restarts the entire buffer, which may duplicate bytes.
/// Returns Complete once all bytes are written, or Stopped on cancellation.
/// Propagates connection and I/O errors to run().
TCPChannel::IOResult
TCPChannel::writeBuffer(const TupleBuffer& buffer, std::optional<Socket>& socket, const std::stop_token& stopToken) const
{
    if (!socket)
    {
        return IOResult::Stopped;
    }

    std::vector<BufferIterator::BufferElement> elements;
    BufferIterator iterator{buffer};
    for (auto element = iterator.getNextElement(); element; element = iterator.getNextElement())
    {
        elements.emplace_back(std::move(*element));
    }

    while (true)
    {
        bool reconnect = false;
        for (const auto& element : elements)
        {
            const auto data = element.buffer.getAvailableMemoryArea<const char>().first(element.contentLength);
            const auto result = writeData(data, socket.value(), stopToken);
            if (result == IOResult::Stopped)
            {
                return result;
            }
            if (result == IOResult::Disconnected)
            {
                reconnect = true;
                break;
            }
        }
        if (!reconnect)
        {
            return IOResult::Complete;
        }

        NES_WARNING("TCPChannel disconnected from {}:{}; reconnecting", host, port);
        socket.reset();
        socket = connectUntil(connectionTimeout, stopToken);
        if (!socket)
        {
            return IOResult::Stopped;
        }
        NES_INFO("Reconnected TCPChannel to {}:{}.", host, port);
    }
}

/// Entry point for the I/O thread. Establishes a connection before consuming queued buffers.
/// Reads the queue with a 10 ms timeout so stop requests are checked while the queue is empty.
/// Writes one buffer at a time, decrementing pendingBuffers only after writeBuffer completes.
/// Returns when a stop request is observed and stores any exception for producer-side calls to rethrow.
void TCPChannel::run(const std::stop_token& stopToken)
{
    CPPTRACE_TRY
    {
        auto socket = connectUntil(connectionTimeout, stopToken);
        if (!socket)
        {
            return;
        }
        NES_INFO("Connected TCPChannel to {}:{}.", host, port);

        while (true)
        {
            TupleBuffer buffer;
            if (!queue.tryReadUntil(std::chrono::steady_clock::now() + std::chrono::milliseconds{IO_POLL_INTERVAL_MS}, buffer))
            {
                if (stopToken.stop_requested())
                {
                    return;
                }
                continue;
            }
            if (stopToken.stop_requested())
            {
                return;
            }
            if (writeBuffer(buffer, socket, stopToken) == IOResult::Stopped)
            {
                return;
            }
            pendingBuffers.fetch_sub(1, std::memory_order_release);
        }
    }
    CPPTRACE_CATCH(...)
    {
        storeError(std::current_exception());
    }
}

bool TCPChannel::tryClose()
{
    rethrowError();
    if (!closeDeadline)
    {
        closeDeadline = std::chrono::steady_clock::now() + closeTimeout;
    }

    const auto pending = pendingBuffers.load(std::memory_order_acquire);
    if (pending == 0)
    {
        stop();
        return true;
    }
    if (std::chrono::steady_clock::now() < *closeDeadline)
    {
        return false;
    }

    NES_WARNING(
        "Aborting TCPChannel for {}:{} after waiting {} milliseconds; {} accepted buffers may be lost",
        host,
        port,
        closeTimeout.count(),
        pending);
    stop();
    return true;
}

void TCPChannel::stop()
{
    if (!ioThread.joinable())
    {
        return;
    }
    ioThread.requestStop();
    ioThread = Thread{};
}

void TCPChannel::storeError(std::exception_ptr exception)
{
    *error.wlock() = std::move(exception);
}

void TCPChannel::rethrowError() const
{
    const auto lockedError = error.rlock();
    if (*lockedError)
    {
        std::rethrow_exception(*lockedError);
    }
}

}
