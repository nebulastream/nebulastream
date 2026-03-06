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

#include <TCPSocket.hpp>

#include <array>
#include <cerrno>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <netdb.h>
#include <poll.h>
#include <unistd.h>
#include <Util/Files.hpp>
#include <fmt/format.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{

struct ResolvedAddress
{
    std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> info{nullptr, &freeaddrinfo};
};

using ResolveResult = std::variant<ResolvedAddress, TCPSocket::ConnectError>;

ResolveResult resolve(const std::string& host, uint16_t port)
{
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    addrinfo* rawResult = nullptr;
    const std::string portStr = std::to_string(port);
    const auto errorCode = getaddrinfo(host.c_str(), portStr.c_str(), &hints, &rawResult);
    if (errorCode != 0)
    {
        return TCPSocket::ConnectError{fmt::format("Failed getaddrinfo with error: {}", gai_strerror(errorCode))};
    }
    return ResolvedAddress{.info = {rawResult, &freeaddrinfo}};
}

} /// anonymous namespace

/// --- StopEventFD ---

class StopEventFD
{
public:
    explicit StopEventFD(const std::stop_token& stopToken) : eventFd(::eventfd(0, EFD_NONBLOCK)), cb(stopToken, [this] { notify(); })
    {
        if (eventFd == -1)
        {
            throw CannotOpenSource("eventfd creation failed: {}", getErrorMessage(errno));
        }
    }

    ~StopEventFD()
    {
        if (eventFd != -1)
        {
            ::close(eventFd);
        }
    }

    StopEventFD(const StopEventFD&) = delete;
    StopEventFD& operator=(const StopEventFD&) = delete;
    StopEventFD(StopEventFD&&) = delete;
    StopEventFD& operator=(StopEventFD&&) = delete;

    [[nodiscard]] int fd() const { return eventFd; }

private:
    void notify() const
    {
        const uint64_t val = 1;
        ::write(eventFd, &val, sizeof(val));
    }

    int eventFd;
    std::stop_callback<std::function<void()>> cb;
};

CancellableSleepResult cancellableSleep(std::chrono::milliseconds duration, const std::stop_token& stopToken)
{
    const StopEventFD stopEvent(stopToken);
    pollfd pfd = {.fd = stopEvent.fd(), .events = POLLIN, .revents = 0};
    if (::poll(&pfd, 1, static_cast<int>(duration.count())) > 0)
    {
        return Stopped{};
    }
    return Elapsed{};
}

/// --- TCPSocket ---

TCPSocket::TCPSocket(int socketFd, std::unique_ptr<StopEventFD> stopEvent) : socketFd(socketFd), stopEvent(std::move(stopEvent))
{
}

TCPSocket::~TCPSocket()
{
    if (socketFd != -1)
    {
        ::close(socketFd);
    }
}

TCPSocket::TCPSocket(TCPSocket&& other) noexcept : socketFd(other.socketFd), stopEvent(std::move(other.stopEvent))
{
    other.socketFd = -1;
}

TCPSocket& TCPSocket::operator=(TCPSocket&& other) noexcept
{
    if (this != &other)
    {
        if (socketFd != -1)
        {
            ::close(socketFd);
        }
        socketFd = other.socketFd;
        stopEvent = std::move(other.stopEvent);
        other.socketFd = -1;
    }
    return *this;
}

TCPSocket::ConnectResult
TCPSocket::connect(const std::string& host, uint16_t port, const std::stop_token& stopToken, std::chrono::milliseconds connectionTimeout)
{
    /// Create StopEventFD if we have a stoppable token
    std::unique_ptr<StopEventFD> stopEvent;
    if (stopToken.stop_possible())
    {
        stopEvent = std::make_unique<StopEventFD>(stopToken);
    }

    /// DNS resolution
    auto resolveResult = resolve(host, port);
    if (auto* error = std::get_if<ConnectError>(&resolveResult))
    {
        return std::move(*error);
    }
    auto& addrResult = std::get<ResolvedAddress>(resolveResult).info;

    /// Create non-blocking socket
    const auto sockfd = ::socket(addrResult->ai_family, addrResult->ai_socktype | SOCK_NONBLOCK, addrResult->ai_protocol);
    if (sockfd == -1)
    {
        return ConnectError{fmt::format("Failed to create socket with error: {}", getErrorMessage(errno))};
    }
    TCPSocket sock(sockfd, std::move(stopEvent));

    /// Non-blocking connect: succeeds immediately (e.g. localhost) or returns EINPROGRESS
    if (::connect(sock.socketFd, addrResult->ai_addr, addrResult->ai_addrlen) == 0)
    {
        return sock;
    }
    if (errno != EINPROGRESS)
    {
        return ConnectError{fmt::format("Failed to connect with error: {}", getErrorMessage(errno))};
    }

    /// Wait for connect to complete
    const int timeoutMs = connectionTimeout.count() > 0 ? static_cast<int>(connectionTimeout.count()) : -1;
    int nfds = 1;
    std::array<pollfd, 2> fds{};
    fds[0] = {.fd = sock.socketFd, .events = POLLOUT, .revents = 0};
    if (sock.stopEvent)
    {
        fds[1] = {.fd = sock.stopEvent->fd(), .events = POLLIN, .revents = 0};
        nfds = 2;
    }

    auto pollResult = ::poll(fds.data(), nfds, timeoutMs);
    if (pollResult == -1)
    {
        return ConnectError{fmt::format("Failed to poll with error: {}", getErrorMessage(errno))};
    }
    if (pollResult == 0)
    {
        return Timeout{};
    }
    if (sock.stopEvent && (fds[1].revents & POLLIN) != 0)
    {
        return Stopped{};
    }

    /// Check for socket errors after poll
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(sock.socketFd, SOL_SOCKET, SO_ERROR, &error, &len) == -1)
    {
        return ConnectError{fmt::format("Failed to get socket error: {}", getErrorMessage(errno))};
    }
    if (error != 0)
    {
        return ConnectError{fmt::format("When connecting: Socket has error: {}", getErrorMessage(error))};
    }

    return sock;
}

TCPSocket::RecvResult TCPSocket::recvExact(std::span<uint8_t> buffer, std::chrono::milliseconds timeout, const std::stop_token& stopToken)
{
    /// Lazily create StopEventFD on first call with a stoppable token
    if (!stopEvent && stopToken.stop_possible())
    {
        stopEvent = std::make_unique<StopEventFD>(stopToken);
    }

    /// Negative = block forever, 0 = non-blocking, positive = that many ms
    const int timeoutMs = timeout.count() >= 0 ? static_cast<int>(timeout.count()) : -1;

    size_t totalReceived = 0;

    while (totalReceived < buffer.size())
    {
        int nfds = 1;
        std::array<pollfd, 2> fds{};
        fds[0] = {.fd = socketFd, .events = POLLIN, .revents = 0};

        if (stopEvent)
        {
            fds[1] = {.fd = stopEvent->fd(), .events = POLLIN, .revents = 0};
            nfds = 2;
        }

        auto pollResult = ::poll(fds.data(), nfds, timeoutMs);

        if (pollResult == -1)
        {
            if (errno == EINTR)
            {
                continue;
            }
            if (totalReceived > 0)
            {
                return std::span(buffer.data(), totalReceived);
            }
            return RecvError{fmt::format("poll failed: {}", getErrorMessage(errno))};
        }
        if (pollResult == 0)
        {
            if (totalReceived > 0)
            {
                return std::span(buffer.data(), totalReceived);
            }
            return Timeout{};
        }
        if (stopEvent && (fds[1].revents & POLLIN) != 0)
        {
            if (totalReceived > 0)
            {
                return std::span(buffer.data(), totalReceived);
            }
            return Stopped{};
        }

        /// Socket is readable — recv into remaining portion of buffer
        auto remaining = buffer.subspan(totalReceived);
        auto resultSize = ::recv(socketFd, remaining.data(), remaining.size(), 0);
        if (resultSize == -1)
        {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK)
            {
                continue;
            }
            if (totalReceived > 0)
            {
                return std::span(buffer.data(), totalReceived);
            }
            return RecvError{fmt::format("recv failed: {}", getErrorMessage(errno))};
        }
        if (resultSize == 0)
        {
            if (totalReceived > 0)
            {
                return std::span(buffer.data(), totalReceived);
            }
            return EoS{};
        }
        totalReceived += static_cast<size_t>(resultSize);
    }

    return std::span(buffer.data(), totalReceived);
}

}
