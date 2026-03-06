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

#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <stop_token>
#include <string>
#include <variant>

namespace NES
{

/// Forward declaration — implementation detail, defined in TCPSocket.cpp.
class StopEventFD;

struct Stopped
{
};

struct Elapsed
{
};

using CancellableSleepResult = std::variant<Elapsed, Stopped>;

/// Cancellable sleep: waits for @p duration or until @p stopToken is triggered.
CancellableSleepResult cancellableSleep(std::chrono::milliseconds duration, const std::stop_token& stopToken);

/// RAII wrapper around a TCP socket. Handles DNS resolution, connection with timeout, and receiving data.
/// Does not contain any reconnection logic — that is a policy decision for the caller (e.g. TCPSource).
class TCPSocket
{
public:
    struct Timeout
    {
    };

    struct EoS
    {
    };

    using ConnectError = std::string;
    using RecvError = std::string;
    using ConnectResult = std::variant<TCPSocket, Timeout, Stopped, ConnectError>;
    using RecvResult = std::variant<std::span<uint8_t>, Timeout, EoS, Stopped, RecvError>;

    /// Resolve host, create socket, and connect.
    /// Blocks until the connection succeeds, @p stopToken is triggered,
    /// or @p connectionTimeout expires (0 means no timeout).
    static ConnectResult
    connect(const std::string& host, uint16_t port, const std::stop_token& stopToken, std::chrono::milliseconds connectionTimeout);

    /// Receive up to @p buffer.size() bytes. Keeps reading until the buffer is full, @p timeout
    /// expires, @p stopToken fires, the peer closes the connection, or an error occurs.
    /// If any data was received before an interruption, returns the filled portion of the buffer.
    /// Timeout, EoS, Stopped, and RecvError are only returned when no data was received.
    RecvResult recvExact(std::span<uint8_t> buffer, std::chrono::milliseconds timeout, const std::stop_token& stopToken);

    ~TCPSocket();
    TCPSocket(TCPSocket&&) noexcept;
    TCPSocket& operator=(TCPSocket&&) noexcept;

    TCPSocket(const TCPSocket&) = delete;
    TCPSocket& operator=(const TCPSocket&) = delete;

private:
    explicit TCPSocket(int socketFd, std::unique_ptr<StopEventFD> stopEvent);

    int socketFd = -1;
    std::unique_ptr<StopEventFD> stopEvent;
};

}
