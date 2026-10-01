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

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <span>
#include <stop_token>
#include <string>
#include <variant>
#include <vector>
#include <Runtime/TupleBuffer.hpp>
#include <folly/MPMCQueue.h>
#include <folly/Synchronized.h>
#include <Thread.hpp>

namespace NES
{

/// A bounded producer queue backed by a single TCP I/O thread.
class TCPChannel
{
public:
    enum class SendResult : uint8_t
    {
        Accepted,
        Full,
    };

    /// Creates a TCP channel with a bounded queue. Call start() to launch the I/O thread.
    /// @param host Server hostname or IP address.
    /// @param port Server TCP port.
    /// @param connectionTimeout Timeout for connecting or reconnecting, including DNS resolution.
    /// @param closeTimeout Time allowed for accepted buffers to drain after the first tryClose() call.
    /// @param maxQueuedBuffers Maximum number of queued buffers, excluding the buffer being written.
    TCPChannel(
        std::string host,
        uint32_t port,
        std::chrono::milliseconds connectionTimeout,
        std::chrono::milliseconds closeTimeout,
        size_t maxQueuedBuffers);
    /// Stops and joins the I/O thread if still running.
    /// Pending buffers may be discarded; call tryClose() to drain them before destruction.
    ~TCPChannel();

    TCPChannel(const TCPChannel&) = delete;
    TCPChannel& operator=(const TCPChannel&) = delete;
    TCPChannel(TCPChannel&&) = delete;
    TCPChannel& operator=(TCPChannel&&) = delete;

    /// Launches the I/O thread to establish the connection and send queued buffers.
    /// Returns without waiting for the connection to be established.
    /// Connection errors are rethrown by subsequent trySend() or tryClose() calls.
    /// @pre The I/O thread has not already been started.
    void start();
    /// Attempts to enqueue a buffer without waiting for queue capacity.
    /// Retains a reference to the buffer when accepted.
    /// Rethrows any stored I/O thread error.
    /// @param buffer Buffer to send.
    /// @return Accepted if queued, or Full if the caller must retry.
    /// @pre The I/O thread has been started.
    SendResult trySend(const TupleBuffer& buffer);
    /// Attempts to close the channel, allowing accepted buffers to drain.
    /// The first call starts the close timeout; subsequent calls use the same deadline.
    /// Stops and joins the I/O thread when drained or timed out.
    /// Pending buffers may be discarded on timeout.
    /// Rethrows any stored I/O thread error.
    /// @return True once closed, or false if draining should be retried.
    /// @pre Producers have finished submitting buffers.
    bool tryClose();

    [[nodiscard]] const std::string& getHost() const { return host; }

    [[nodiscard]] uint32_t getPort() const { return port; }

private:
    class Socket;
    struct ResolvedAddress;
    struct Ok;

    struct Error
    {
        std::string message;
    };

    struct Stopped
    {
    };

    using ConnectResult = std::variant<Ok, Error, Stopped>;

    enum class IOResult : uint8_t
    {
        Complete,
        Disconnected,
        Stopped,
    };

    void run(const std::stop_token& stopToken);
    static std::optional<std::vector<ResolvedAddress>>
    resolveAddresses(const std::string& host, uint32_t port, std::chrono::milliseconds timeout, const std::stop_token& stopToken);
    static ConnectResult tryConnect(const ResolvedAddress& address, std::chrono::milliseconds timeout, const std::stop_token& stopToken);
    std::optional<Socket> connectUntil(std::chrono::milliseconds timeout, const std::stop_token& stopToken) const;
    static IOResult writeData(std::span<const char> data, Socket& socket, const std::stop_token& stopToken);
    IOResult writeBuffer(const TupleBuffer& buffer, std::optional<Socket>& socket, const std::stop_token& stopToken) const;
    void stop();
    void storeError(std::exception_ptr exception);
    void rethrowError() const;

    folly::MPMCQueue<TupleBuffer> queue;
    const std::chrono::milliseconds connectionTimeout;
    const std::chrono::milliseconds closeTimeout;
    std::atomic<size_t> pendingBuffers = 0;
    folly::Synchronized<std::exception_ptr> error;
    std::optional<std::chrono::steady_clock::time_point> closeDeadline;
    const std::string host;
    const uint32_t port;
    /// Declared last so it is joined before state accessed by the I/O thread is destroyed.
    Thread ioThread;
};

}
