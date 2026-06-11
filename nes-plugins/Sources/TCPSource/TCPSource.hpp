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
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace NES
{

/// A source that connects (client-only) to a remote TCP server and fills
/// TupleBuffers with the raw bytes it reads off the socket. The source does no
/// framing or parsing — the configured input formatter owns the wire format
/// (e.g. newline-delimited JSON), the same byte-pipe role as MQTTSource.
///
/// End-of-stream is the peer closing its write side (TCP's native EoS): the
/// remaining buffered bytes are delivered and the next fill returns EoS. A
/// connection that merely goes silent is bridged indefinitely — unlike MQTT
/// there is no idle budget, because TCP has a real end-of-stream signal; the
/// engine's stop token ends the wait. Any other socket failure (reset, link
/// loss) fails the query: there is no broker to buffer for us, so reconnecting
/// would silently lose whatever was in flight.
class TCPSource : public Source
{
public:
    /// NOLINTNEXTLINE(cert-err58-cpp): static-storage std::string init is the project-wide plugin-name idiom; matches sibling Source plugins.
    static inline const std::string NAME = "TCP";

    explicit TCPSource(const SourceDescriptor& sourceDescriptor);
    ~TCPSource() override = default;

    TCPSource(const TCPSource&) = delete;
    TCPSource& operator=(const TCPSource&) = delete;
    TCPSource(TCPSource&&) = delete;
    TCPSource& operator=(TCPSource&&) = delete;

    /// Connect to the remote server. When this returns, the TCP connection is
    /// established — a peer that accepted the connection can start sending
    /// immediately. Failure to resolve/connect within `connect_timeout_ms`
    /// throws CannotOpenSource.
    void open(std::shared_ptr<AbstractBufferProvider>) override;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Close the socket. Errors are ignored: the peer may already be gone, and
    /// tearing down a dead connection is not a query failure.
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    std::string host;
    uint32_t port;
    std::chrono::milliseconds connectTimeout;
    std::chrono::milliseconds flushInterval;

    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::socket socket{ioContext};
    /// The peer closed its write side; everything it sent has been delivered.
    bool peerClosed = false;
};

/// Defines the names, (optional) default values, (optional) validation & config functions for all TCP source config parameters.
/// NOLINTBEGIN(cert-err58-cpp): static-storage ConfigParameter initialization is the project-wide pattern for source plugins
/// (MQTTSource, FileSource, etc.). The constructors can theoretically throw `std::bad_alloc`; in practice they are evaluated
/// once at static-init time on a path the runtime cannot meaningfully recover from. Refactoring would require redesigning the
/// ConfigParameter registry across every plugin — out of scope for any single PR.
struct ConfigParametersTCPSource
{
    /// NOTE: the connection-target keys are `socket_host` / `socket_port`, not `host` / `port`, for the same reason
    /// ODBCSource uses `db_host` / `db_port`: the SQL binder reserves a bare `host` config key for worker placement
    /// and strips it before the source ever sees it.
    static inline const DescriptorConfig::ConfigParameter<std::string> SOCKET_HOST{
        "socket_host",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SOCKET_HOST, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> SOCKET_PORT{
        "socket_port",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto port = DescriptorConfig::tryGet(SOCKET_PORT, config);
            if (port && (*port < 1 || *port > 65535))
            {
                NES_ERROR("TCPSource: socket_port is: {}, but must be in [1, 65535].", *port);
                return std::nullopt;
            }
            return port;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT_MS{
        "connect_timeout_ms",
        5000,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECT_TIMEOUT_MS, config); }};

    /// Upper bound on how long a *partially* filled buffer is held back before
    /// being handed downstream (latency knob). 0 means "flush as soon as any
    /// bytes arrived". An empty buffer is NOT subject to this interval — the
    /// source waits for data (or peer close / stop) indefinitely, because TCP
    /// signals end-of-stream explicitly and silence is not an error.
    static inline const DescriptorConfig::ConfigParameter<uint32_t> FLUSH_INTERVAL_MS{
        "flush_interval_ms",
        100,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, SOCKET_HOST, SOCKET_PORT, CONNECT_TIMEOUT_MS, FLUSH_INTERVAL_MS);
};

/// NOLINTEND(cert-err58-cpp)

}
