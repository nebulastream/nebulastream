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
#include <limits>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
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

/// A source that connects (client-only) to a remote TCP server and fills TupleBuffers with the raw bytes it reads off the socket.
/// The source does no framing or parsing: the configured input formatter owns the wire format (e.g. newline-delimited JSON),
/// the same byte-pipe role as MQTTSource.
///
/// End-of-stream is the peer closing its write side (TCP's native EoS):
/// the remaining buffered bytes are delivered and the next fill returns EoS.
/// A connection that merely goes silent is bridged indefinitely.
/// Unlike MQTT there is no idle budget, because TCP has a real end-of-stream signal; the engine's stop token ends the wait.
/// Any other socket failure (reset, link loss) fails the query: there is no broker to buffer for us,
/// so reconnecting would silently lose whatever was in flight.
class TCPSource : public Source
{
public:
    static constexpr std::string_view NAME = "TCP";

    explicit TCPSource(const SourceDescriptor& sourceDescriptor);
    ~TCPSource() override = default;

    TCPSource(const TCPSource&) = delete;
    TCPSource& operator=(const TCPSource&) = delete;
    TCPSource(TCPSource&&) = delete;
    TCPSource& operator=(TCPSource&&) = delete;

    /// Connect to the remote server.
    /// When this returns, the TCP connection is established, and a peer that accepted it can start sending immediately.
    /// Failure to resolve or connect within `CONNECT_TIMEOUT_MS` throws CannotOpenSource.
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
/// NOLINTBEGIN(cert-err58-cpp, bugprone-throwing-static-initialization): every source plugin declares its parameters this way.
/// Their initializers can in theory throw `std::bad_alloc` at static-init time, a state the runtime cannot recover from anyway.
struct ConfigParametersTCPSource
{
    /// NOTE: the connection-target keys are `SOCKET_HOST` / `SOCKET_PORT`, not `HOST` / `PORT`: the SQL binder reserves a bare
    /// `HOST` config key for worker placement and strips it before the source ever sees it.
    static inline const DescriptorConfig::ConfigParameter<std::string> SOCKET_HOST{
        "SOCKET_HOST",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SOCKET_HOST, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> SOCKET_PORT{
        "SOCKET_PORT",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto port = DescriptorConfig::tryGet(SOCKET_PORT, config);
            if (port && (*port < 1 || *port > std::numeric_limits<uint16_t>::max()))
            {
                NES_ERROR("TCPSource: SOCKET_PORT is: {}, but must be in [1, {}].", *port, std::numeric_limits<uint16_t>::max());
                return std::nullopt;
            }
            return port;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT_MS{
        "CONNECT_TIMEOUT_MS",
        5000,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECT_TIMEOUT_MS, config); }};

    /// Upper bound on how long a partially filled buffer is held back before it is handed downstream (a latency knob).
    /// 0 means "flush as soon as any bytes arrived".
    /// An empty buffer is not subject to this interval: the source waits for data, peer close, or stop indefinitely,
    /// because TCP signals end-of-stream explicitly and silence is not an error.
    static inline const DescriptorConfig::ConfigParameter<uint32_t> FLUSH_INTERVAL_MS{
        "FLUSH_INTERVAL_MS",
        100,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, SOCKET_HOST, SOCKET_PORT, CONNECT_TIMEOUT_MS, FLUSH_INTERVAL_MS);
};

/// NOLINTEND(cert-err58-cpp, bugprone-throwing-static-initialization)

}
