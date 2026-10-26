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
#include <expected>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>

namespace NES
{

struct TCPSourceConfig
{
    struct SocketDestination
    {
        std::string socketHost;
        uint32_t socketPort;
    };

    /// To be left empty if overwriteable host and port is set, for example in the systests
    std::optional<SocketDestination> socketDestination;
    uint32_t flushIntervalMs{};
    uint32_t connectTimeoutMs{};

    static std::expected<TCPSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
};

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

    explicit TCPSource(const TCPSourceConfig& config);
    ~TCPSource() override = default;

    TCPSource(const TCPSource&) = delete;
    TCPSource& operator=(const TCPSource&) = delete;
    TCPSource(TCPSource&&) = delete;
    TCPSource& operator=(TCPSource&&) = delete;

    /// Connect to the remote server.
    /// When this returns, the TCP connection is established, and a peer that accepted it can start sending immediately.
    /// A failed resolution, or a connect that fails or does not finish within `CONNECT_TIMEOUT_MS`, throws CannotOpenSource.
    /// The host name is resolved before the timeout starts, so a slow DNS lookup delays open by the system resolver's own timeout.
    void open(std::shared_ptr<AbstractBufferProvider>) override;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Close the socket. Errors are ignored: the peer may already be gone, and
    /// tearing down a dead connection is not a query failure.
    void close() override;

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

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

}
