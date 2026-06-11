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
#include <mutex>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

/// A sink that connects (client-only) to a remote TCP server and writes the
/// bytes of every incoming TupleBuffer to the socket. The sink does no
/// formatting of its own — by the time bytes reach it they are already the
/// serialized payload (OUTPUT_FORMAT is lowered into an upstream formatter
/// stage), the same byte-pipe role as MQTTSink.
///
/// Fail-fast: a write that errors or stalls past `write_timeout_ms` fails the
/// query (CannotOpenSink). There is no reconnect and no offline buffering —
/// TCP has no broker to queue for us, so riding out a connection loss would
/// silently drop whatever was in flight.
///
/// Thread-safety: execute() is called concurrently by many worker threads on
/// one TCPSink instance, but a single socket must be written by one thread at
/// a time (interleaved writes would corrupt the byte stream), so execute()
/// serializes on a mutex — same model as ODBCSink, contrast MQTTSink whose
/// paho client is internally thread-safe.
class TCPSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "TCP";

    explicit TCPSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    std::string host;
    uint32_t port;
    std::chrono::milliseconds connectTimeout;
    std::chrono::milliseconds writeTimeout;

    boost::asio::io_context ioContext;
    boost::asio::ip::tcp::socket socket{ioContext};
    /// Serializes execute() (and stop()): one socket, one writer at a time.
    std::mutex writeMutex;
};

/// Defines the names, (optional) default values, (optional) validation & config functions for all TCP sink config parameters.
/// NOLINTBEGIN(cert-err58-cpp): static-storage ConfigParameter initialization is the project-wide pattern for sink plugins
/// (MQTTSink, FileSink, etc.). The constructors can theoretically throw `std::bad_alloc`; in practice they are evaluated
/// once at static-init time on a path the runtime cannot meaningfully recover from. Refactoring would require redesigning
/// the ConfigParameter registry across every plugin — out of scope for any single PR.
struct ConfigParametersTCPSink
{
    /// NOTE: the connection-target keys are `socket_host` / `socket_port`, not `host` / `port`, for the same reason
    /// ODBCSink uses `db_host` / `db_port`: the SQL binder reserves a bare `host` config key for worker placement
    /// and strips it before the sink ever sees it.
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
                NES_ERROR("TCPSink: socket_port is: {}, but must be in [1, 65535].", *port);
                return std::nullopt;
            }
            return port;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT_MS{
        "connect_timeout_ms",
        5000,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECT_TIMEOUT_MS, config); }};

    /// Upper bound on a single buffer write. A peer (or link) that stops
    /// draining the stream for this long is treated as dead — fail-fast, since
    /// the sink has no way to buffer past the kernel's send queue.
    static inline const DescriptorConfig::ConfigParameter<uint32_t> WRITE_TIMEOUT_MS{
        "write_timeout_ms",
        10000,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(WRITE_TIMEOUT_MS, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap, SOCKET_HOST, SOCKET_PORT, CONNECT_TIMEOUT_MS, WRITE_TIMEOUT_MS);
};

/// NOLINTEND(cert-err58-cpp)

}

FMT_OSTREAM(NES::TCPSink);
