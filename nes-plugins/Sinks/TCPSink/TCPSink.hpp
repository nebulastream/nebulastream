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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/BackpressureHandler.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Formatter.hpp>
#include <BackpressureChannel.hpp>
#include <PipelineExecutionContext.hpp>
#include <TCPChannel.hpp>

namespace NES
{

/// Streams the bytes produced by the configured output formatter to a TCP server.
/// Delivery is best-effort across reconnects: data sent before a disconnect may be lost, and the buffer being written
/// is sent again from its start on the new connection.
class TCPSink final : public Sink
{
public:
    static constexpr std::string_view NAME = "TCP";
    static constexpr auto BACKPRESSURE_RETRY_INTERVAL = std::chrono::milliseconds(10);

    explicit TCPSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~TCPSink() override = default;

    TCPSink(const TCPSink&) = delete;
    TCPSink& operator=(const TCPSink&) = delete;
    TCPSink(TCPSink&&) = delete;
    TCPSink& operator=(TCPSink&&) = delete;

    void start(PipelineExecutionContext&) override;
    void stop(PipelineExecutionContext&) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& os) const override;

private:
    TCPChannel channel;
    BackpressureHandler backpressureHandler;
};

struct ConfigParametersTCPSink
{
    ///NOLINTBEGIN(cert-err58-cpp)
    static inline const DescriptorConfig::ConfigParameter<std::string> HOST{
        "SOCKET_HOST",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HOST, config); }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "SOCKET_PORT",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto port = DescriptorConfig::tryGet(PORT, config);
            if (!port || port.value() == 0 || port.value() > std::numeric_limits<uint16_t>::max())
            {
                return std::nullopt;
            }
            return port;
        }};

    /// Maximum time spent establishing or reestablishing a connection before failing the query.
    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT_MS{
        "CONNECT_TIMEOUT_MS",
        10000,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto timeout = DescriptorConfig::tryGet(CONNECT_TIMEOUT_MS, config);
            if (!timeout || timeout.value() == 0)
            {
                return std::nullopt;
            }
            return timeout;
        }};

    /// Maximum time stop waits without an accepted buffer being written before aborting the writer thread.
    static inline const DescriptorConfig::ConfigParameter<uint32_t> CLOSE_TIMEOUT_MS{
        "CLOSE_TIMEOUT_MS",
        10000,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint32_t>
        {
            const auto timeout = DescriptorConfig::tryGet(CLOSE_TIMEOUT_MS, config);
            if (!timeout || timeout.value() == 0)
            {
                return std::nullopt;
            }
            return timeout;
        }};

    /// Maximum number of buffers owned by the TCP channel's asynchronous writer.
    static inline const DescriptorConfig::ConfigParameter<size_t> MAX_QUEUED_BUFFERS{
        "MAX_QUEUED_BUFFERS",
        16,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<size_t>
        {
            const auto maximum = DescriptorConfig::tryGet(MAX_QUEUED_BUFFERS, config);
            if (!maximum || maximum.value() == 0)
            {
                return std::nullopt;
            }
            return maximum;
        }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SinkDescriptor::parameterMap, HOST, PORT, CONNECT_TIMEOUT_MS, CLOSE_TIMEOUT_MS, MAX_QUEUED_BUFFERS);
    ///NOLINTEND(cert-err58-cpp)
};

}

FMT_OSTREAM(NES::TCPSink);
