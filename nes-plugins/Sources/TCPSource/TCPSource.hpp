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
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <variant>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <TCPSocket.hpp>

namespace NES
{

/// Defines the names, (optional) default values, (optional) validation & config functions, for all TCP config parameters.
struct ConfigParametersTCP
{
    static inline const DescriptorConfig::ConfigParameter<std::string> HOST{
        "socket_host",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HOST, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "socket_port",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            /// Mandatory (no default value)
            const auto portNumber = DescriptorConfig::tryGet(PORT, config);
            if (portNumber.has_value())
            {
                constexpr uint32_t PORT_NUMBER_MAX = 65535;
                if (portNumber.value() <= PORT_NUMBER_MAX)
                {
                    return portNumber;
                }
                NES_ERROR("TCPSource specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};
    static inline const DescriptorConfig::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flush_interval_ms",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT_MS{
        "connect_timeout_ms",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECT_TIMEOUT_MS, config); }};
    static inline const DescriptorConfig::ConfigParameter<bool> AUTO_RECONNECT{
        "auto_reconnect",
        false,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(AUTO_RECONNECT, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, HOST, PORT, FLUSH_INTERVAL_MS, CONNECT_TIMEOUT_MS, AUTO_RECONNECT);
};

class TCPSource final : public Source
{
public:
    static const std::string& name()
    {
        static const std::string Instance = "TCP";
        return Instance;
    }

    explicit TCPSource(const SourceDescriptor& sourceDescriptor);
    ~TCPSource() override = default;

    TCPSource(const TCPSource&) = delete;
    TCPSource& operator=(const TCPSource&) = delete;
    TCPSource(TCPSource&&) = delete;
    TCPSource& operator=(TCPSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open TCP connection.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Close TCP connection.
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    struct Connecting
    {
        std::chrono::milliseconds backoff{0};
    };

    struct Receiving
    {
        TCPSocket socket;
    };

    struct EoS
    {
    };

    using State = std::variant<Connecting, Receiving, EoS>;

    State state = Connecting{};

    std::string socketHost;
    uint16_t socketPort;
    std::chrono::milliseconds flushInterval;
    uint64_t generatedBuffers{0};
    std::chrono::milliseconds connectionTimeout;
    bool autoReconnect;
};

}
