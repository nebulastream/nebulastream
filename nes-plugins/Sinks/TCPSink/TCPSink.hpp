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

#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace NES
{

struct ConfigParametersTCPSink
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
            const auto portNumber = DescriptorConfig::tryGet(PORT, config);
            if (portNumber.has_value())
            {
                constexpr uint32_t PORT_NUMBER_MAX = 65535;
                if (portNumber.value() <= PORT_NUMBER_MAX)
                {
                    return portNumber;
                }
                NES_ERROR("TCPSink specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};

    static inline const DescriptorConfig::ConfigParameter<int32_t> DOMAIN{
        "socket_domain",
        AF_INET,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int32_t>
        {
            auto socketDomainString = config.at(DOMAIN);
            for (auto& character : socketDomainString)
            {
                character = static_cast<char>(toupper(static_cast<unsigned char>(character)));
            }
            if (socketDomainString == "AF_INET")
            {
                return AF_INET;
            }
            if (socketDomainString == "AF_INET6")
            {
                return AF_INET6;
            }
            NES_ERROR("TCPSink: Domain value is: {}, but the domain value must be AF_INET or AF_INET6", socketDomainString);
            return std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<int32_t> TYPE{
        "socket_type",
        SOCK_STREAM,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int32_t>
        {
            auto socketTypeString = config.at(TYPE);
            for (auto& character : socketTypeString)
            {
                character = static_cast<char>(toupper(static_cast<unsigned char>(character)));
            }
            if (socketTypeString == "SOCK_STREAM")
            {
                return SOCK_STREAM;
            }
            NES_ERROR("TCPSink: Socket type is: {}, but only SOCK_STREAM is currently supported", socketTypeString);
            return std::nullopt;
        }};

    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT{
        "connect_timeout_seconds",
        10,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECT_TIMEOUT, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(SinkDescriptor::parameterMap, HOST, PORT, DOMAIN, TYPE, CONNECT_TIMEOUT);
};

class TCPSink final : public Sink
{
    constexpr static suseconds_t IMPLICIT_TIMEOUT_USEC = 1;
    constexpr static size_t ERROR_MESSAGE_BUFFER_SIZE = 256;

public:
    static constexpr std::string_view NAME = "TCP";

    explicit TCPSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor);
    ~TCPSink() override = default;

    TCPSink(const TCPSink&) = delete;
    TCPSink& operator=(const TCPSink&) = delete;
    TCPSink(TCPSink&&) = delete;
    TCPSink& operator=(TCPSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    void tryToConnect(const addrinfo* result);
    void sendAll(const char* data, size_t length);
    void closeSocket();

    int socketFileDescriptor = -1;
    bool isOpen = false;
    std::array<char, ERROR_MESSAGE_BUFFER_SIZE> errBuffer{};
    std::string socketHost;
    std::string socketPort;
    int socketType;
    int socketDomain;
    uint32_t connectionTimeout;
};

}

namespace fmt
{
template <>
struct formatter<NES::TCPSink> : ostream_formatter
{
};
}
