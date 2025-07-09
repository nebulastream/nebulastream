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
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <unordered_map>
#include <netdb.h>
#include <Configurations/Descriptor.hpp>
#include <Configurations/Enums/EnumWrapper.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/socket.h> /// For socket functions
#include <sys/types.h>

namespace NES::Sources
{

class TCPSource : public Source
{
    constexpr static ssize_t INVALID_RECEIVED_BUFFER_SIZE = -1;
    /// A return value of '0' means an EoF in the context of a read(socket..) (https://man.archlinux.org/man/core/man-pages/read.2.en)
    constexpr static ssize_t EOF_RECEIVED_BUFFER_SIZE = 0;
    /// We implicitly add one microsecond to avoid operation from never timing out
    /// (https://linux.die.net/man/7/socket)
    constexpr static suseconds_t IMPLICIT_TIMEOUT_USEC = 1;
    constexpr static size_t ERROR_MESSAGE_BUFFER_SIZE = 256;

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

    size_t fillTupleBuffer(
        NES::Memory::TupleBuffer& tupleBuffer, Memory::AbstractBufferProvider& bufferProvider, const std::stop_token& stopToken) override;

    /// Open TCP connection.
    void open() override;
    /// Close TCP connection.
    void close() override;

    static NES::Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    bool tryToConnect(const addrinfo* result, int flags);
    bool fillBuffer(NES::Memory::TupleBuffer& tupleBuffer, size_t& numReceivedBytes);

    int connection = -1;
    int sockfd = -1;

    /// buffer for thread-safe strerror_r
    std::array<char, ERROR_MESSAGE_BUFFER_SIZE> errBuffer;

    std::string socketHost;
    std::string socketPort;
    int socketType;
    int socketDomain;
    char tupleDelimiter;
    size_t socketBufferSize;
    size_t bytesUsedForSocketBufferSizeTransfer;
    float flushIntervalInMs;
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
    u_int32_t connectionTimeout;
};


/// Defines the names, (optional) default values, (optional) validation & config functions, for all TCP config parameters.
struct ConfigParametersTCP
{
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<std::string> HOST{
        "socketHost",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return NES::Configurations::DescriptorConfig::tryGet(HOST, config); }};
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "socketPort",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            /// Mandatory (no default value)
            const auto portNumber = NES::Configurations::DescriptorConfig::tryGet(PORT, config);
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
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<int32_t> DOMAIN{
        "socketDomain",
        AF_INET,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int>
        {
            /// User specified value, set if input is valid, throw if not.
            const auto& socketDomainString = config.at(DOMAIN);
            if (strcasecmp(socketDomainString.c_str(), "AF_INET") == 0)
            {
                return (AF_INET);
            }
            if (strcasecmp(socketDomainString.c_str(), "AF_INET6") == 0)
            {
                return AF_INET6;
            }
            NES_ERROR("TCPSource: Domain value is: {}, but the domain value must be AF_INET or AF_INET6", socketDomainString);
            return std::nullopt;
        }};
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<int32_t> TYPE{
        "socketType",
        SOCK_STREAM,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int>
        {
            auto socketTypeString = config.at(TYPE);
            for (auto& character : socketTypeString)
            {
                character = toupper(character);
            }
            if (socketTypeString == "SOCK_STREAM")
            {
                return SOCK_STREAM;
            }
            if (socketTypeString == "SOCK_DGRAM")
            {
                return SOCK_DGRAM;
            }
            if (socketTypeString == "SOCK_SEQPACKET")
            {
                return SOCK_SEQPACKET;
            }
            if (socketTypeString == "SOCK_RAW")
            {
                return SOCK_RAW;
            }
            if (socketTypeString == "SOCK_RDM")
            {
                return SOCK_RDM;
            }
            NES_ERROR(
                "TCPSource: Socket type is: {}, but the socket type must be SOCK_STREAM, SOCK_DGRAM, SOCK_SEQPACKET, SOCK_RAW, or "
                "SOCK_RDM",
                socketTypeString)
            return std::nullopt;
        }};
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<char> SEPARATOR{
        "tupleDelimiter",
        '\n',
        [](const std::unordered_map<std::string, std::string>& config)
        { return NES::Configurations::DescriptorConfig::tryGet(SEPARATOR, config); }};
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flushIntervalMS",
        0,
        [](const std::unordered_map<std::string, std::string>& config)
        { return NES::Configurations::DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<uint32_t> SOCKET_BUFFER_SIZE{
        "socketBufferSize",
        1024,
        [](const std::unordered_map<std::string, std::string>& config)
        { return NES::Configurations::DescriptorConfig::tryGet(SOCKET_BUFFER_SIZE, config); }};
    static inline const NES::Configurations::DescriptorConfig::ConfigParameter<uint32_t> SOCKET_BUFFER_TRANSFER_SIZE{
        "bytesUsedForSocketBufferSizeTransfer",
        0,
        [](const std::unordered_map<std::string, std::string>& config)
        { return NES::Configurations::DescriptorConfig::tryGet(SOCKET_BUFFER_TRANSFER_SIZE, config); }};
    static inline const Configurations::DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT{
        "connectTimeoutSeconds",
        10,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(CONNECT_TIMEOUT, config); }};

    static inline std::unordered_map<std::string, NES::Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = NES::Configurations::DescriptorConfig::createConfigParameterContainerMap(
            HOST, PORT, DOMAIN, TYPE, SEPARATOR, FLUSH_INTERVAL_MS, SOCKET_BUFFER_SIZE, SOCKET_BUFFER_TRANSFER_SIZE, CONNECT_TIMEOUT);
};

}
