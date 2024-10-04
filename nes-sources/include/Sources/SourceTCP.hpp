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

#include <cstdint>
#include <memory>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/EnumWrapper.hpp>
#include <Sources/Parsers/Parser.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SourcesUtils/MMapCircularBuffer.hpp>
#include <sys/socket.h> /// For socket functions
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Sources
{

/// Defines the names, (optional) default values, (optional) validation & config functions, for all TCP config parameters.
struct ConfigParametersTCP
{
    static inline const SourceDescriptor::ConfigParameter<EnumWrapper, Configurations::InputFormat> INPUT_FORMAT{
        "inputFormat", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return SourceDescriptor::tryGet(INPUT_FORMAT, config);
        }};
    static inline const SourceDescriptor::ConfigParameter<std::string> HOST{
        "socketHost", std::nullopt, [](const std::unordered_map<std::string, std::string>& config) {
            return SourceDescriptor::tryGet(HOST, config);
        }};
    static inline const SourceDescriptor::ConfigParameter<uint32_t> PORT{
        "socketPort",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        {
            /// Mandatory (no default value)
            const auto portNumber = SourceDescriptor::tryGet(PORT, config);
            if (portNumber.has_value())
            {
                constexpr uint32_t PORT_NUMBER_MAX = 65535;
                if (portNumber.value() <= PORT_NUMBER_MAX)
                {
                    return portNumber;
                }
                NES_ERROR("SourceTCP specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};
    static inline const SourceDescriptor::ConfigParameter<int32_t> DOMAIN{
        "socketDomain",
        AF_INET,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int>
        {
            /// User specified value, set if input is valid, throw if not.
            const auto socketDomainString = config.at(DOMAIN);
            if (strcasecmp(socketDomainString.c_str(), "AF_INET") == 0)
            {
                return (AF_INET);
            }
            if (strcasecmp(socketDomainString.c_str(), "AF_INET6") == 0)
            {
                return AF_INET6;
            }
            NES_ERROR("SourceTCP: Domain value is: {}, but the domain value must be AF_INET or AF_INET6", socketDomainString);
            return std::nullopt;
        }};
    static inline const SourceDescriptor::ConfigParameter<int32_t> TYPE{
        "socketType",
        SOCK_STREAM,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int>
        {
            const auto socketTypeString = config.at(TYPE);
            if (strcasecmp(socketTypeString.c_str(), "SOCK_STREAM") == 0)
            {
                return SOCK_STREAM;
            }
            else if (strcasecmp(socketTypeString.c_str(), "SOCK_DGRAM") == 0)
            {
                return SOCK_DGRAM;
            }
            else if (strcasecmp(socketTypeString.c_str(), "SOCK_SEQPACKET") == 0)
            {
                return SOCK_SEQPACKET;
            }
            else if (strcasecmp(socketTypeString.c_str(), "SOCK_RAW") == 0)
            {
                return SOCK_RAW;
            }
            else if (strcasecmp(socketTypeString.c_str(), "SOCK_RDM") == 0)
            {
                return SOCK_RDM;
            }
            else
            {
                NES_ERROR(
                    "SourceTCP: Socket type is: {}, but the socket type must be SOCK_STREAM, SOCK_DGRAM, SOCK_SEQPACKET, SOCK_RAW, or "
                    "SOCK_RDM",
                    socketTypeString)
                return std::nullopt;
            }
        }};
    static inline const SourceDescriptor::ConfigParameter<char> SEPARATOR{
        "tupleSeparator", '\n', [](const std::unordered_map<std::string, std::string>& config) {
            return SourceDescriptor::tryGet(SEPARATOR, config);
        }};
    static inline const SourceDescriptor::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flushIntervalMS", 0, [](const std::unordered_map<std::string, std::string>& config) {
            return SourceDescriptor::tryGet(FLUSH_INTERVAL_MS, config);
        }};
    static inline const SourceDescriptor::ConfigParameter<EnumWrapper, Configurations::TCPDecideMessageSize> DECIDED_MESSAGE_SIZE{
        "decideMessageSize",
        EnumWrapper::create<Configurations::TCPDecideMessageSize>(Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR),
        [](const std::unordered_map<std::string, std::string>& config) { return SourceDescriptor::tryGet(DECIDED_MESSAGE_SIZE, config); }};
    static inline const SourceDescriptor::ConfigParameter<uint32_t> SOCKET_BUFFER_SIZE{
        "socketBufferSize", 1024, [](const std::unordered_map<std::string, std::string>& config) {
            return SourceDescriptor::tryGet(SOCKET_BUFFER_SIZE, config);
        }};
    static inline const SourceDescriptor::ConfigParameter<uint32_t> SOCKET_BUFFER_TRANSFER_SIZE{
        "bytesUsedForSocketBufferSizeTransfer", 0, [](const std::unordered_map<std::string, std::string>& config) {
            return SourceDescriptor::tryGet(SOCKET_BUFFER_TRANSFER_SIZE, config);
        }};

    static inline std::unordered_map<std::string, SourceDescriptor::ConfigParameterContainer> parameterMap
        = SourceDescriptor::createConfigParameterContainerMap(
            INPUT_FORMAT,
            HOST,
            PORT,
            DOMAIN,
            TYPE,
            SEPARATOR,
            FLUSH_INTERVAL_MS,
            DECIDED_MESSAGE_SIZE,
            SOCKET_BUFFER_SIZE,
            SOCKET_BUFFER_TRANSFER_SIZE);
};

class SourceTCP : public Source
{
    constexpr static std::chrono::microseconds TCP_SOCKET_DEFAULT_TIMEOUT{100000};

public:
    static inline const std::string NAME = "TCP";

    explicit SourceTCP(const Schema& schema, const SourceDescriptor& sourceDescriptor);
    ~SourceTCP() override = default;

    bool fillTupleBuffer(
        NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, std::shared_ptr<Schema> schema) override;

    /// Open TCP connection.
    void open() override;
    /// Close TCP connection.
    void close() override;

    static std::unique_ptr<SourceDescriptor::Config> validateAndFormat(std::unordered_map<std::string, std::string>&& config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    bool
    fillBuffer(NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, std::shared_ptr<Schema> schema);

    /// Converts buffersize in either binary (NES Format) or ASCII (Json and CSV)
    /// takes 'data', which is a data memory segment which contains the buffersize
    [[nodiscard]] size_t parseBufferSize(std::span<const char> data) const;

    std::vector<std::shared_ptr<NES::PhysicalType>> physicalTypes;
    std::shared_ptr<Sources::Parser> inputParser;
    int connection = -1;
    uint64_t tupleSize;
    int sockfd = -1;
    MMapCircularBuffer circularBuffer;

    SchemaPtr schema;
    Configurations::InputFormat inputFormat;
    std::string socketHost;
    std::string socketPort;
    int socketType;
    int socketDomain;
    Configurations::TCPDecideMessageSize decideMessageSize;
    char tupleSeparator;
    size_t socketBufferSize;
    size_t bytesUsedForSocketBufferSizeTransfer;
    float flushIntervalInMs;
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
};

using SourceTCPPtr = std::shared_ptr<SourceTCP>;
}
