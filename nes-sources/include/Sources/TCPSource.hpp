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
    static inline const SourceDescriptor::ConfigParameter<Configurations::InputFormat> INPUT_FORMAT{
        "inputFormat",
        [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig)
        {
            return SourceDescriptor::tryMandatoryConfigure(
                INPUT_FORMAT,
                config,
                validatedConfig,
                [](const std::map<std::string, std::string>& lambdaConfig)
                {
                    ///-Todo: could pass userSpecifiedInputFormat as argument to function, but needs templating.
                    const auto userSpecifiedInputFormat = SourceDescriptor::tryGet(INPUT_FORMAT, lambdaConfig);
                    if (not NES::Configurations::validateInputFormat(userSpecifiedInputFormat.value()))
                    {
                        NES_ERROR(
                            "TCPSource: mandatory input format was: {}, which is not supported.",
                            magic_enum::enum_name(userSpecifiedInputFormat.value()));
                        return false;
                    }
                    return true;
                });
        }};
    static inline const SourceDescriptor::ConfigParameter<std::string> HOST{
        "socketHost", [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig) {
            return SourceDescriptor::tryMandatoryConfigure(HOST, config, validatedConfig);
        }};
    static inline const SourceDescriptor::ConfigParameter<uint32_t> PORT{
        "socketPort",
        [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig)
        {
            /// Mandatory (no default value)
            if (const auto portNumber = SourceDescriptor::tryGet(PORT, config); portNumber.has_value())
            {
                constexpr uint32_t PORT_NUMBER_MAX = 65535;
                if (portNumber.value() <= PORT_NUMBER_MAX)
                {
                    validatedConfig.emplace(std::make_pair(PORT, portNumber.value()));
                    return true;
                }
                NES_ERROR("TCPSource specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return false;
        }};
    static inline const SourceDescriptor::ConfigParameter<int32_t> DOMAIN{
        "socketDomain",
        [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig)
        {
            /// Set DEFAULT if not specified by user.
            if (not config.contains(DOMAIN))
            {
                validatedConfig.emplace(std::make_pair(DOMAIN, AF_INET));
            }

            /// User specified value, set if input is valid, throw if not.
            const auto socketDomainString = config.at(DOMAIN);
            if (strcasecmp(socketDomainString.c_str(), "AF_INET") == 0)
            {
                validatedConfig.emplace(std::make_pair(DOMAIN, AF_INET));
            }
            else if (strcasecmp(socketDomainString.c_str(), "AF_INET6") == 0)
            {
                validatedConfig.emplace(std::make_pair(DOMAIN, AF_INET6));
            }
            else
            {
                NES_ERROR("TCPSource: Domain value is: {}, but the domain value must be AF_INET or AF_INET6", socketDomainString);
                return false;
            }
            return true;
        }};
    static inline const SourceDescriptor::ConfigParameter<int32_t> TYPE{
        "socketType",
        [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig)
        {
            if (not config.contains(TYPE))
            {
                validatedConfig.emplace(std::make_pair(DOMAIN, SOCK_STREAM));
            }

            const auto socketTypeString = config.at(TYPE);
            if (strcasecmp(socketTypeString.c_str(), "SOCK_STREAM") == 0)
            {
                validatedConfig.emplace(std::make_pair(TYPE, SOCK_STREAM));
            }
            else if (strcasecmp(socketTypeString.c_str(), "SOCK_DGRAM") == 0)
            {
                validatedConfig.emplace(std::make_pair(TYPE, SOCK_DGRAM));
            }
            else if (strcasecmp(socketTypeString.c_str(), "SOCK_SEQPACKET") == 0)
            {
                validatedConfig.emplace(std::make_pair(TYPE, SOCK_SEQPACKET));
            }
            else if (strcasecmp(socketTypeString.c_str(), "SOCK_RAW") == 0)
            {
                validatedConfig.emplace(std::make_pair(TYPE, SOCK_RAW));
            }
            else if (strcasecmp(socketTypeString.c_str(), "SOCK_RDM") == 0)
            {
                validatedConfig.emplace(std::make_pair(TYPE, SOCK_RDM));
            }
            else
            {
                NES_ERROR(
                    "TCPSource: Socket type is: {}, but the socket type must be SOCK_STREAM, SOCK_DGRAM, SOCK_SEQPACKET, SOCK_RAW, or "
                    "SOCK_RDM",
                    socketTypeString)
                return false;
            }
            return true;
        }};
    static inline const SourceDescriptor::ConfigParameter<char> SEPARATOR{
        "tupleSeparator", [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig) {
            return SourceDescriptor::tryOptionalConfigure('\n', SEPARATOR, config, validatedConfig);
        }};
    static inline const SourceDescriptor::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flushIntervalMS", [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig) {
            return SourceDescriptor::tryOptionalConfigure(0, FLUSH_INTERVAL_MS, config, validatedConfig);
        }};
    static inline const SourceDescriptor::ConfigParameter<Configurations::TCPDecideMessageSize> DECIDED_MESSAGE_SIZE{
        "decideMessageSize",
        [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig)
        {
            return SourceDescriptor::tryOptionalConfigure(
                Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR, DECIDED_MESSAGE_SIZE, config, validatedConfig);
        }};
    static inline const SourceDescriptor::ConfigParameter<uint32_t> SOCKET_BUFFER_SIZE{
        "socketBufferSize", [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig) {
            return SourceDescriptor::tryOptionalConfigure(1024, SOCKET_BUFFER_SIZE, config, validatedConfig);
        }};
    static inline const SourceDescriptor::ConfigParameter<uint32_t> SOCKET_BUFFER_TRANSFER_SIZE{
        "bytesUsedForSocketBufferSizeTransfer",
        [](const std::map<std::string, std::string>& config, SourceDescriptor::Config& validatedConfig)
        { return SourceDescriptor::tryOptionalConfigure(0, SOCKET_BUFFER_TRANSFER_SIZE, config, validatedConfig); }};

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

class TCPSource : public Source
{
    /// TODO #74: make timeout configurable via descriptor
    constexpr static std::chrono::microseconds TCP_SOCKET_DEFAULT_TIMEOUT{100000};

public:
    static inline const std::string NAME = "TCP";

    explicit TCPSource(const Schema& schema, const SourceDescriptor& sourceDescriptor);
    ~TCPSource() override = default;

    bool fillTupleBuffer(
        NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, std::shared_ptr<Schema> schema) override;

    /// Open TCP connection.
    void open() override;
    /// Close TCP connection.
    void close() override;

    static SourceDescriptor::Config validateAndFormat(std::map<std::string, std::string>&& config);

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

using TCPSourcePtr = std::shared_ptr<TCPSource>;
}
