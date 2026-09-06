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
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>

namespace NES
{

/// NOTE: LLMRSource is a near-verbatim CLONE of nes-plugins/Sources/TCPSource.
/// The only behavioural difference is the LLM_QUERY_ID handshake (see
/// LLMRSource::open): the LLM Operator multiplexes several queries over one
/// TCP port and routes a connection by the query id it receives as the first
/// line. Everything else -- socket setup, buffer filling, systest adaptors --
/// is TCPSource's. Because this is a clone rather than a subclass, it must be
/// RE-SYNCED whenever TCPSource changes upstream.

/// Defines the names, (optional) default values, (optional) validation & config functions, for all LLMR config parameters.
struct ConfigParametersLLMR
{
    static inline const DescriptorConfig::ConfigParameter<std::string> HOST{
        "SOCKET_HOST",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HOST, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "SOCKET_PORT",
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
                NES_ERROR("LLMRSource specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};
    static inline const DescriptorConfig::ConfigParameter<int32_t> DOMAIN{
        "SOCKET_DOMAIN",
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
            NES_ERROR("LLMRSource: Domain value is: {}, but the domain value must be AF_INET or AF_INET6", socketDomainString);
            return std::nullopt;
        }};
    static inline const DescriptorConfig::ConfigParameter<int32_t> TYPE{
        "SOCKET_TYPE",
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
                "LLMRSource: Socket type is: {}, but the socket type must be SOCK_STREAM, SOCK_DGRAM, SOCK_SEQPACKET, SOCK_RAW, or "
                "SOCK_RDM",
                socketTypeString)
            return std::nullopt;
        }};
    static inline const DescriptorConfig::ConfigParameter<char> SEPARATOR{
        "TUPLE_DELIMITER",
        '\n',
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SEPARATOR, config); }};
    static inline const DescriptorConfig::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "FLUSH_INTERVAL_MS",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> SOCKET_BUFFER_SIZE{
        "SOCKET_BUFFER_SIZE",
        1024,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SOCKET_BUFFER_SIZE, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> SOCKET_BUFFER_TRANSFER_SIZE{
        "BYTES_SED_FOR_SOCKET_BUFFER_SIZE_TRANSFER",
        0,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(SOCKET_BUFFER_TRANSFER_SIZE, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT{
        "CONNECT_TIMEOUT_SECONDS",
        10,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECT_TIMEOUT, config); }};

    /// Identifies which registered query's result stream this source wants.
    /// Sent verbatim (plus a newline) as the first bytes after connecting;
    /// must match the `query_id` of the SINK.QUERY blob the LLMSink registered.
    /// Mandatory: there is no sensible default, and a wrong id silently yields
    /// an empty stream.
    static inline const DescriptorConfig::ConfigParameter<std::string> LLM_QUERY_ID{
        "LLM_QUERY_ID",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(LLM_QUERY_ID, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap,
            LLM_QUERY_ID,
            HOST,
            PORT,
            DOMAIN,
            TYPE,
            SEPARATOR,
            FLUSH_INTERVAL_MS,
            SOCKET_BUFFER_SIZE,
            SOCKET_BUFFER_TRANSFER_SIZE,
            CONNECT_TIMEOUT);
};

class LLMRSource : public Source
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
        static const std::string Instance = "LLMR";
        return Instance;
    }

    explicit LLMRSource(const SourceDescriptor& sourceDescriptor);
    ~LLMRSource() override = default;

    LLMRSource(const LLMRSource&) = delete;
    LLMRSource& operator=(const LLMRSource&) = delete;
    LLMRSource(LLMRSource&&) = delete;
    LLMRSource& operator=(LLMRSource&&) = delete;

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open the TCP connection and send the LLM_QUERY_ID handshake.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Close the TCP connection.
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    /// Systest adaptors: materialize inline/file test data by spinning up a LLMRDataServer.
    static InlineDataRegistryReturnType provideInlineData(InlineDataRegistryArguments systestAdaptorArguments);
    static FileDataRegistryReturnType provideFileData(FileDataRegistryArguments systestAdaptorArguments);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    bool tryToConnect(const addrinfo* result, int flags);
    bool fillBuffer(TupleBuffer& tupleBuffer, size_t& numReceivedBytes);

    int connection = -1;
    int sockfd = -1;

    /// buffer for thread-safe strerror_r
    std::array<char, ERROR_MESSAGE_BUFFER_SIZE> errBuffer;

    std::string llmQueryId;
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

}
