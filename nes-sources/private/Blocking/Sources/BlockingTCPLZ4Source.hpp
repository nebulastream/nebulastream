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
#include <Sources/BlockingSource.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/socket.h> /// For socket functions
#include <sys/types.h>
#include <lz4.h>
#include <lz4frame.h>

namespace NES::Sources
{

/// Defines the names, (optional) default values, (optional) validation & config functions, for all TCPLZ4 config parameters.
struct ConfigParametersBlockingTCPLZ4
{
    static inline const DescriptorConfig::ConfigParameter<std::string> HOST{
        "socketHost",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(HOST, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> PORT{
        "socketPort",
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
                NES_ERROR("BlockingTCPLZ4Source specified port is: {}, but ports must be between 0 and {}", portNumber.value(), PORT_NUMBER_MAX);
            }
            return portNumber;
        }};
    static inline const DescriptorConfig::ConfigParameter<int32_t> DOMAIN{
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
            NES_ERROR("BlockingTCPLZ4Source: Domain value is: {}, but the domain value must be AF_INET or AF_INET6", socketDomainString);
            return std::nullopt;
        }};
    static inline const DescriptorConfig::ConfigParameter<int32_t> TYPE{
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
                "BlockingTCPLZ4Source: Socket type is: {}, but the socket type must be SOCK_STREAM, SOCK_DGRAM, SOCK_SEQPACKET, SOCK_RAW, or "
                "SOCK_RDM",
                socketTypeString)
            return std::nullopt;
        }};
    static inline const DescriptorConfig::ConfigParameter<char> SEPARATOR{
        "tupleDelimiter",
        '\n',
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SEPARATOR, config); }};
    static inline const DescriptorConfig::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flushIntervalMs",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> SOCKET_BUFFER_SIZE{
        "socketBufferSize",
        1024,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(SOCKET_BUFFER_SIZE, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> SOCKET_BUFFER_TRANSFER_SIZE{
        "bytesUsedForSocketBufferSizeTransfer",
        0,
        [](const std::unordered_map<std::string, std::string>& config)
        { return DescriptorConfig::tryGet(SOCKET_BUFFER_TRANSFER_SIZE, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT{
        "connectTimeoutSeconds",
        10,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECT_TIMEOUT, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap,
            HOST,
            PORT,
            DOMAIN,
            TYPE,
            SEPARATOR,
            FLUSH_INTERVAL_MS,
            SOCKET_BUFFER_SIZE,
            SOCKET_BUFFER_TRANSFER_SIZE);
};

class BlockingTCPLZ4Source : public BlockingSource
{
    constexpr static ssize_t INVALID_RECEIVED_BUFFER_SIZE = -1;
    /// A return value of '0' means an EoF in the context of a read(socket..) (https://man.archlinux.org/man/core/man-pages/read.2.en)
    constexpr static ssize_t EOF_RECEIVED_BUFFER_SIZE = 0;
    /// We implicitly add one microsecond to avoid operation from never timing out
    /// (https://linux.die.net/man/7/socket)
    constexpr static suseconds_t IMPLICIT_TIMEOUT_USEC = 1;
    constexpr static std::chrono::microseconds TCP_SOCKET_DEFAULT_TIMEOUT{100000};
    constexpr static size_t ERROR_MESSAGE_BUFFER_SIZE = 256;

public:
    static constexpr std::string_view NAME = "TCPLZ4";

    explicit BlockingTCPLZ4Source(const SourceDescriptor& sourceDescriptor);
    ~BlockingTCPLZ4Source() override = default;

    BlockingTCPLZ4Source(const BlockingTCPLZ4Source&) = delete;
    BlockingTCPLZ4Source& operator=(const BlockingTCPLZ4Source&) = delete;
    BlockingTCPLZ4Source(BlockingTCPLZ4Source&&) = delete;
    BlockingTCPLZ4Source& operator=(BlockingTCPLZ4Source&&) = delete;

    size_t fillBuffer(IOBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open TCP connection.
    void open() override;
    /// Close TCP connection.
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    bool fillBuffer(IOBuffer& tupleBuffer, size_t& numReceivedBytes);

    int connection = -1;
    int sockfd = -1;

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

    /// buffer for thread-safe strerror_r
    std::array<char, ERROR_MESSAGE_BUFFER_SIZE> errBuffer;

    /// Decompression context to keep track of decompression metadata during decoding of source input
    LZ4F_decompressionContext_t decompCtx;
    /// Amount of bytes that were obtained by last read.
    ssize_t receivedCompressedBytes = 0;
    /// Position of first byte in srcBuffer that has yet to be decoded and emitted. If srcBufferPos == receivedCompressedBytes, we decoded all data from the last read and can proceed with the next read
    ssize_t srcBufferPos = 0;
    /// Data from the last socket read. Must be decoded and emitted before next read
    /// Since we cannot know in advance, how large the decompressed data chunk of the read will be, we can not adjust the read to fit IOBuffer size.
    /// Therefore, there might be some leftover data of the last read, that has to be emitted in the next fillBuffer call.
    std::vector<char> srcBuffer;
};

}
