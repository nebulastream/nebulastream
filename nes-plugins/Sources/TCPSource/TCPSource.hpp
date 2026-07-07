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
#include "Configurations/ConfigValue.hpp"

namespace NES
{

/// Defines the names, (optional) default values, (optional) validation & config functions, for all TCP config parameters.
struct ConfigParametersTCP
{
    static inline const ConfigField<std::string> HOST{
        "SOCKET_HOST", [](const ConfigLiteral& config) { return NES::tryGetOr<std::string>(config, expectedType<std::string>()); }};

    static inline const ConfigField<uint32_t> PORT{
        "SOCKET_PORT",
        [](const ConfigLiteral& literal)
        {
            return NES::tryGetOr<std::uint64_t>(literal, expectedType<std::uint64_t>())
                .and_then(downcastConfigValue<std::uint64_t, uint32_t, 65535>);
        }};
    static inline const ConfigField<int32_t> DOMAIN{
        "SOCKET_DOMAIN",
        [](const ConfigLiteral& literal)
        {
            return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
                .and_then(
                    [](const std::string& value) -> std::expected<int32_t, Exception>
                    {
                        if (strcasecmp(value.c_str(), "AF_INET") == 0)
                        {
                            return AF_INET;
                        }
                        if (strcasecmp(value.c_str(), "AF_INET6") == 0)
                        {
                            return AF_INET6;
                        }
                        return std::unexpected{InvalidConfigParameter(
                            "TCPSource: Domain value is: {}, but the domain value must be AF_INET or AF_INET6", value)};
                    });
        },
        [] { return AF_INET; } /// For some reason I couldn't pass the macro as an argument here
    };

    static inline const ConfigField<int32_t> TYPE{
        "SOCKET_TYPE",
        [](const ConfigLiteral& literal)
        {
            return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
                .and_then(Identifier::tryParse)
                .and_then(
                    [](const Identifier& socketType) -> std::expected<int32_t, Exception>
                    {
                        if (socketType == Identifier::parse("SOCK_STREAM"))
                        {
                            return SOCK_STREAM;
                        }
                        if (socketType == Identifier::parse("SOCK_DGRAM"))
                        {
                            return SOCK_DGRAM;
                        }
                        if (socketType == Identifier::parse("SOCK_SEQPACKET"))
                        {
                            return SOCK_SEQPACKET;
                        }
                        if (socketType == Identifier::parse("SOCK_RAW"))
                        {
                            return SOCK_RAW;
                        }
                        if (socketType == Identifier::parse("SOCK_RDM"))
                        {
                            return SOCK_RDM;
                        }
                        return std::unexpected{InvalidConfigParameter(
                            "TCPSource: Socket type is: {}, but the socket type must be SOCK_STREAM, SOCK_DGRAM, SOCK_SEQPACKET, SOCK_RAW, "
                            "or "
                            "SOCK_RDM",
                            socketType)};
                    });
        },
        SOCK_STREAM,
    };
    static inline const ConfigField<char> SEPARATOR{
        "TUPLE_DELIMITER",
        [](const ConfigLiteral& literal)
        {
            return NES::tryGetOr<std::string>(literal, expectedType<std::string>())
                .and_then(
                    [](const std::string& value) -> std::expected<char, Exception>
                    {
                        if (std::ranges::size(value) == 1)
                        {
                            return value.front();
                        }
                        return std::unexpected{InvalidConfigParameter("Expected a single character, got {}", value)};
                    });
        },
        '\n',
    };
    static inline const ConfigField<float> FLUSH_INTERVAL_MS{
        "FLUSH_INTERVAL_MS",
        [](const ConfigLiteral& literal) -> std::expected<float, Exception>
        { return NES::tryGetOr<double>(literal, expectedType<float>()).and_then(downcastConfigValue<double, float>); },
        1.0};

    static inline const ConfigField<uint32_t> SOCKET_BUFFER_SIZE{
        "SOCKET_BUFFER_SIZE",
        [](const ConfigLiteral& config)
        { return NES::tryGetOr<uint64_t>(config, expectedType<uint64_t>()).and_then(downcastConfigValue<uint64_t, uint32_t>); },
        1024};
    static inline const ConfigField<uint32_t> SOCKET_BUFFER_TRANSFER_SIZE{
        "BYTES_SED_FOR_SOCKET_BUFFER_SIZE_TRANSFER",
        [](const ConfigLiteral& literal)
        { return NES::tryGetOr<uint64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<uint64_t, uint32_t>);
        },
        0};
    static inline const ConfigField<uint32_t> CONNECT_TIMEOUT{
        "CONNECT_TIMEOUT_SECONDS",
        [](const ConfigLiteral& literal) { return NES::tryGetOr<uint64_t>(literal, expectedType<uint64_t>()).and_then(downcastConfigValue<uint64_t, uint32_t>); },
    10};

    static inline const auto configSchema = createConfigSchema(
        Identifier::parse("TCP_SOURCE"),
        HOST,
        PORT,
        DOMAIN,
        TYPE,
        SEPARATOR,
        FLUSH_INTERVAL_MS,
        SOCKET_BUFFER_SIZE,
        SOCKET_BUFFER_TRANSFER_SIZE
    );
};

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

    explicit TCPSource(const InstantiatedConfig& sourceDescriptor);
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
    bool tryToConnect(const addrinfo* result, int flags);
    bool fillBuffer(TupleBuffer& tupleBuffer, size_t& numReceivedBytes);

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

}
