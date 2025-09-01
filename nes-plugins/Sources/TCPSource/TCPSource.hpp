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
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <variant>
#include <netdb.h>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <sys/socket.h> /// For socket functions
#include <sys/types.h>

namespace NES
{

struct AddressInfo
{
    static std::expected<AddressInfo, std::string> from(const std::string& host, uint16_t port);

    auto operator*() const { return *result; }

    auto operator->() const { return result.operator->(); }

    std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> result;
};

struct OwnedDescriptor
{
    int descriptor = -1;
    explicit OwnedDescriptor(int descriptor);
    ~OwnedDescriptor();

    explicit operator int() const { return descriptor; }

    OwnedDescriptor(OwnedDescriptor&& other) noexcept;
    OwnedDescriptor& operator=(OwnedDescriptor&& other) noexcept;
    OwnedDescriptor(const OwnedDescriptor& other) = delete;
    OwnedDescriptor& operator=(const OwnedDescriptor& other) = delete;
};

struct Socket
{
    static std::expected<Socket, std::string> create(const AddressInfo& address);

    std::expected<void, std::string> addFlag(unsigned flag);

    std::expected<void, std::string> removeFlag(unsigned flag);

    auto setNonBlock();
    auto setBlock();

    std::expected<void, std::string> setTimeouts(std::chrono::microseconds timeoutDuration);

    [[nodiscard]] std::expected<void, std::string> checkError() const;

    OwnedDescriptor descriptor;
    unsigned flags;
    std::chrono::microseconds timeout;
};

struct Connection
{
    static std::expected<Connection, std::string> from(Socket sock, AddressInfo addrinfo, std::chrono::microseconds connectionTimeout);

    std::expected<void, std::string> reconnect();

    struct EoS
    {
    };

    std::expected<std::span<uint8_t>, std::variant<EoS, std::string>> receive(std::span<uint8_t> result, std::chrono::microseconds timeout);

    Socket socket;
    AddressInfo addrinfo;
    std::chrono::microseconds connectionTimeout;
};

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
    static inline const DescriptorConfig::ConfigParameter<int32_t> DOMAIN{
        "socket_domain",
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
    static inline const DescriptorConfig::ConfigParameter<int32_t> TYPE{
        "socket_type",
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
    static inline const DescriptorConfig::ConfigParameter<float> FLUSH_INTERVAL_MS{
        "flush_interval_ms",
        0,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(FLUSH_INTERVAL_MS, config); }};
    static inline const DescriptorConfig::ConfigParameter<uint32_t> CONNECT_TIMEOUT{
        "connect_timeout_seconds",
        10,
        [](const std::unordered_map<std::string, std::string>& config) { return DescriptorConfig::tryGet(CONNECT_TIMEOUT, config); }};

    static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
        = DescriptorConfig::createConfigParameterContainerMap(
            SourceDescriptor::parameterMap, HOST, PORT, DOMAIN, TYPE, FLUSH_INTERVAL_MS, CONNECT_TIMEOUT);
};

class TCPSource final : public Source
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

    FillTupleBufferResult fillTupleBuffer(TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open TCP connection.
    void open(std::shared_ptr<AbstractBufferProvider> bufferProvider) override;
    /// Close TCP connection.
    void close() override;

    static DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

private:
    bool fillBuffer(TupleBuffer& tupleBuffer, size_t& numReceivedBytes);

    std::optional<Connection> connection;

    std::string socketHost;
    uint16_t socketPort;
    int socketType;
    int socketDomain;
    float flushIntervalInMs;
    uint64_t generatedBuffers{0};
    u_int32_t connectionTimeout;
};

}
