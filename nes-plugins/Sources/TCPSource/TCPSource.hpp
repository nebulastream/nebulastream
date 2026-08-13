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
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <netdb.h>
#include <sys/types.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>

namespace NES
{

struct TCPSourceConfig
{
    struct SocketDestination
    {
        std::string socketHost;
        uint32_t socketPort;
    };

    /// To be left empty if overwriteable host and port is set, for example in the systests
    std::optional<SocketDestination> socketDestination;
    int32_t socketDomain{};
    int32_t socketType{};
    float flushIntervalInMs{};
    uint32_t connectTimeoutSeconds{};

    static std::expected<TCPSourceConfig, Exception> fromConfig(const InstantiatedConfig& config);
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
    constexpr static std::string_view NAME = "TCP";

    explicit TCPSource(const TCPSourceConfig& config);
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

    static Schema<QualifiedErasedConfigField, Ordered> getConfigSchema();

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

    std::string socketHost;
    std::string socketPort;
    int socketType;
    int socketDomain;
    float flushIntervalInMs;
    uint64_t generatedBuffers{0};
    uint32_t connectionTimeout;
};

}
