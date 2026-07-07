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
#include <ostream>
#include <stop_token>
#include <string>
#include <string_view>
#include <netdb.h>
#include <sys/types.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigValue.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sources/Source.hpp>

namespace NES
{

/// Source-defined config struct: instantiated from the generic config by the SourceConfig
/// registry entry, carried through the SourceDescriptor as std::any, and serialized via
/// reflection of exactly this struct (all members are reflectable).
struct TCPSourceConfig
{
    std::string socketHost;
    uint32_t socketPort;
    int32_t socketDomain;
    int32_t socketType;
    char tupleDelimiter;
    uint32_t socketBufferSize;
    uint32_t bytesUsedForSocketBufferSizeTransfer;
    float flushIntervalInMs;
    uint32_t connectTimeoutSeconds;

    static TCPSourceConfig fromConfig(const InstantiatedConfig& config);
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
    uint32_t connectionTimeout;
};

}
