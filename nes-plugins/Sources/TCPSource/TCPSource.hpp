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
    explicit TCPSource(const SourceDescriptor& sourceDescriptor);
    ~TCPSource() override = default;

    TCPSource(const TCPSource&) = delete;
    TCPSource& operator=(const TCPSource&) = delete;
    TCPSource(TCPSource&&) = delete;
    TCPSource& operator=(TCPSource&&) = delete;

    size_t fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token& stopToken) override;

    /// Open TCP connection.
    void open() override;
    /// Close TCP connection.
    void close() override;

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

}
