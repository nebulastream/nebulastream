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
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Sources/Util/MMapCircularBuffer.hpp>

namespace NES
{
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class PhysicalType;
using PhysicalTypePtr = std::shared_ptr<PhysicalType>;
}

namespace NES::Sources
{

class Parser;
using ParserPtr = std::shared_ptr<Parser>;

class TCPSource : public Source
{
    /// TODO #74: make timeout configurable via descriptor
    constexpr static std::chrono::microseconds TCP_SOCKET_DEFAULT_TIMEOUT{100000};

public:
    static inline const std::string PLUGIN_NAME = "TCP";
    TCPSource(const Schema& schema, std::unique_ptr<SourceDescriptor>&& sourceDescriptor);

    bool fillTupleBuffer(
        NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, std::shared_ptr<Schema> schema) override;

    std::string toString() const override;

    SourceType getType() const override;

    /// Open TCP connection.
    void open() override;
    /// Close TCP connection.
    void close() override;

private:
    bool
    fillBuffer(NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, std::shared_ptr<Schema> schema);

    /// Converts buffersize in either binary (NES Format) or ASCII (Json and CSV)
    /// takes 'data', which is a data memory segment which contains the buffersize
    [[nodiscard]] size_t parseBufferSize(std::span<const char> data) const;

    std::vector<NES::PhysicalTypePtr> physicalTypes;
    ParserPtr inputParser;
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
