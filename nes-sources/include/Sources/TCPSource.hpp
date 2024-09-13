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
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SourcesUtils/MMapCircularBuffer.hpp>

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
    struct ConfigParametersTCP
    {
        static inline const SourceDescriptor::ConfigKey<std::string> HOST{"socketHost"};
        static inline const SourceDescriptor::ConfigKey<uint32_t> PORT{"socketPort"};
        static inline const SourceDescriptor::ConfigKey<int32_t> DOMAIN{"socketDomain"};
        static inline const SourceDescriptor::ConfigKey<int32_t> TYPE{"socketType"};
        static inline const SourceDescriptor::ConfigKey<char> SEPARATOR{"tupleSeparator", '\n'};
        static inline const SourceDescriptor::ConfigKey<float> FLUSH_INTERVAL_MS{"flushIntervalMS"};
        static inline const SourceDescriptor::ConfigKey<Configurations::TCPDecideMessageSize> DECIDED_MESSAGE_SIZE{
            "decideMessageSize", Configurations::TCPDecideMessageSize::TUPLE_SEPARATOR};
        static inline const SourceDescriptor::ConfigKey<uint32_t> SOCKET_BUFFER_SIZE{"socketBufferSize"};
        static inline const SourceDescriptor::ConfigKey<uint32_t> SOCKET_BUFFER_TRANSFER_SIZE{"bytesUsedForSocketBufferSizeTransfer", 0};
        static inline const SourceDescriptor::ConfigKey<Configurations::InputFormat> INPUT_FORMAT{"inputFormat"};
    };

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
