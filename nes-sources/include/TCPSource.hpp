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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_TCPSOURCE_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_TCPSOURCE_HPP_

#include <cstdint>
#include <memory>
#include <Configurations/Worker/PhysicalSourceTypes/TCPSourceType.hpp>
#include <Source.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

#include <MMapCircularBuffer.hpp>

namespace NES
{

class Parser;
using ParserPtr = std::shared_ptr<Parser>;

class TCPSource : public Source
{
    /// TODO #74: make timeout configurable via descriptor
    constexpr static timeval TCP_SOCKET_DEFAULT_TIMEOUT{0, 100000};

public:
    explicit TCPSource(SchemaPtr schema, TCPSourceTypePtr tcpSourceType);

    bool fillTupleBuffer(Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer) override;

    bool fillBuffer(Runtime::MemoryLayouts::TestTupleBuffer&);

    std::string toString() const override;

    SourceType getType() const override;

    const TCPSourceTypePtr& getSourceConfig() const;

    /// Open TCP connection.
    void open() override;
    /// Close TCP connection.
    void close() override;

private:
    /// Oonverts buffersize in either binary (NES Format) or ASCII (Json and CSV)
    /// takes 'data', which is a data memory segment which contains the buffersize
    [[nodiscard]] size_t parseBufferSize(SPAN_TYPE<const char> data) const;

    std::vector<PhysicalTypePtr> physicalTypes;
    ParserPtr inputParser;
    int connection = -1;
    uint64_t tupleSize;
    uint64_t tuplesThisPass;
    TCPSourceTypePtr sourceConfig;
    int sockfd = -1;
    timeval timeout;
    MMapCircularBuffer circularBuffer;

    SchemaPtr schema;
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
};
using TCPSourcePtr = std::shared_ptr<TCPSource>;
} /// namespace NES
#endif /// NES_RUNTIME_INCLUDE_SOURCES_TCPSOURCE_HPP_
