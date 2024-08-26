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
#include <Sources/Util/MMapCircularBuffer.hpp>
#include <Util/TestTupleBuffer.hpp>

namespace NES::Sources
{

class Parser;
using ParserPtr = std::shared_ptr<Parser>;

class TCPSource : public Source
{
    /// TODO #74: make timeout configurable via descriptor
    constexpr static timeval TCP_SOCKET_DEFAULT_TIMEOUT{0, 100000};

public:
    explicit TCPSource(const Schema& schema, std::unique_ptr<SourceDescriptor>&& sourceDescriptor);

    bool fillTupleBuffer(
        NES::Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer,
        const std::shared_ptr<NES::Runtime::AbstractBufferProvider>& bufferManager,
        const Schema& schema) override;

    bool fillBuffer(
        NES::Runtime::MemoryLayouts::TestTupleBuffer&,
        const std::shared_ptr<NES::Runtime::AbstractBufferProvider>& bufferManager,
        const Schema& schema);

    std::string toString() const override;

    const SourceDescriptor::Config& getSourceConfig() const;

    /// Open TCP connection.
    void open() override;
    /// Close TCP connection.
    void close() override;

private:
    /// Converts buffersize in either binary (NES Format) or ASCII (Json and CSV)
    /// takes 'data', which is a data memory segment which contains the buffersize
    [[nodiscard]] static size_t parseBufferSize(SPAN_TYPE<const char> data);

    std::vector<NES::PhysicalTypePtr> physicalTypes;
    ParserPtr inputParser;
    int connection = -1;
    uint64_t tupleSize;
    uint64_t tuplesThisPass;
    std::unique_ptr<SourceDescriptor> descriptor; ///-Todo: make member of Source?, also 'generatedTuples' and 'generatedBuffers'?
    int sockfd = -1;
    timeval timeout;
    MMapCircularBuffer circularBuffer;

    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
};

using TCPSourcePtr = std::shared_ptr<TCPSource>;

}
