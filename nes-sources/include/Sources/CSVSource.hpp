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

#include <fstream>
#include <string>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Sources/Source.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

namespace NES::Sources
{

class CSVParser;
using CSVParserPtr = std::shared_ptr<CSVParser>;

class CSVSource : public Source
{
public:
    CSVSource() = default;
    void configure(const Schema& schema, std::unique_ptr<SourceDescriptor>&& sourceDescriptor) override;

    bool fillTupleBuffer(
        NES::Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer,
        const std::shared_ptr<Runtime::AbstractBufferProvider>& bufferManager,
        const Schema& schema) override;

    void open() override { /* noop */ };
    void close() override { /* noop */ };

    std::string toString() const override;

    SourceType getType() const override;

    const CSVSourceTypePtr& getSourceConfig() const;

protected:
    std::ifstream input;
    bool fileEnded;

private:
    CSVSourceTypePtr csvSourceType;
    std::string filePath;
    uint64_t tupleSize;
    uint64_t numberOfTuplesToProducePerBuffer;
    std::string delimiter;
    uint64_t currentPositionInFile{0};
    std::vector<PhysicalTypePtr> physicalTypes;
    size_t fileSize;
    bool skipHeader;
    CSVParserPtr inputParser;

    uint64_t numberOfBuffersToProduce = std::numeric_limits<decltype(numberOfBuffersToProduce)>::max();
    uint64_t generatedTuples{0};
    uint64_t generatedBuffers{0};
};

using CSVSourcePtr = std::shared_ptr<CSVSource>;

}
