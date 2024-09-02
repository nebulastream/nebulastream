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
#include <chrono>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Operators/LogicalOperators/Sources/CsvSourceDescriptor.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/std.h>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Sources
{

/// Todo #72: remove schema from CSVSource (only required by parser).
CSVSource::CSVSource(const Schema& schema, CSVSourceTypePtr&& csvSourceType) : fileEnded(false), tupleSize(schema.getSchemaSizeInBytes())
{
    this->filePath = csvSourceType->getFilePath()->getValue();
    this->numberOfTuplesToProducePerBuffer = csvSourceType->getNumberOfTuplesToProducePerBuffer()->getValue();
    this->delimiter = csvSourceType->getDelimiter()->getValue();
    this->skipHeader = csvSourceType->getSkipHeader()->getValue();
    this->numberOfBuffersToProduce = csvSourceType->getNumberOfBuffersToProduce()->getValue();

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : schema.fields)
    {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    this->inputParser = std::make_shared<CSVParser>(schema.getSize(), physicalTypes, delimiter);
}

void CSVSource::open()
{
    struct Deleter
    {
        void operator()(const char* ptr) { std::free(const_cast<char*>(ptr)); }
    };
    const auto realCSVPath = realpath(filePath.c_str(), nullptr);
    const auto path = std::unique_ptr<const char, Deleter>(realCSVPath);
    if (realCSVPath == nullptr)
    {
        NES_THROW_RUNTIME_ERROR("Could not determine absolute pathname: " << filePath.c_str() << " - " << std::strerror(errno));
    }

    input.open(path.get());
    if (!(input.is_open() && input.good()))
    {
        throw Exceptions::RuntimeException("Cannot open file: " + std::string(path.get()));
    }

    NES_DEBUG("CSVSource: Opening path {}", path.get());
    input.seekg(0, std::ifstream::end);
    if (auto const reportedFileSize = input.tellg(); reportedFileSize == -1)
    {
        throw Exceptions::RuntimeException("CSVSource::CSVSource File " + filePath + " is corrupted");
    }
    else
    {
        this->fileSize = static_cast<decltype(this->fileSize)>(reportedFileSize);
    }

    NES_DEBUG("CSVSource: tupleSize={} numBuff={}", this->tupleSize, this->numberOfTuplesToProducePerBuffer);
}

void CSVSource::close()
{
    input.close();
}

bool CSVSource::fillTupleBuffer(
    NES::Memory::TupleBuffer& tupleBuffer,
    const std::shared_ptr<NES::Memory::AbstractBufferProvider>& bufferManager,
    std::shared_ptr<Schema> schema)
{
    NES_TRACE("CSVSource::fillBuffer: start at pos={} fileSize={}", currentPositionInFile, fileSize);
    if (this->fileEnded)
    {
        NES_WARNING("CSVSource::fillBuffer: but file has already ended");
        return false;
    }

    input.seekg(currentPositionInFile, std::ifstream::beg);

    uint64_t generatedTuplesThisPass = 0;
    ///fill buffer maximally
    if (numberOfTuplesToProducePerBuffer == 0)
    {
        generatedTuplesThisPass = tupleBuffer.getBufferSize() / tupleSize;
    }
    else
    {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
        NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < tupleBuffer.getBufferSize(), "Wrong parameters");
    }
    NES_TRACE("CSVSource::fillBuffer: fill buffer with #tuples={} of size={}", generatedTuplesThisPass, tupleSize);

    std::string line;
    uint64_t tupleCount = 0;

    if (skipHeader && currentPositionInFile == 0)
    {
        NES_TRACE("CSVSource: Skipping header");
        std::getline(input, line);
        currentPositionInFile = input.tellg();
    }

    while (tupleCount < generatedTuplesThisPass)
    {
        ///Check if EOF has reached
        if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1)
        {
            input.clear();
            NES_TRACE("CSVSource::fillBuffer: break because file ended");
            this->fileEnded = true;
            break;
        }

        std::getline(input, line);
        NES_TRACE("CSVSource line={} val={}", tupleCount, line);
        /// TODO #74: there will be a problem with non-printable characters (at least with null terminators). Check sources
        inputParser->writeInputTupleToTupleBuffer(line, tupleCount, tupleBuffer, schema, bufferManager);
        tupleCount++;
    } ///end of while

    currentPositionInFile = input.tellg();
    tupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    NES_TRACE("CSVSource::fillBuffer: reading finished read {} tuples at posInFile={}", tupleCount, currentPositionInFile);
    return true;
}

std::string CSVSource::toString() const
{
    return fmt::format("FILE={} numBuff={})", filePath, this->numberOfTuplesToProducePerBuffer);
}

SourceType CSVSource::getType() const
{
    return SourceType::CSV_SOURCE;
}

}
