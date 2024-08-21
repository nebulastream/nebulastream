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
#include <Parsers/CSVParser.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <fmt/std.h>
#include <CSVSource.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES
{

CSVSource::CSVSource(SchemaPtr schema, CSVSourceTypePtr csvSourceType)
    : fileEnded(false)
    , csvSourceType(csvSourceType)
    , filePath(csvSourceType->getFilePath()->getValue())
    , delimiter(csvSourceType->getDelimiter()->getValue())
    , skipHeader(csvSourceType->getSkipHeader()->getValue())
    , schema(schema)
{
    this->numberOfBuffersToProduce = csvSourceType->getNumberOfBuffersToProduce()->getValue();
    this->numberOfTuplesToProducePerBuffer = csvSourceType->getNumberOfTuplesToProducePerBuffer()->getValue();
    this->tupleSize = schema->getSchemaSizeInBytes();

    struct Deleter
    {
        void operator()(const char* ptr) { std::free(const_cast<char*>(ptr)); }
    };
    const auto realCSVPath = realpath(filePath.c_str(), nullptr); /// realpath: canonical absolute name of file
    const auto path = std::unique_ptr<const char, Deleter>(const_cast<const char*>(realCSVPath));
    if (path == nullptr)
    {
        NES_THROW_RUNTIME_ERROR("Could not determine absolute pathname: " << filePath.c_str());
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

    DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();
    for (const AttributeFieldPtr& field : schema->fields)
    {
        auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
        physicalTypes.push_back(physicalField);
    }

    this->inputParser = std::make_shared<CSVParser>(schema->getSize(), physicalTypes, delimiter);
}

bool CSVSource::fillTupleBuffer(
    Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer, const std::shared_ptr<Runtime::AbstractBufferProvider>& bufferManager)
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
        generatedTuplesThisPass = tupleBuffer.getCapacity();
    }
    else
    {
        generatedTuplesThisPass = numberOfTuplesToProducePerBuffer;
        NES_ASSERT2_FMT(generatedTuplesThisPass * tupleSize < tupleBuffer.getBuffer().getBufferSize(), "Wrong parameters");
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
    NES_TRACE("CSVSource::receiveData filled buffer with tuples= {}", tupleBuffer.getNumberOfTuples());
    NES_TRACE("CSVSource::fillBuffer: reading finished read {} tuples at posInFile={}", tupleCount, currentPositionInFile);
    return true;
}

std::string CSVSource::toString() const
{
    return fmt::format("CSV_SOURCE(SCHEMA({}), FILE={} numBuff={})", schema->toString(), filePath, this->numberOfTuplesToProducePerBuffer);
}

SourceType CSVSource::getType() const
{
    return SourceType::CSV_SOURCE;
}

const CSVSourceTypePtr& CSVSource::getSourceConfig() const
{
    return csvSourceType;
}
} /// namespace NES
