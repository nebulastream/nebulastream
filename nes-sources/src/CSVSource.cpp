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
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Sources/CSVSource.hpp>
#include <Sources/GeneratedSourceRegistrar.hpp>
#include <Sources/Parsers/CSVParser.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceRegistry.hpp>
#include <SourcesValidation/GeneratedRegistrarSourceValidation.hpp>
#include <SourcesValidation/RegistrySourceValidation.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestTupleBuffer.hpp>
#include <fmt/std.h>
#include <ErrorHandling.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

namespace NES::Sources
{

CSVSource::CSVSource(const Schema& schema, const SourceDescriptor& sourceDescriptor)
    : fileEnded(false)
    , filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH))
    , tupleSize(schema.getSchemaSizeInBytes())
    , delimiter(sourceDescriptor.getFromConfig(ConfigParametersCSV::DELIMITER))
    , skipHeader(sourceDescriptor.getFromConfig(ConfigParametersCSV::SKIP_HEADER))
{
    /// Determine the physical types and create the inputParser (Todo: remove in #72).
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

    NES_DEBUG("CSVSource: tupleSize={}", this->tupleSize);
}

void CSVSource::close()
{
    input.close();
}

bool CSVSource::fillTupleBuffer(
    NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, std::shared_ptr<Schema> schema)
{
    NES_TRACE("CSVSource::fillBuffer: start at pos={} fileSize={}", currentPositionInFile, fileSize);
    if (this->fileEnded)
    {
        NES_WARNING("CSVSource::fillBuffer: but file has already ended");
        return false;
    }

    /// Todo #72: remove TestTupleBuffer creation.
    /// We need to create a TestTupleBuffer here, because if we do it after calling 'writeInputTupleToTupleBuffer' we repeatedly create a
    /// TestTupleBuffer for the same TupleBuffer.
    auto testTupleBuffer = NES::Memory::MemoryLayouts::TestTupleBuffer::createTestTupleBuffer(tupleBuffer, schema);
    uint64_t generatedTuplesThisPass = testTupleBuffer.getCapacity();
    NES_TRACE("CSVSource::fillBuffer: fill buffer with #tuples={} of size={}", generatedTuplesThisPass, tupleSize);

    std::string line;
    uint64_t tupleCount = 0;

    input.seekg(currentPositionInFile, std::ifstream::beg);
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
        inputParser->writeInputTupleToTupleBuffer(line, tupleCount, testTupleBuffer, *schema, bufferManager);
        tupleCount++;
    } ///end of while

    currentPositionInFile = input.tellg();
    testTupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    NES_TRACE("CSVSource::fillBuffer: reading finished read {} tuples at posInFile={}", tupleCount, currentPositionInFile);
    return true;
}

SourceDescriptor::Config CSVSource::validateAndFormat(std::map<std::string, std::string>&& config)
{
    SourceDescriptor::Config validatedConfig;

    SourceDescriptor::validateAndFormatParameter(ConfigParametersCSV::FILEPATH, config, validatedConfig);
    SourceDescriptor::validateAndFormatParameter(ConfigParametersCSV::DELIMITER, config, validatedConfig);
    SourceDescriptor::validateAndFormatParameter(ConfigParametersCSV::SKIP_HEADER, config, validatedConfig);

    return validatedConfig;
}

std::ostream& CSVSource::toString(std::ostream& str) const
{
    str << "TCPSource(";
    str << "Filesize:" << this->fileSize;
    str << "Tuplesize:" << this->tupleSize;
    str << "Generated tuples: " << this->generatedTuples;
    str << "Generated buffers: " << this->generatedBuffers;
    str << ")\n";
    return str;
}

void GeneratedRegistrarSourceValidation::RegisterSourceValidationCSV(RegistrySourceValidation& registry)
{
    const auto validateFunc = [](std::map<std::string, std::string>&& sourceConfig) -> SourceDescriptor::Config
    { return CSVSource::validateAndFormat(std::move(sourceConfig)); };
    registry.registerPlugin((CSVSource::NAME), validateFunc);
}

void GeneratedSourceRegistrar::RegisterCSVSource(SourceRegistry& registry)
{
    const auto constructorFunc = [](const Schema& schema, const SourceDescriptor& sourceDescriptor) -> std::unique_ptr<Source>
    { return std::make_unique<CSVSource>(schema, sourceDescriptor); };
    registry.registerPlugin((CSVSource::NAME), constructorFunc);
}

}
