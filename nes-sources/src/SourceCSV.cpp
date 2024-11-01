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
#include <SourceParsers/SourceParserCSV.hpp>
#include <Sources/SourceCSV.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceRegistry.hpp>
#include <SourcesValidation/SourceRegistryValidation.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/std.h>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

SourceCSV::SourceCSV(const SourceDescriptor& sourceDescriptor)
    : fileEnded(false)
    , filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH))
    , delimiter(sourceDescriptor.getFromConfig(ConfigParametersCSV::DELIMITER))
    , skipHeader(sourceDescriptor.getFromConfig(ConfigParametersCSV::SKIP_HEADER))
{
}

void SourceCSV::open()
{
    struct Deleter
    {
        void operator()(const char* ptr) { std::free(const_cast<char*>(ptr)); }
    };
    const auto realCSVPath = realpath(filePath.c_str(), nullptr);
    const auto path = std::unique_ptr<const char, Deleter>(realCSVPath);
    if (realCSVPath == nullptr)
    {
        throw InvalidConfigParameter(fmt::format("Could not determine absolute pathname: {} - {}", filePath.c_str(), std::strerror(errno)));
    }

    input.open(path.get());
    if (!(input.is_open() && input.good()))
    {
        throw CannotOpenSource(fmt::format("Cannot open file: {}", std::string(path.get())));
    }

    NES_DEBUG("SourceCSV: Opening path {}", path.get());
    input.seekg(0, std::ifstream::end);
    if (auto const reportedFileSize = input.tellg(); reportedFileSize == -1)
    {
        throw CannotOpenSource("SourceCSV::SourceCSV File {} is corrupted", filePath);
    }
    else
    {
        this->fileSize = static_cast<decltype(this->fileSize)>(reportedFileSize);
    }
}

void SourceCSV::close()
{
    input.close();
}

bool SourceCSV::fillTupleBuffer(
    NES::Memory::TupleBuffer& tupleBuffer, NES::Memory::AbstractBufferProvider& bufferManager, const ParserCSV& csvParser)
{
    NES_TRACE("SourceCSV::fillBuffer: start at pos={} fileSize={}", currentPositionInFile, fileSize);
    if (this->fileEnded)


    {
        NES_WARNING("SourceCSV::fillBuffer: but file has already ended");
        return false;
    }
    auto maxTuplesInBuffer = tupleBuffer.getBufferSize() / csvParser.getSizeOfSchemaInBytes();
    NES_TRACE("SourceCSV::fillBuffer: fill buffer with #tuples={} of size={}", maxTuplesInBuffer, csvParser.getSizeOfSchemaInBytes());

    std::string line;
    uint64_t tupleCount = 0;

    input.seekg(currentPositionInFile, std::ifstream::beg);
    if (skipHeader && currentPositionInFile == 0)
    {
        NES_TRACE("SourceCSV: Skipping header");
        std::getline(input, line);
        currentPositionInFile = input.tellg();
    }

    while (tupleCount < maxTuplesInBuffer)
    {
        ///Check if EOF has reached
        if (auto const tg = input.tellg(); (tg >= 0 && static_cast<uint64_t>(tg) >= fileSize) || tg == -1)
        {
            input.clear();
            NES_TRACE("SourceCSV::fillBuffer: break because file ended");
            this->fileEnded = true;
            break;
        }

        std::getline(input, line);
        NES_TRACE("SourceCSV line={} val={}", tupleCount, line);
        /// #72: there will be a problem with non-printable characters (at least with null terminators). Check sources
        csvParser.writeInputTupleToTupleBuffer(line, tupleCount, tupleBuffer, bufferManager);
        tupleCount++;
    } ///end of while

    currentPositionInFile = input.tellg();
    tupleBuffer.setNumberOfTuples(tupleCount);
    generatedTuples += tupleCount;
    generatedBuffers++;
    NES_TRACE("SourceCSV::fillBuffer: reading finished read {} tuples at posInFile={}", tupleCount, currentPositionInFile);
    return true;
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SourceCSV::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
}

std::ostream& SourceCSV::toString(std::ostream& str) const
{
    str << "\nSourceCSV(";
    str << "\n  Filesize:" << this->fileSize;
    str << "\n  Generated tuples: " << this->generatedTuples;
    str << "\n  Generated buffers: " << this->generatedBuffers;
    str << ")\n";
    return str;
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SourceGeneratedRegistrarValidation::RegisterSourceValidationCSV(std::unordered_map<std::string, std::string>&& sourceConfig)
{
    return SourceCSV::validateAndFormat(std::move(sourceConfig));
}


std::unique_ptr<Source> SourceGeneratedRegistrar::RegisterSourceCSV(const SourceDescriptor& sourceDescriptor)
{
    return std::make_unique<SourceCSV>(sourceDescriptor);
}

}
