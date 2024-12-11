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

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <format>
#include <ios>
#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/Source.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <ErrorHandling.hpp>
#include <FileSource.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources
{

FileSource::FileSource(const SourceDescriptor& sourceDescriptor) : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH))
{
}

void FileSource::open()
{
    auto* const realCSVPath = realpath(this->filePath.c_str(), nullptr);
    this->inputFile = std::ifstream(realCSVPath, std::ios::binary);
    if (not this->inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), std::strerror(errno));
    }
}

void FileSource::close()
{
    this->inputFile.close();
}

size_t FileSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer)
{
    this->inputFile.read(tupleBuffer.getBuffer<char>(), static_cast<std::streamsize>(tupleBuffer.getBufferSize()));
    const auto numBytesRead = this->inputFile.gcount();
    this->totalNumBytesRead += numBytesRead;
    return numBytesRead;
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
FileSource::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
}

std::ostream& FileSource::toString(std::ostream& str) const
{
    str << std::format("\nFileSource(filepath: {}, totalNumBytesRead: {})", this->filePath, this->totalNumBytesRead);
    return str;
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SourceValidationGeneratedRegistrar::RegisterFileSourceValidation(std::unordered_map<std::string, std::string>&& sourceConfig)
{
    return FileSource::validateAndFormat(std::move(sourceConfig));
}


std::unique_ptr<Source> SourceGeneratedRegistrar::RegisterFileSource(const SourceDescriptor& sourceDescriptor)
{
    return std::make_unique<FileSource>(sourceDescriptor);
}

}
