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


#include <MemorySource.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

namespace NES::Sources
{
MemorySource::MemorySource(const SourceDescriptor& sourceDescriptor) : filePath(sourceDescriptor.getFromConfig(ConfigParametersMemory::FILEPATH)), totalNumBytesRead(0)
{
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    auto inputFile = std::ifstream(realCSVPath.get(), std::ios::binary);
    if (not inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), std::strerror(errno));
    }

    /// We read the full data into _buffer
    fileSize = std::filesystem::file_size(filePath);
    _buffer.resize(fileSize);
    _buffer.reserve(fileSize);
    inputFile.read(reinterpret_cast<char*>(_buffer.data()), fileSize);
    inputFile.close();
}

size_t MemorySource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
{
    /// Calculate the number of bytes to read. Per default we read the bufferSize. If there are not enough bytes in the _buffer, we read the remaining
    auto bytesToRead = tupleBuffer.getBufferSize();
    if (totalNumBytesRead + bytesToRead >= fileSize)
    {
        bytesToRead = fileSize - totalNumBytesRead;
    }

    std::memcpy(tupleBuffer.getBuffer(), _buffer.data() + totalNumBytesRead, bytesToRead);
    totalNumBytesRead += bytesToRead;
    return bytesToRead;
}

void MemorySource::open()
{
}

void MemorySource::close()
{
}

NES::Configurations::DescriptorConfig::Config MemorySource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersMemory>(std::move(config), NAME);
}

std::ostream& MemorySource::toString(std::ostream& str) const
{
    str << std::format("\nMemorySource(filepath: {}, totalNumBytesRead: {})", this->filePath, this->totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterMemorySourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return MemorySource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterMemorySource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<MemorySource>(sourceRegistryArguments.sourceDescriptor);
}


}