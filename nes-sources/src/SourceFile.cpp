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
#include <Sources/SourceDescriptor.hpp>
#include <Sources/SourceFile.hpp>
#include <Sources/SourceRegistry.hpp>
#include <SourcesValidation/SourceRegistryValidation.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/std.h>
#include <ErrorHandling.hpp>

namespace NES::Sources
{

SourceFile::SourceFile(const SourceDescriptor& sourceDescriptor) : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH))
{
}

void SourceFile::open()
{
    const auto realCSVPath = realpath(filePath.c_str(), nullptr);
    inputFile = std::ifstream(realCSVPath, std::ios::binary);
    if (!inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", filePath.c_str(), std::strerror(errno));
    }
}

void SourceFile::close()
{
    inputFile.close();
}

size_t SourceFile::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer)
{
    inputFile.read(reinterpret_cast<char*>(tupleBuffer.getBuffer()), tupleBuffer.getBufferSize());
    return inputFile.gcount();
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SourceFile::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
}

std::ostream& SourceFile::toString(std::ostream& str) const
{
    str << std::format("\nSourceFile(filepath: {})", this->filePath);
    return str;
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SourceGeneratedRegistrarValidation::RegisterSourceValidationFile(std::unordered_map<std::string, std::string>&& sourceConfig)
{
    return SourceFile::validateAndFormat(std::move(sourceConfig));
}


std::unique_ptr<Source> SourceGeneratedRegistrar::RegisterSourceFile(const SourceDescriptor& sourceDescriptor)
{
    return std::make_unique<SourceFile>(sourceDescriptor);
}

}
