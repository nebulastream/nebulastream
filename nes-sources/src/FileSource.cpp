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

#include <FileSource.hpp>

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <format>
#include <fstream>
#include <ios>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>


namespace NES::Sources
{

FileSource::FileSource(const SourceDescriptor& sourceDescriptor) : filePath(sourceDescriptor.getFromConfig(ConfigParametersCSV::FILEPATH))
{
}

void FileSource::open(std::shared_ptr<Memory::AbstractBufferProvider>)
{
    const auto realCSVPath = std::unique_ptr<char, decltype(std::free)*>{realpath(this->filePath.c_str(), nullptr), std::free};
    this->inputFile = std::ifstream(realCSVPath.get(), std::ios::binary);
    if (not this->inputFile)
    {
        throw InvalidConfigParameter("Could not determine absolute pathname: {} - {}", this->filePath.c_str(), std::strerror(errno));
    }
}

void FileSource::close()
{
    this->inputFile.close();
}
size_t FileSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, const std::stop_token&)
{
    this->inputFile.read(tupleBuffer.getBuffer<char>(), static_cast<std::streamsize>(tupleBuffer.getBufferSize()));
    const auto numBytesRead = this->inputFile.gcount();
    this->totalNumBytesRead += numBytesRead;
    return numBytesRead;
}

NES::Configurations::DescriptorConfig::Config FileSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::Configurations::DescriptorConfig::validateAndFormat<ConfigParametersCSV>(std::move(config), NAME);
}

std::ostream& FileSource::toString(std::ostream& str) const
{
    str << std::format("\nFileSource(filepath: {}, totalNumBytesRead: {})", this->filePath, this->totalNumBytesRead.load());
    return str;
}

SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterFileSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return FileSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterFileSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<FileSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterFileInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.attachSource.tuples)
    {
        if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(std::string(SYSTEST_FILE_PATH_PARAMETER));
            filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            filePath->second = systestAdaptorArguments.testFilePath;
            if (std::ofstream testFile(systestAdaptorArguments.testFilePath); testFile.is_open())
            {
                /// Write inline tuples to test file.
                for (const auto& tuple : systestAdaptorArguments.attachSource.tuples.value())
                {
                    testFile << tuple << "\n";
                }
                testFile.flush();
                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw TestException("Could not open source file \"{}\"", systestAdaptorArguments.testFilePath);
        }
        throw InvalidConfigParameter("A FileSource config must contain filePath parameter");
    }
    throw TestException("An INLINE SystestAttachSource must not have a 'tuples' vector that is null.");
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterFileFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    /// Check that the test data dir is defined and that the 'filePath' parameter is set
    /// Replace the 'TESTDATA' placeholder in the filepath
    if (const auto attachSourceFilePath = systestAdaptorArguments.attachSource.fileDataPath)
    {
        if (const auto filePath = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(std::string(SYSTEST_FILE_PATH_PARAMETER));
            filePath != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            filePath->second = attachSourceFilePath.value();
            return systestAdaptorArguments.physicalSourceConfig;
        }
        throw InvalidConfigParameter("A FileSource config must contain filePath parameter.");
    }
    throw InvalidConfigParameter("An attach source of type FileData must contain a filePath configuration.");
}


}
