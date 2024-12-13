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

#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/FileSink.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkRegistry.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksValidation/SinkValidationRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sinks
{

FileSink::FileSink(const QueryId queryId, const SinkDescriptor& sinkDescriptor)
    : Sink(queryId)
    , outputFilePath(sinkDescriptor.getFromConfig(ConfigParametersFile::FILEPATH))
    , isAppend(sinkDescriptor.getFromConfig(ConfigParametersFile::APPEND))
    , isOpen(false)
{
    switch (const auto inputFormat = sinkDescriptor.getFromConfig(ConfigParametersFile::INPUT_FORMAT))
    {
        case Configurations::InputFormat::CSV:
            formatter = std::make_unique<CSVFormat>(sinkDescriptor.schema);
            break;
        default:
            throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
    }
}

std::ostream& FileSink::toString(std::ostream& str) const
{
    str << fmt::format("FileSink(filePathOutput: {}, isAppend: {})", outputFilePath, isAppend);
    return str;
}
bool FileSink::equals(const Sink& other) const
{
    if (const auto* otherFileSink = dynamic_cast<const FileSink*>(&other))
    {
        return (this->queryId == other.queryId) && (this->outputFilePath == otherFileSink->outputFilePath)
            && (this->isAppend == otherFileSink->isAppend) && (this->isOpen == otherFileSink->isOpen);
    }
    return false;
}

void FileSink::open()
{
    NES_DEBUG("Setting up file sink: {}", *this);
    /// Remove an existing file unless the isAppend mode is isAppend.
    if (!isAppend)
    {
        if (std::filesystem::exists(outputFilePath.c_str()))
        {
            std::error_code ec;
            if (!std::filesystem::remove(outputFilePath.c_str(), ec))
            {
                NES_ERROR("Could not remove existing output file: filePath={} ", outputFilePath);
                isOpen = false;
                return;
            }
        }
    }

    /// Open the file stream
    if (!outputFileStream.is_open())
    {
        outputFileStream.open(outputFilePath, std::ofstream::binary | std::ofstream::app);
    }
    isOpen = outputFileStream.is_open() && outputFileStream.good();
    if (!isOpen)
    {
        NES_ERROR(
            "Could not open output file; filePathOutput={}, is_open()={}, good={}",
            outputFilePath,
            outputFileStream.is_open(),
            outputFileStream.good());
        return;
    }

    /// Write the schema to the file, if it is empty.
    if (outputFileStream.tellp() == 0)
    {
        const auto schemaStr = formatter->getFormattedSchema();
        outputFileStream.write(schemaStr.c_str(), static_cast<int64_t>(schemaStr.length()));
    }
}

void FileSink::close()
{
    NES_DEBUG("Closing file sink, filePathOutput={}", outputFilePath);
    outputFileStream.close();
}

bool FileSink::emitTupleBuffer(Memory::TupleBuffer& inputBuffer)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in FileSink.");
    if (!isOpen)
    {
        NES_ERROR("The output file could not be opened during setup of the file sink.");
        return false;
    }

    std::unique_lock lock(writeMutex);
    {
        auto fBuffer = formatter->getFormattedBuffer(inputBuffer);
        NES_TRACE("Writing tuples to file sink; filePathOutput={}, fBuffer={}", outputFilePath, fBuffer);
        outputFileStream.write(fBuffer.c_str(), fBuffer.size());
        outputFileStream.flush();
    }
    return true;
}

std::unique_ptr<Configurations::DescriptorConfig::Config> FileSink::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersFile>(std::move(config), NAME);
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SinkValidationGeneratedRegistrar::RegisterSinkValidationFile(std::unordered_map<std::string, std::string>&& sinkConfig)
{
    return FileSink::validateAndFormat(std::move(sinkConfig));
}

std::unique_ptr<Sink> SinkGeneratedRegistrar::RegisterFileSink(const QueryId queryId, const Sinks::SinkDescriptor& sinkDescriptor)
{
    return std::make_unique<FileSink>(queryId, sinkDescriptor);
}

}
