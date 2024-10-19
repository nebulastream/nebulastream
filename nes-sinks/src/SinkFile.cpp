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
#include <string>
#include <utility>

#include <Configurations/ConfigurationsNames.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkFile.hpp>
#include <Sinks/SinkRegistry.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksValidation/SinkRegistryValidation.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Sinks
{

SinkFile::SinkFile(const SinkDescriptor& sinkDescriptor)
    : Sink()
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
uint32_t SinkFile::setup(Runtime::Execution::PipelineExecutionContext&)
{
    NES_DEBUG("Setting up file sink: {}", *this);
    auto stream = outputFileStream.wlock();
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
                return 1;
            }
        }
    }

    /// Open the file stream
    if (!stream->is_open())
    {
        stream->open(outputFilePath, std::ofstream::binary | std::ofstream::app);
    }
    isOpen = stream->is_open() && stream->good();
    if (!isOpen)
    {
        NES_ERROR(
            "Could not open output file; filePathOutput={}, is_open()={}, good={}", outputFilePath, stream->is_open(), stream->good());
        return 1;
    }

    /// Write the schema to the file, if it is empty.
    if (stream->tellp() == 0)
    {
        const auto schemaStr = formatter->getFormattedSchema();
        stream->write(schemaStr.c_str(), static_cast<int64_t>(schemaStr.length()));
    }

    return 0;
}
void SinkFile::execute(const Memory::TupleBuffer& inputBuffer, Runtime::Execution::PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in SinkFile.");
    PRECONDITION(isOpen, "Sink was not opened");

    {
        auto fBuffer = formatter->getFormattedBuffer(inputBuffer);
        NES_TRACE("Writing tuples to file sink; filePathOutput={}, fBuffer={}", outputFilePath, fBuffer);
        {
            auto wlocked = outputFileStream.wlock();
            wlocked->write(fBuffer.c_str(), fBuffer.size());
            wlocked->flush();
        }
    }
}
uint32_t SinkFile::stop(Runtime::Execution::PipelineExecutionContext&)
{
    NES_DEBUG("Closing file sink, filePathOutput={}", outputFilePath);
    outputFileStream.wlock()->close();
    return 0;
}

std::ostream& SinkFile::toString(std::ostream& str) const
{
    str << fmt::format("SinkFile(filePathOutput: {}, isAppend: {})", outputFilePath, isAppend);
    return str;
}

std::unique_ptr<Configurations::DescriptorConfig::Config> SinkFile::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersFile>(std::move(config), NAME);
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SinkGeneratedRegistrarValidation::RegisterSinkValidationFile(std::unordered_map<std::string, std::string>&& sinkConfig)
{
    return SinkFile::validateAndFormat(std::move(sinkConfig));
}

std::unique_ptr<Sink> SinkGeneratedRegistrar::RegisterSinkFile(const Sinks::SinkDescriptor& sinkDescriptor)
{
    return std::make_unique<SinkFile>(sinkDescriptor);
}

}
