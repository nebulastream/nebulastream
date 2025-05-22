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

#include <cstdint>
#include <filesystem>
#include <fstream>
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
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{

FileSink::FileSink(const SinkDescriptor& sinkDescriptor)
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

std::ostream& FileSink::toString(std::ostream& str) const
{
    str << fmt::format("FileSink(filePathOutput: {}, isAppend: {})", outputFilePath, isAppend);
    return str;
}

void FileSink::start(Runtime::Execution::PipelineExecutionContext&)
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
                isOpen = false;
                throw CannotOpenSink("Could not remove existing output file: filePath={} ", outputFilePath);
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
        throw CannotOpenSink(
            "Could not open output file; filePathOutput={}, is_open()={}, good={}", outputFilePath, stream->is_open(), stream->good());
    }

    /// Write the schema to the file, if it is empty.
    if (stream->tellp() == 0)
    {
        const auto schemaStr = formatter->getFormattedSchema();
        stream->write(schemaStr.c_str(), static_cast<int64_t>(schemaStr.length()));
    }
}

void FileSink::execute(const Memory::TupleBuffer& inputTupleBuffer, Runtime::Execution::PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in FileSink.");
    PRECONDITION(isOpen, "Sink was not opened");

    {
        auto fBuffer = formatter->getFormattedBuffer(inputTupleBuffer);
        NES_TRACE("Writing tuples to file sink; filePathOutput={}, fBuffer={}", outputFilePath, fBuffer);
        {
            auto wlocked = outputFileStream.wlock();
            wlocked->write(fBuffer.c_str(), static_cast<long>(fBuffer.size()));
            wlocked->flush();
        }
    }
}

void FileSink::stop(Runtime::Execution::PipelineExecutionContext&)
{
    NES_DEBUG("Closing file sink, filePathOutput={}", outputFilePath);
    auto stream = outputFileStream.wlock();
    stream->flush();
    stream->close();
}

Configurations::DescriptorConfig::Config FileSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersFile>(std::move(config), NAME);
}

SinkValidationRegistryReturnType SinkValidationGeneratedRegistrar::RegisterFileSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return FileSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterFileSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<FileSink>(sinkRegistryArguments.sinkDescriptor);
}

}
