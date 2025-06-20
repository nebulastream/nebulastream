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

#include <ChecksumSink.hpp>

#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{

ChecksumSink::ChecksumSink(const SinkDescriptor& sinkDescriptor)
    : isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(SinkDescriptor::FILE_PATH))
    , formatter(std::make_unique<CSVFormat>(*sinkDescriptor.getSchema()))
{
}

void ChecksumSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up checksum sink: {}", *this);
    if (std::filesystem::exists(outputFilePath.c_str()))
    {
        std::error_code ec;
        if (!std::filesystem::remove(outputFilePath.c_str(), ec))
        {
            throw CannotOpenSink("Could not remove existing output file: filePath={} ", outputFilePath);
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
        throw CannotOpenSink(
            "Could not open output file; filePathOutput={}, is_open()={}, good={}",
            outputFilePath,
            outputFileStream.is_open(),
            outputFileStream.good());
    }
}

void ChecksumSink::stop(PipelineExecutionContext&)
{
    NES_INFO("Checksum Sink completed. Checksum: {}", fmt::streamed(checksum));

    outputFileStream << "S$Count:UINT64,S$Checksum:UINT64" << '\n';
    outputFileStream << checksum.numberOfTuples << "," << checksum.checksum << '\n';
    outputFileStream.close();
    isOpen = false;
}

void ChecksumSink::execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in ChecksumSink.");
    const std::string formatted = formatter->getFormattedBuffer(inputBuffer);
    checksum.add(formatted);
}

Configurations::DescriptorConfig::Config ChecksumSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersChecksum>(std::move(config), NAME);
}

SinkValidationRegistryReturnType
SinkValidationGeneratedRegistrar::RegisterChecksumSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return ChecksumSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterChecksumSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<ChecksumSink>(sinkRegistryArguments.sinkDescriptor);
}

}
