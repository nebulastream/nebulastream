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
#include <Sinks/ChecksumSink.hpp>

#include <algorithm>
#include <cstddef>
#include <filesystem>
#include <iostream>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkRegistry.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksValidation/SinkValidationRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sinks
{

ChecksumSink::ChecksumSink(const SinkDescriptor& sinkDescriptor)
    : isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(ConfigParametersChecksum::FILEPATH))
    , schemaSizeInBytes(sinkDescriptor.schema->getSchemaSizeInBytes())
    , formatter(std::make_unique<CSVFormat>(sinkDescriptor.schema))
{
}

void ChecksumSink::start(Runtime::Execution::PipelineExecutionContext&)
{
    NES_DEBUG("Setting up checksum sink: {}", *this);
    if (std::filesystem::exists(outputFilePath.c_str()))
    {
        std::error_code ec;
        if (!std::filesystem::remove(outputFilePath.c_str(), ec))
        {
            throw CannotOpenSink("Could not remove existing output file: filePath={} ", outputFilePath);
            isOpen = false;
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

void ChecksumSink::stop(Runtime::Execution::PipelineExecutionContext&)
{
    NES_INFO(
        "Checksum Sink completed. Number of Tuples: {}, Checksum: {}, FormattedChecksum: {}", numberOfTuples, checksum, formattedChecksum);

    outputFileStream << "S$Count:UINT64,S$Checksum:UINT64" << '\n';
    outputFileStream << numberOfTuples << "," << formattedChecksum << std::endl;
    outputFileStream.close();
    isOpen = false;
}

namespace
{
size_t localChecksum(const Memory::TupleBuffer& buffer, size_t schemaSizeInBytes)
{
    std::span data{buffer.getBuffer<std::byte>(), schemaSizeInBytes * buffer.getNumberOfTuples()};
    size_t checksum = 0;
    for (auto byte : data)
    {
        checksum += static_cast<size_t>(byte);
    }
    return checksum;
}
size_t localFormattedChecksum(const Memory::TupleBuffer& buffer, CSVFormat& formatter)
{
    std::string formatted = formatter.getFormattedBuffer(buffer);
    size_t checksum = 0;
    for (auto character : formatted)
    {
        checksum += static_cast<size_t>(character);
    }
    return checksum;
}
}

void ChecksumSink::execute(const Memory::TupleBuffer& inputBuffer, Runtime::Execution::PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in ChecksumSink.");
    checksum += localChecksum(inputBuffer, schemaSizeInBytes);
    formattedChecksum += localFormattedChecksum(inputBuffer, *formatter);
    numberOfTuples += inputBuffer.getNumberOfTuples();
}

std::unique_ptr<Configurations::DescriptorConfig::Config>
ChecksumSink::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersChecksum>(std::move(config), NAME);
}

std::unique_ptr<Configurations::DescriptorConfig::Config>
SinkValidationGeneratedRegistrar::RegisterSinkValidationChecksum(std::unordered_map<std::string, std::string>&& sinkConfig)
{
    return ChecksumSink::validateAndFormat(std::move(sinkConfig));
}

std::unique_ptr<Sink> SinkGeneratedRegistrar::RegisterChecksumSink(const SinkDescriptor& sinkDescriptor)
{
    return std::make_unique<ChecksumSink>(sinkDescriptor);
}

}
