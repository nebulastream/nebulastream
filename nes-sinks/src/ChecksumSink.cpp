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

ChecksumSink::ChecksumSink(const QueryId queryId, const SinkDescriptor& sinkDescriptor)
    : Sink(queryId)
    , isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(ConfigParametersChecksum::FILEPATH))
    , schemaSizeInBytes(sinkDescriptor.schema->getSchemaSizeInBytes())
    , formatter(std::make_unique<CSVFormat>(sinkDescriptor.schema))
{
}
ChecksumSink::~ChecksumSink()
{
    try
    {
        ChecksumSink::close();
    }
    catch (...)
    {
        tryLogCurrentException();
    }
}


std::ostream& ChecksumSink::toString(std::ostream& str) const
{
    str << fmt::format("ChecksumSink(filePathOutput: {})", outputFilePath);
    return str;
}
bool ChecksumSink::equals(const Sink& other) const
{
    if (const auto* otherFileSink = dynamic_cast<const ChecksumSink*>(&other))
    {
        return (this->queryId == other.queryId) && (this->outputFilePath == otherFileSink->outputFilePath)
            && (this->isOpen == otherFileSink->isOpen);
    }
    return false;
}

void ChecksumSink::open()
{
    NES_DEBUG("Setting up checksum sink: {}", *this);
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
    }
}

void ChecksumSink::close()
{
    if (!isOpen)
    {
        return;
    }

    NES_INFO(
        "Checksum Sink completed. Number of Tuples: {}, Checksum: {}, FormattedChecksum: {}", numberOfTuples, checksum, formattedChecksum);
    outputFileStream << numberOfTuples << ", " << formattedChecksum << std::endl;
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
size_t localFormattedChecksum(Memory::TupleBuffer& buffer, CSVFormat& formatter)
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

bool ChecksumSink::emitTupleBuffer(Memory::TupleBuffer& inputBuffer)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in ChecksumSink.");
    checksum += localChecksum(inputBuffer, schemaSizeInBytes);
    formattedChecksum += localFormattedChecksum(inputBuffer, *formatter);
    numberOfTuples += inputBuffer.getNumberOfTuples();
    return true;
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

std::unique_ptr<Sink> SinkGeneratedRegistrar::RegisterChecksumSink(const QueryId queryId, const SinkDescriptor& sinkDescriptor)
{
    return std::make_unique<ChecksumSink>(queryId, sinkDescriptor);
}

}
