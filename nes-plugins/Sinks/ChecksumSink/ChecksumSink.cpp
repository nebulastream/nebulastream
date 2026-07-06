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
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksParsing/NoneWithIteratorFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Strings.hpp>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

ChecksumSink::ChecksumSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(ConfigParametersChecksum::FILE_PATH))
{
    const auto legacyOutputFormat = sinkDescriptor.getFromConfig(ConfigParametersChecksum::LEGACY_OUTPUT_FORMAT);
    if (legacyOutputFormat != "None")
    {
        format = std::make_unique<CSVFormat>(*sinkDescriptor.getSchema());
    }
    else
    {
        format = std::make_unique<NoneWithIteratorFormat>(*sinkDescriptor.getSchema());
    }
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

    outputFileStream << "S$Count:UINT64:" << magic_enum::enum_name(DataType::NULLABLE::NOT_NULLABLE)
                     << ",S$Checksum:UINT64:" << magic_enum::enum_name(DataType::NULLABLE::NOT_NULLABLE) << '\n';
    outputFileStream << checksum.numberOfTuples << "," << checksum.checksum << '\n';
    outputFileStream.close();
    isOpen = false;
}

void ChecksumSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in ChecksumSink.");
    /// Direct-buffer format: bytes go straight into a reused per-thread buffer (no per-call std::string).
    thread_local std::vector<char> formatBuffer;
    const auto bytes = format->formatToBuffer(inputBuffer, formatBuffer);
    checksum.add(std::string_view(formatBuffer.data(), bytes));
}

DescriptorConfig::Config ChecksumSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    DescriptorConfig::Config validatedConfig = DescriptorConfig::validateAndFormat<ConfigParametersChecksum>(std::move(config), NAME);
    /// Very hacky way to ensure that legacy output format is set correctly
    if (toUpperCase(std::get<std::string>(validatedConfig.at("legacy_output_format"))) != "NONE")
    {
        validatedConfig["legacy_output_format"] = "CSV";
        validatedConfig["output_format"] = "NATIVE";
    }
    return validatedConfig;
}

SinkValidationRegistryReturnType RegisterChecksumSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return ChecksumSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterChecksumSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<ChecksumSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
