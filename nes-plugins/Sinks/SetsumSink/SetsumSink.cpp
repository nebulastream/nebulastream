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

#include "SetsumSink.hpp"

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

namespace NES
{

SetsumSink::SetsumSink(const SinkDescriptor& sinkDescriptor)
    : isOpen(false)
    , outputFilePath(sinkDescriptor.getFromConfig(SinkDescriptor::FILE_PATH))
    , formatter(std::make_unique<CSVFormat>(*sinkDescriptor.getSchema(), true))
{
}

void SetsumSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up setsum sink: {}", *this);
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

void SetsumSink::stop(PipelineExecutionContext&)
{
    NES_INFO("Setsum Sink completed. Setsum: {}", fmt::streamed(setsum));

    outputFileStream << "S$Count:UINT32" << '\n' << ",S$Setsum:UINT32" << '\n';
    for (size_t i = 0; i < 8; ++i) {
        outputFileStream << ",S$Col" << i << ":UINT32";
    }
    outputFileStream << '\n';

    bool first = true;
    for (const auto& col : setsum.columns) {
        if (!first) {
            outputFileStream << ",";
        }
        // You MUST use .load() to get the value out of the atomic
        outputFileStream << col.load(std::memory_order_relaxed);
        first = false;
    }

    outputFileStream << '\n';
    isOpen = false;
}

void SetsumSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in SetsumSink.");
    const std::string formatted = formatter->getFormattedBuffer(inputBuffer);
    setsum.add(formatted);
}

DescriptorConfig::Config SetsumSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSetsum>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterSetsumSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return SetsumSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterSetsumSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<SetsumSink>(sinkRegistryArguments.sinkDescriptor);
}

}
