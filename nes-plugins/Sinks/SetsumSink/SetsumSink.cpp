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
#include <sstream>

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

constexpr size_t NUM_COLUMNS = 8;

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

    // CSV Header: Count, followed by 8 columns
    outputFileStream << "S$Count:UINT64";
    for (size_t i = 0; i < NUM_COLUMNS; ++i) {
        outputFileStream << ",S$Col" << i << ":UINT32";
    }
    outputFileStream << '\n';

    // CSV Values
    outputFileStream << tupleCount.load(std::memory_order::relaxed);
    
    for (const auto& col : setsum.columns) {
        outputFileStream << "," << col.load(std::memory_order::relaxed);
    }

    outputFileStream << '\n';
    isOpen = false;
}

void SetsumSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in SetsumSink.");
    
    // Get CSV formatted buffer (potentially multiple lines)
    const std::string formatted = formatter->getFormattedBuffer(inputBuffer);
    
    // Split by newline to process each tuple individually
    std::stringstream ss(formatted);
    std::string line;
    uint64_t count = 0;
    
    while (std::getline(ss, line)) {
        if (!line.empty()) {
            setsum.add(line);
            count++;
        }
    }
    
    // Atomically update total tuple count
    tupleCount.fetch_add(count, std::memory_order::relaxed);
}

DescriptorConfig::Config SetsumSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersSetsum>(std::move(config), NAME);
}

SinkValidationRegistryReturnType SetsumSink::RegisterSetsumSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return SetsumSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SetsumSink::RegisterSetsumSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<SetsumSink>(sinkRegistryArguments.sinkDescriptor);
}


// TupleBuffer AddToBuffer (std::string_view string)
// {
//     TupleBuffer buffer;
//
//     return buffer;
// }
}
