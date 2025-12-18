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

#include <DiscardSink.hpp>

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

DiscardSink::DiscardSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor) : Sink(std::move(backpressureController))
{
    const auto outputFilePath = sinkDescriptor.getFromConfig(SinkDescriptor::FILE_PATH);
    if (std::filesystem::exists(outputFilePath.c_str()))
    {
        std::error_code ec;
        if (!std::filesystem::remove(outputFilePath.c_str(), ec))
        {
            throw CannotOpenSink("Could not remove existing output file: filePath={} ", outputFilePath);
        }
    }

    /// Open the file stream
    std::ofstream outputFileStream;
    if (!outputFileStream.is_open())
    {
        outputFileStream.open(outputFilePath, std::ofstream::binary | std::ofstream::app);
    }
    const auto isOpen = outputFileStream.is_open() && outputFileStream.good();
    if (!isOpen)
    {
        throw CannotOpenSink(
            "Could not open output file; filePathOutput={}, is_open()={}, good={}",
            outputFilePath,
            outputFileStream.is_open(),
            outputFileStream.good());
    }

    switch (const auto inputFormat = sinkDescriptor.getFromConfig(ConfigParametersDiscard::INPUT_FORMAT))
    {
        case InputFormat::CSV: {
            const auto formatter = std::make_unique<CSVFormat>(*sinkDescriptor.getSchema());
            const auto schemaStr = formatter->getFormattedSchema();
            outputFileStream << schemaStr;
            outputFileStream.close();
            break;
        }
        default:
            throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
    }
}

void DiscardSink::start(PipelineExecutionContext&)
{
}

void DiscardSink::stop(PipelineExecutionContext&)
{
}

void DiscardSink::execute(const TupleBuffer&, PipelineExecutionContext&)
{
    /// This sink discards the tuple buffer, therefore we do nothing here
}

std::ostream& DiscardSink::toString(std::ostream& str) const
{
    str << fmt::format("DiscardSink()");
    return str;
}

DescriptorConfig::Config DiscardSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::DescriptorConfig::validateAndFormat<ConfigParametersDiscard>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterDiscardSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return DiscardSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterDiscardSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<DiscardSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
