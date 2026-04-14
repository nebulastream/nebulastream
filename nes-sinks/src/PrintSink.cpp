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

#include <Sinks/PrintSink.hpp>

#include <cstdint>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <span>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

PrintSink::PrintSink(
    BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor, std::optional<std::unique_ptr<Encoder>> encoder)
    : Sink(std::move(backpressureController))
    , outputStream(&std::cout)
    , encoder(std::move(encoder))
    , ingestion(sinkDescriptor.getFromConfig(ConfigParametersPrint::INGESTION))
{
}

void PrintSink::start(PipelineExecutionContext&)
{
}

void PrintSink::stop(PipelineExecutionContext&)
{
}

void PrintSink::execute(const TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in PrintSink.");
    std::string bufferAsString;
    /// Create a buffer iterator to help iterate through the tuplebuffer and its children
    BufferIterator iterator{inputBuffer};

    std::optional<BufferIterator::BufferElement> element = iterator.getNextElement();
    while (element.has_value())
    {
        bufferAsString += std::string{element.value().buffer.getAvailableMemoryArea<char>().data(), element.value().contentLength};
        /// Get the next buffer to be written
        element = iterator.getNextElement();
    }

    /// Encode data if encoder was provided
    if (encoder)
    {
        const auto stringSpan = std::as_bytes(std::span(bufferAsString));
        std::vector<char> encodedData{};
        auto encodingResult = encoder.value()->encodeBufferFramed(stringSpan, encodedData);
        PRECONDITION(encodingResult.status == Encoder::EncodeStatusType::SUCCESSFULLY_ENCODED, "Error occured during encoding process.");
        bufferAsString = std::string(encodedData.data(), encodingResult.compressedSize);
    }

    {
        const auto wlocked = outputStream.wlock();

            (*wlocked)->write(
                bufferAsString.data(), static_cast<std::streamsize>(bufferAsString.length()));
        (*wlocked)->flush();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds{ingestion});
}

std::ostream& PrintSink::toString(std::ostream& str) const
{
    str << fmt::format("PRINT_SINK");
    return str;
}

DescriptorConfig::Config PrintSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersPrint>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterPrintSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return PrintSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterPrintSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<PrintSink>(
        std::move(sinkRegistryArguments.backpressureController),
        sinkRegistryArguments.sinkDescriptor,
        std::move(sinkRegistryArguments.encoder));
}

}
