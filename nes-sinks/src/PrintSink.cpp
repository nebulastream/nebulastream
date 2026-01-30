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
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

PrintSink::PrintSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController))
    , outputStream(&std::cout)
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
    const uint64_t bytesInTupleBuffer = std::min(inputBuffer.getBufferSize(), inputBuffer.getNumberOfTuples());
    {
        const auto wlocked = *outputStream.wlock();
        const uint64_t bytesInTupleBuffer = std::min(inputBuffer.getBufferSize(), inputBuffer.getNumberOfTuples());
        wlocked->write(inputBuffer.getAvailableMemoryArea<char>().data(), bytesInTupleBuffer);
        for (size_t index = 0; index < inputBuffer.getNumberOfChildBuffers(); index++)
        {
            auto childBuffer = inputBuffer.loadChildBuffer(VariableSizedAccess::Index(index));
            wlocked->write(childBuffer.getAvailableMemoryArea<char>().data(), static_cast<int64_t>(childBuffer.getNumberOfTuples()));
        }
        wlocked->write(std::string("\n").data(), 1);
        wlocked->flush();
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
    return std::make_unique<PrintSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}

}
