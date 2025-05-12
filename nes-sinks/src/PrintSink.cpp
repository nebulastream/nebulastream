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
#include <Configurations/ConfigurationsNames.hpp>
#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/PrintSink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{

PrintSink::PrintSink(Valve valve, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(valve)), outputStream(&std::cout), ingestion(sinkDescriptor.getFromConfig(ConfigParametersPrint::INGESTION))
{
    switch (const auto inputFormat = sinkDescriptor.getFromConfig(ConfigParametersPrint::INPUT_FORMAT))
    {
        case Configurations::InputFormat::CSV:
            outputParser = std::make_unique<CSVFormat>(sinkDescriptor.schema);
            break;
        default:
            throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
    }
}
void PrintSink::start(PipelineExecutionContext&)
{
}

void PrintSink::stop(PipelineExecutionContext&)
{
}

void PrintSink::execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in PrintSink.");

    const auto bufferAsString = outputParser->getFormattedBuffer(inputBuffer);
    *(*outputStream.wlock()) << bufferAsString << '\n';
    std::this_thread::sleep_for(std::chrono::milliseconds{ingestion});
}

std::ostream& PrintSink::toString(std::ostream& str) const
{
    str << fmt::format("PRINT_SINK(Writing to: std::cout, using outputParser: {}", *outputParser);
    return str;
}

Configurations::DescriptorConfig::Config PrintSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersPrint>(std::move(config), NAME);
}

SinkValidationRegistryReturnType SinkValidationGeneratedRegistrar::RegisterPrintSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return PrintSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterPrintSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<PrintSink>(std::move(sinkRegistryArguments.valve), sinkRegistryArguments.sinkDescriptor);
}

}
