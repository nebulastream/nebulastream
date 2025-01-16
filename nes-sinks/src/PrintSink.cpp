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
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkRegistry.hpp>
#include <SinksValidation/SinkValidationRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <magic_enum.hpp>

namespace NES::Sinks
{

PrintSink::PrintSink(const SinkDescriptor& sinkDescriptor) : outputStream(&std::cout)
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
void PrintSink::start(Runtime::Execution::PipelineExecutionContext&)
{
}

void PrintSink::stop(Runtime::Execution::PipelineExecutionContext&)
{
}

void PrintSink::execute(const Memory::TupleBuffer& inputBuffer, Runtime::Execution::PipelineExecutionContext&)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in PrintSink.");

    const auto bufferAsString = outputParser->getFormattedBuffer(inputBuffer);
    *(*outputStream.wlock()) << bufferAsString << '\n';
}

std::ostream& PrintSink::toString(std::ostream& str) const
{
    str << fmt::format("PRINT_SINK(Writing to: std::cout, using outputParser: {}", *outputParser);
    return str;
}

std::unique_ptr<Configurations::DescriptorConfig::Config>
PrintSink::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersPrint>(std::move(config), NAME);
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SinkValidationGeneratedRegistrar::RegisterSinkValidationPrint(std::unordered_map<std::string, std::string>&& sinkConfig)
{
    return PrintSink::validateAndFormat(std::move(sinkConfig));
}

std::unique_ptr<Sink> SinkGeneratedRegistrar::RegisterPrintSink(const Sinks::SinkDescriptor& sinkDescriptor)
{
    return std::make_unique<PrintSink>(sinkDescriptor);
}

}
