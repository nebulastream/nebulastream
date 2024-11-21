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

#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/PrintSink.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Sinks/SinkRegistry.hpp>
#include <SinksValidation/SinkValidationRegistry.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sinks
{

PrintSink::PrintSink(const QueryId queryId, const SinkDescriptor& sinkDescriptor) : Sink(queryId), outputStream(std::cout)
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

bool PrintSink::emitTupleBuffer(Memory::TupleBuffer& tupleBuffer)
{
    PRECONDITION(tupleBuffer, "Invalid input buffer in PrintSink.");

    const auto bufferAsString = outputParser->getFormattedBuffer(tupleBuffer);
    outputStream << bufferAsString << std::endl;
    return true;
}

std::ostream& PrintSink::toString(std::ostream& str) const
{
    str << fmt::format("PRINT_SINK(Writing to: std::cout, using outputParser: {}", *outputParser);
    return str;
}

bool PrintSink::equals(const Sink& other) const
{
    return this->queryId == other.queryId;
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

std::unique_ptr<Sink> SinkGeneratedRegistrar::RegisterPrintSink(const QueryId queryId, const Sinks::SinkDescriptor& sinkDescriptor)
{
    return std::make_unique<PrintSink>(queryId, sinkDescriptor);
}

}
