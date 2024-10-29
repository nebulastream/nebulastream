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

#include <sstream>
#include <string>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkPrint.hpp>
#include <Sinks/SinkRegistry.hpp>
#include <SinksValidation/SinkRegistryValidation.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Sinks
{

SinkPrint::SinkPrint(const SinkDescriptor& sinkDescriptor) : outputStream(std::cout)
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

bool SinkPrint::emitTupleBuffer(Memory::TupleBuffer& inputBuffer)
{
    PRECONDITION(inputBuffer, "Invalid input buffer in SinkPrint.");

    const auto bufferAsString = outputParser->getFormattedBuffer(inputBuffer);
    outputStream << bufferAsString << std::endl;
    return true;
}

std::ostream& SinkPrint::toString(std::ostream& str) const
{
    str << fmt::format("PRINT_SINK(Writing to: std::cout, using outputParser: {}", *outputParser);
    return str;
}

std::unique_ptr<Configurations::DescriptorConfig::Config>
SinkPrint::validateAndFormat(std::unordered_map<std::string, std::string>&& config)
{
    return Configurations::DescriptorConfig::validateAndFormat<ConfigParametersPrint>(std::move(config), NAME);
}

std::unique_ptr<NES::Configurations::DescriptorConfig::Config>
SinkGeneratedRegistrarValidation::RegisterSinkValidationPrint(std::unordered_map<std::string, std::string>&& sinkConfig)
{
    return SinkPrint::validateAndFormat(std::move(sinkConfig));
}

std::unique_ptr<Sink> SinkGeneratedRegistrar::RegisterSinkPrint(const Sinks::SinkDescriptor& sinkDescriptor)
{
    return std::make_unique<SinkPrint>(sinkDescriptor);
}

}
