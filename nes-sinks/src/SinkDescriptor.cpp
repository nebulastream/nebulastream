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

#include <ostream>
#include <sstream>
#include <API/Schema.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksValidation/SinkRegistryValidation.hpp>
#include <ErrorHandling.hpp>

namespace NES::Sinks
{

SinkDescriptor::SinkDescriptor(std::string sinkType, Configurations::DescriptorConfig::Config&& config, bool addTimestamp)
    : Descriptor(std::move(config)), sinkType(std::move(sinkType)), addTimestamp(addTimestamp)
{
}

SinkDescriptor::SinkDescriptor(
    std::shared_ptr<Schema> schema, std::string sinkType, Configurations::DescriptorConfig::Config&& config, bool addTimestamp)
    : Descriptor(std::move(config)), schema(std::move(schema)), sinkType(std::move(sinkType)), addTimestamp(addTimestamp)
{
}
std::unique_ptr<SinkDescriptor> SinkDescriptor::create(std::string sinkType, std::unordered_map<std::string, std::string> configPairs)
{
    if (auto validatedConfig = SinkRegistryValidation::instance().tryCreate(sinkType, std::move(configPairs)))
    {
        return std::make_unique<SinkDescriptor>(std::move(sinkType), std::move(validatedConfig.value()), true);
    }
    throw UnknownSinkType(fmt::format("Cannot find sink type: {} in validation registry for sinks.", sinkType));
}

std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor)
{
    return out << "SinkDescriptor:"
               << "\nSink type: " << sinkDescriptor.sinkType << "\nadd timestamp: " << sinkDescriptor.addTimestamp << "\nConfig:\n"
               << sinkDescriptor.toStringConfig(); ///-Todo: use ostream(Config) from Descriptor?
}

bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs)
{
    return lhs.sinkType == rhs.sinkType && lhs.config == rhs.config;
}

} /// namespace NES
