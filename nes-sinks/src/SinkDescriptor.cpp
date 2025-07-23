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

#include <Sinks/SinkDescriptor.hpp>

#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <ErrorHandling.hpp>
#include <ProtobufHelper.hpp> /// NOLINT
#include <SerializableOperator.pb.h>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{

SinkDescriptor::SinkDescriptor(std::string sinkType, DescriptorConfig::Config&& config, bool addTimestamp)
    : Descriptor(std::move(config)), sinkType(std::move(sinkType)), addTimestamp(addTimestamp)
{
}

DescriptorConfig::Config
SinkDescriptor::validateAndFormatConfig(const std::string& sinkType, std::unordered_map<std::string, std::string> configPairs)
{
    auto sinkValidationRegistryArguments = NES::Sinks::SinkValidationRegistryArguments(std::move(configPairs));
    if (auto validatedConfig = SinkValidationRegistry::instance().create(sinkType, std::move(sinkValidationRegistryArguments)))
    {
        return validatedConfig.value();
    }
    throw UnknownSinkType(fmt::format("Cannot find sink type: {} in validation registry for sinks.", sinkType));
}

std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor)
{
    out << fmt::format(
        "SinkDescriptor: (type: {}, add_timestamp: {}, Config: {})",
        sinkDescriptor.sinkType,
        sinkDescriptor.addTimestamp,
        sinkDescriptor.toStringConfig());
    return out;
}

bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs)
{
    return lhs.sinkType == rhs.sinkType and lhs.getConfig() == rhs.getConfig() and lhs.addTimestamp == rhs.addTimestamp
        and lhs.schema == rhs.schema;
}

SerializableSinkDescriptor SinkDescriptor::serialize() const
{
    SerializableSinkDescriptor serializedSinkDescriptor;
    SchemaSerializationUtil::serializeSchema(schema, serializedSinkDescriptor.mutable_sinkschema());
    serializedSinkDescriptor.set_sinktype(sinkType);
    serializedSinkDescriptor.set_addtimestamp(addTimestamp);
    /// Iterate over SinkDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : getConfig())
    {
        auto* kv = serializedSinkDescriptor.mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    return serializedSinkDescriptor;
}

}
