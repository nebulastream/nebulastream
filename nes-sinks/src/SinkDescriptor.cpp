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
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
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
SinkDescriptor::SinkDescriptor(
    std::string sinkName,
    std::shared_ptr<const Schema>&& schema,
    const std::string_view sinkType,
    Configurations::DescriptorConfig::Config&& config)
    : Descriptor(std::move(config)), sinkName(std::move(sinkName)), schema(std::move(schema)), sinkType(sinkType)
{
}


std::shared_ptr<const Schema> SinkDescriptor::getSchema() const
{
    return schema;
}

std::string SinkDescriptor::getSinkType() const
{
    return sinkType;
}

std::string SinkDescriptor::getSinkName() const
{
    return sinkName;
}


std::optional<Configurations::DescriptorConfig::Config>
SinkDescriptor::validateAndFormatConfig(const std::string_view sinkType, std::unordered_map<std::string, std::string> configPairs)
{
    auto sinkValidationRegistryArguments = NES::Sinks::SinkValidationRegistryArguments(std::move(configPairs));
    return SinkValidationRegistry::instance().create(std::string{sinkType}, std::move(sinkValidationRegistryArguments));
}

std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor)
{
    fmt::println(
        out,
        "SinkDescriptor: (name: {}, type: {}, Config: {})",
        sinkDescriptor.sinkName,
        sinkDescriptor.sinkType,
        sinkDescriptor.toStringConfig());
    return out;
}

bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs)
{
    return lhs.sinkType == rhs.sinkType and lhs.getConfig() == rhs.getConfig() and lhs.schema == rhs.schema;
}

SerializableSinkDescriptor SinkDescriptor::serialize() const
{
    SerializableSinkDescriptor serializedSinkDescriptor;
    serializedSinkDescriptor.set_sinkname(sinkName);
    SchemaSerializationUtil::serializeSchema(*schema, serializedSinkDescriptor.mutable_sinkschema());
    serializedSinkDescriptor.set_sinktype(sinkType);
    /// Iterate over SinkDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : getConfig())
    {
        auto* kv = serializedSinkDescriptor.mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    return serializedSinkDescriptor;
}

}
