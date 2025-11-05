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

#include <cstdint>
#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>

#include <ErrorHandling.hpp>
#include <ProtobufHelper.hpp> /// NOLINT
#include <SerializableOperator.pb.h>
#include <SinkValidationRegistry.hpp>

namespace NES
{

SinkDescriptor::SinkDescriptor(
    std::variant<std::string, uint64_t> sinkName, const Schema& schema, const std::string_view sinkType, DescriptorConfig::Config config)
    : Descriptor(std::move(config)), sinkName(std::move(sinkName)), schema(std::make_shared<Schema>(schema)), sinkType(sinkType)
{
}

std::shared_ptr<const Schema> SinkDescriptor::getSchema() const
{
    return schema;
}

std::string_view SinkDescriptor::getFormatType() const
{
    return magic_enum::enum_name(getFromConfig(INPUT_FORMAT));
}

std::string SinkDescriptor::getSinkType() const
{
    return sinkType;
}

std::string SinkDescriptor::getSinkName() const
{
    return std::visit(
        Overloaded{
            [](const std::string& name) { return name; },
            [](const uint64_t& name) { return std::to_string(name); },
        },
        sinkName);
}

bool SinkDescriptor::isInline() const
{
    return std::holds_alternative<uint64_t>(this->sinkName);
}

std::optional<DescriptorConfig::Config>
SinkDescriptor::validateAndFormatConfig(const std::string_view sinkType, std::unordered_map<std::string, std::string> configPairs)
{
    auto sinkValidationRegistryArguments = SinkValidationRegistryArguments{std::move(configPairs)};
    return SinkValidationRegistry::instance().create(std::string{sinkType}, std::move(sinkValidationRegistryArguments));
}

std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor)
{
    out << fmt::format(
        "SinkDescriptor: (name: {}, type: {}, Config: {})",
        sinkDescriptor.sinkName,
        sinkDescriptor.sinkType,
        sinkDescriptor.toStringConfig());
    return out;
}

bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs)
{
    return lhs.sinkName == rhs.sinkName;
}

SerializableSinkDescriptor SinkDescriptor::serialize() const
{
    SerializableSinkDescriptor serializedSinkDescriptor;
    serializedSinkDescriptor.set_sinkname(getSinkName());
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
