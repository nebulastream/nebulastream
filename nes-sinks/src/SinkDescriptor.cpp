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
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>

#include <ErrorHandling.hpp>
#include <ProtobufHelper.hpp> /// NOLINT
#include <SerializableOperator.pb.h>
#include <SinkValidationRegistry.hpp>
#include "Serialization/IdentifierSerializationUtil.hpp"
#include "Serialization/UnboundSchemaSerializationUtl.hpp"

namespace NES
{

SerializableSinkDescriptor NamedSinkDescriptor::serialize() const
{
    SerializableSinkDescriptor serializedSinkDescriptor;
    IdentifierSerializationUtil::serializeIdentifier(getSinkName(), serializedSinkDescriptor.mutable_sinkname());
    UnboundSchemaSerializationUtil::serializeUnboundSchema(*schema, serializedSinkDescriptor.mutable_sinkschema());
    serializedSinkDescriptor.set_sinktype(sinkType);
    /// Iterate over SinkDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : getConfig())
    {
        auto* kv = serializedSinkDescriptor.mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    return serializedSinkDescriptor;
}

std::ostream& operator<<(std::ostream& out, const NamedSinkDescriptor& sinkDescriptor)
{
    out << fmt::format(
        "SinkDescriptor: (name: {}, type: {}, Config: {})", sinkDescriptor.name, sinkDescriptor.sinkType, sinkDescriptor.toStringConfig());
    return out;
}

bool operator==(const NamedSinkDescriptor& lhs, const NamedSinkDescriptor& rhs)
{
    return lhs.name == rhs.name;
}

std::string_view NamedSinkDescriptor::getFormatType() const
{
    return magic_enum::enum_name(getFromConfig(SinkDescriptor::INPUT_FORMAT));
}

std::string NamedSinkDescriptor::getSinkType() const
{
    return sinkType;
}

std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, true>> NamedSinkDescriptor::getSchema() const
{
    return schema;
}

Identifier NamedSinkDescriptor::getSinkName() const
{
    return name;
}

NamedSinkDescriptor::NamedSinkDescriptor(
    Identifier name,
    SchemaBase<UnboundFieldBase<1>, true> nameWithSchema,
    const std::string_view sinkType,
    DescriptorConfig::Config config)
    : Descriptor(std::move(config))
    , name(std::move(name))
    , schema(std::make_shared<SchemaBase<UnboundFieldBase<1>, true>>(std::move(nameWithSchema)))
    , sinkType(sinkType)
{
}

InlineSinkDescriptor::InlineSinkDescriptor(
    uint64_t sinkId,
    std::variant<std::monostate, SchemaBase<UnboundFieldBase<1>, false>, SchemaBase<UnboundFieldBase<1>, true>> schema,
    const std::string_view sinkType,
    DescriptorConfig::Config config)
    : Descriptor(std::move(config))
    , sinkId(sinkId)
    , schema(
          std::visit(
              [](auto&& arg) -> std::variant<
                                 std::monostate,
                                 std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, false>>,
                                 std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, true>>>
              {
                  using T = std::decay_t<decltype(arg)>;
                  if constexpr (std::is_same_v<T, std::monostate>)
                  {
                      return std::monostate{};
                  }
                  else
                  {
                      return std::make_shared<const T>(std::move(arg));
                  }
              },
              std::move(schema)))
    , sinkType(sinkType)
{
}

SerializableSinkDescriptor InlineSinkDescriptor::serialize() const
{
    SerializableSinkDescriptor serializedSinkDescriptor;
    IdentifierSerializationUtil::serializeIdentifier(
        Identifier::parse(fmt::format("{}", getSinkId())), serializedSinkDescriptor.mutable_sinkname());

    const auto hasOrderedSchema = std::holds_alternative<std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, true>>>(schema);
    PRECONDITION(hasOrderedSchema, "There must be an ordered schema in the inline sink descriptor set before serializing");
    const auto& orderedSchema = std::get<std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, true>>>(schema);
    UnboundSchemaSerializationUtil::serializeUnboundSchema(*orderedSchema, serializedSinkDescriptor.mutable_sinkschema());

    serializedSinkDescriptor.set_sinktype(sinkType);
    /// Iterate over SinkDescriptor config and serialize all key-value pairs.
    for (const auto& [key, value] : getConfig())
    {
        auto* kv = serializedSinkDescriptor.mutable_config();
        kv->emplace(key, descriptorConfigTypeToProto(value));
    }
    return serializedSinkDescriptor;
}

std::ostream& operator<<(std::ostream& out, const InlineSinkDescriptor& sinkDescriptor)
{
    out << fmt::format(
        "SinkDescriptor: (name: {}, type: {}, Config: {})",
        sinkDescriptor.sinkId,
        sinkDescriptor.sinkType,
        sinkDescriptor.toStringConfig());
    return out;
}

bool operator==(const InlineSinkDescriptor& lhs, const InlineSinkDescriptor& rhs)
{
    return lhs.sinkId == rhs.sinkId;
}

std::string_view InlineSinkDescriptor::getFormatType() const
{
    return magic_enum::enum_name(getFromConfig(SinkDescriptor::INPUT_FORMAT));
}

std::string InlineSinkDescriptor::getSinkType() const
{
    return sinkType;
}

std::variant<
    std::monostate,
    std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, false>>,
    std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, true>>>
InlineSinkDescriptor::getSchema() const
{
    return schema;
}

uint64_t InlineSinkDescriptor::getSinkId() const
{
    return sinkId;
}

SinkDescriptor::SinkDescriptor(std::variant<NamedSinkDescriptor, InlineSinkDescriptor> underlying)
    : Descriptor(std::visit([](const auto& var) { return var.getConfig(); }, underlying)), underlying(std::move(underlying))
{
}

std::variant<
    std::monostate,
    std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, false>>,
    std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, true>>>
SinkDescriptor::getSchema() const
{
    return std::visit(
        [](const auto& var)
        {
            return std::variant<
                std::monostate,
                std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, false>>,
                std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, true>>>{var.getSchema()};
        },
        underlying);
}

InlineSinkDescriptor InlineSinkDescriptor::withSchemaOrder(const SchemaBase<UnboundFieldBase<1>, true>& newSchema) const
{
    PRECONDITION(!std::holds_alternative<std::monostate>(this->schema), "Cannot set ordered schema directly from empty");
    const auto alreadyOrderSet = std::holds_alternative<std::shared_ptr<const SchemaBase<UnboundFieldBase<1>, true>>>(this->schema);
    PRECONDITION(!alreadyOrderSet, "Ordered schema already set");

    auto copy = *this;
    copy.schema = std::make_shared<const SchemaBase<UnboundFieldBase<1>, true>>(newSchema);
    return copy;
}

std::string_view SinkDescriptor::getFormatType() const
{
    return magic_enum::enum_name(getFromConfig(INPUT_FORMAT));
}

std::string SinkDescriptor::getSinkType() const
{
    return std::visit([](const auto& var) { return var.getSinkType(); }, underlying);
}

Identifier SinkDescriptor::getSinkName() const
{
    return std::visit(
        Overloaded{
            [](const NamedSinkDescriptor& namedDescriptor) { return namedDescriptor.getSinkName(); },
            [](const InlineSinkDescriptor& inlineDescriptor)
            { return Identifier::parse(fmt::format("{}", inlineDescriptor.getSinkId())); }},
        underlying);
}

bool SinkDescriptor::isInline() const
{
    return std::holds_alternative<InlineSinkDescriptor>(this->underlying);
}

std::optional<DescriptorConfig::Config>
SinkDescriptor::validateAndFormatConfig(const std::string_view sinkType, std::unordered_map<Identifier, std::string> configPairs)
{
    const std::unordered_map<std::string, std::string> stringConfigMap = configPairs
        | std::views::transform([](const auto& pair) { return std::make_pair(std::string{pair.first.getOriginalString()}, pair.second); })
        | std::ranges::to<std::unordered_map>();
    auto sinkValidationRegistryArguments = SinkValidationRegistryArguments{std::move(stringConfigMap)};
    return SinkValidationRegistry::instance().create(std::string{sinkType}, std::move(sinkValidationRegistryArguments));
}

std::ostream& operator<<(std::ostream& out, const SinkDescriptor& sinkDescriptor)
{
    std::visit([&out](const auto& var) { out << var; }, sinkDescriptor.underlying);
    return out;
}

bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs)
{
    return std::visit(
        Overloaded{
            [](const NamedSinkDescriptor& namedLhs, const NamedSinkDescriptor& namedRhs) { return namedLhs == namedRhs; },
            [](const InlineSinkDescriptor& inlineLhs, const InlineSinkDescriptor& inlineRhs) { return inlineLhs == inlineRhs; },
            [](const auto&, const auto&) { return false; }},
        lhs.underlying,
        rhs.underlying);
}

SerializableSinkDescriptor SinkDescriptor::serialize() const
{
    return std::visit([](const auto& var) { return var.serialize(); }, underlying);
}

const std::variant<NamedSinkDescriptor, InlineSinkDescriptor>& SinkDescriptor::getUnderlying() const
{
    return underlying;
}
}
