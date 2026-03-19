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
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <variant>

#include <Configurations/Descriptor.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>

#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

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

std::optional<std::string_view> NamedSinkDescriptor::getFormatType() const
{
    return magic_enum::enum_name(getFromConfig(SinkDescriptor::INPUT_FORMAT));
}

std::string NamedSinkDescriptor::getSinkType() const
{
    return sinkType;
}

std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>> NamedSinkDescriptor::getSchema() const
{
    return schema;
}

Identifier NamedSinkDescriptor::getSinkName() const
{
    return name;
}

NamedSinkDescriptor::NamedSinkDescriptor(
    Identifier name,
    Schema<UnqualifiedUnboundField, Ordered> nameWithSchema,
    const std::string_view sinkType,
    DescriptorConfig::Config config)
    : Descriptor(std::move(config))
    , name(std::move(name))
    , schema(std::make_shared<Schema<UnqualifiedUnboundField, Ordered>>(std::move(nameWithSchema)))
    , sinkType(sinkType)
{
}

InlineSinkDescriptor::InlineSinkDescriptor(
    uint64_t sinkId,
    std::variant<std::monostate, Schema<UnqualifiedUnboundField, Unordered>, Schema<UnqualifiedUnboundField, Ordered>> schema,
    const std::string_view sinkType,
    DescriptorConfig::Config config)
    : Descriptor(std::move(config))
    , sinkId(sinkId)
    , schema(std::visit(
          [](auto&& arg) -> std::variant<
                             std::monostate,
                             std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>,
                             std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>
          {
              using T = std::decay_t<decltype(arg)>;
              if constexpr (std::is_same_v<T, std::monostate>)
              {
                  return std::monostate{};
              }
              else
              {
                  return std::make_shared<const T>(std::forward<decltype(arg)>(arg));
              }
          },
          std::move(schema)))
    , sinkType(sinkType)
{
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

std::optional<std::string_view> InlineSinkDescriptor::getFormatType() const
{
    return magic_enum::enum_name(getFromConfig(SinkDescriptor::INPUT_FORMAT));
}

std::string InlineSinkDescriptor::getSinkType() const
{
    return sinkType;
}

std::variant<
    std::monostate,
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>,
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>
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
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>,
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>
SinkDescriptor::getSchema() const
{
    return std::visit(
        [](const auto& var)
        {
            return std::variant<
                std::monostate,
                std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>,
                std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>{var.getSchema()};
        },
        underlying);
}

InlineSinkDescriptor InlineSinkDescriptor::withSchemaOrder(const Schema<UnqualifiedUnboundField, Ordered>& newSchema) const
{
    PRECONDITION(!std::holds_alternative<std::monostate>(this->schema), "Cannot set ordered schema directly from empty");
    const auto alreadyOrderSet = std::holds_alternative<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(this->schema);
    PRECONDITION(!alreadyOrderSet, "Ordered schema already set");

    auto copy = *this;
    copy.schema = std::make_shared<const Schema<UnqualifiedUnboundField, Ordered>>(newSchema);
    return copy;
}

std::optional<std::string_view> SinkDescriptor::getFormatType() const
{
    try
    {
        return magic_enum::enum_name(getFromConfig(INPUT_FORMAT));
    }
    catch (std::out_of_range& e)
    {
        NES_WARNING("Sinks of the type {} do not have an INPUT_FORMAT parameter.", getSinkType());
        return std::nullopt;
    }
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
        | std::views::transform([](const auto& pair) { return std::make_pair(pair.first.asCanonicalString(), pair.second); })
        | std::ranges::to<std::unordered_map>();
    auto sinkValidationRegistryArguments = SinkValidationRegistryArguments{stringConfigMap};
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

Reflected Reflector<NamedSinkDescriptor>::operator()(const NamedSinkDescriptor& descriptor) const
{
    return reflect(detail::ReflectedNamedSinkDescriptor{
        .name = descriptor.getSinkName(),
        .schema = *descriptor.getSchema(),
        .sinkType = descriptor.getSinkType(),
        .config = descriptor.getReflectedConfig()});
}

NamedSinkDescriptor Unreflector<NamedSinkDescriptor>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    const auto [name, schema, sinkType, config] = context.unreflect<detail::ReflectedNamedSinkDescriptor>(reflected);
    return NamedSinkDescriptor{name, schema, sinkType, Descriptor::unreflectConfig(config, context)};
}

Reflected Reflector<InlineSinkDescriptor>::operator()(const InlineSinkDescriptor& descriptor) const
{
    using SchemaType = decltype(std::declval<detail::ReflectedInlineSinkDescriptor>().schema);
    auto schema = std::visit(
        Overloaded{
            [](const std::monostate&) { return SchemaType{std::monostate{}}; },
            [](const auto& schemaPtr) { return SchemaType{*schemaPtr}; }},
        descriptor.getSchema());

    return reflect(detail::ReflectedInlineSinkDescriptor{
        .sinkId = descriptor.getSinkId(),
        .schema = std::move(schema),
        .sinkType = descriptor.getSinkType(),
        .config = descriptor.getReflectedConfig()});
}

InlineSinkDescriptor Unreflector<InlineSinkDescriptor>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [sinkId, schema, sinkType, config] = context.unreflect<detail::ReflectedInlineSinkDescriptor>(reflected);
    return InlineSinkDescriptor{sinkId, schema, sinkType, Descriptor::unreflectConfig(config, context)};
}

Reflected Reflector<SinkDescriptor>::operator()(const SinkDescriptor& descriptor) const
{
    return reflect(descriptor.underlying);
}

SinkDescriptor Unreflector<SinkDescriptor>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    using UnderlyingType = std::decay_t<decltype(std::declval<SinkDescriptor>().underlying)>;
    return SinkDescriptor{context.unreflect<UnderlyingType>(reflected)};
}

const std::variant<NamedSinkDescriptor, InlineSinkDescriptor>& SinkDescriptor::getUnderlying() const
{
    return underlying;
}
}
