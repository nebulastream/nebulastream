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
#include <Identifiers/Identifiers.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <fmt/ranges.h>
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
    try
    {
        return getFromConfig(SinkDescriptor::OUTPUT_FORMAT);
    }
    catch (std::out_of_range& e)
    {
        /// If no output format is set, then the format will be native
        return "Native";
    }
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

Host NamedSinkDescriptor::getHost() const
{
    return host;
}

std::unordered_map<Identifier, std::string> NamedSinkDescriptor::getOutputFormatterConfig() const
{
    return formatConfig;
}

NamedSinkDescriptor::NamedSinkDescriptor(
    Identifier name,
    Schema<UnqualifiedUnboundField, Ordered> nameWithSchema,
    const std::string_view sinkType,
    Host host,
    std::unordered_map<Identifier, std::string> formatConfig,
    DescriptorConfig::Config config)
    : Descriptor(std::move(config))
    , name(std::move(name))
    , schema(std::make_shared<Schema<UnqualifiedUnboundField, Ordered>>(std::move(nameWithSchema)))
    , sinkType(sinkType)
    , host(std::move(host))
    , formatConfig(std::move(formatConfig))
{
}

AnonymousSinkDescriptor::AnonymousSinkDescriptor(
    uint64_t sinkId,
    std::variant<std::monostate, Schema<UnqualifiedUnboundField, Unordered>, Schema<UnqualifiedUnboundField, Ordered>> schema,
    const std::string_view sinkType,
    Host host,
    std::unordered_map<Identifier, std::string> formatConfig,
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
    , host(std::move(host))
    , formatConfig(std::move(formatConfig))
{
}

std::ostream& operator<<(std::ostream& out, const AnonymousSinkDescriptor& sinkDescriptor)
{
    out << fmt::format(
        "SinkDescriptor: (name: {}, type: {}, host: {}, Config: {})",
        sinkDescriptor.sinkId,
        sinkDescriptor.sinkType,
        sinkDescriptor.host,
        sinkDescriptor.toStringConfig());
    return out;
}

bool operator==(const AnonymousSinkDescriptor& lhs, const AnonymousSinkDescriptor& rhs)
{
    return lhs.sinkId == rhs.sinkId;
}

std::string AnonymousSinkDescriptor::getFormatType() const
{
    try
    {
        return getFromConfig(SinkDescriptor::OUTPUT_FORMAT);
    }
    catch (std::out_of_range& e)
    {
        return "Native";
    }
}

std::string AnonymousSinkDescriptor::getSinkType() const
{
    return sinkType;
}

std::variant<
    std::monostate,
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Unordered>>,
    std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>
AnonymousSinkDescriptor::getSchema() const
{
    return schema;
}

uint64_t AnonymousSinkDescriptor::getSinkId() const
{
    return sinkId;
}

Host AnonymousSinkDescriptor::getHost() const
{
    return host;
}

std::unordered_map<Identifier, std::string> AnonymousSinkDescriptor::getOutputFormatterConfig() const
{
    return formatConfig;
}

SinkDescriptor::SinkDescriptor(std::variant<NamedSinkDescriptor, AnonymousSinkDescriptor> underlying)
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

AnonymousSinkDescriptor AnonymousSinkDescriptor::withSchemaOrder(const Schema<UnqualifiedUnboundField, Ordered>& newSchema) const
{
    PRECONDITION(!std::holds_alternative<std::monostate>(this->schema), "Cannot set ordered schema directly from empty");
    const auto alreadyOrderSet = std::holds_alternative<std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>>>(this->schema);
    PRECONDITION(!alreadyOrderSet, "Ordered schema already set");

    auto copy = *this;
    copy.schema = std::make_shared<const Schema<UnqualifiedUnboundField, Ordered>>(newSchema);
    return copy;
}

std::string SinkDescriptor::getFormatType() const
{
    try
    {
        return std::visit([](const auto& var) { return var.getFromConfig(OUTPUT_FORMAT); }, underlying);
    }
    catch (std::out_of_range& e)
    {
        /// If no output format is set, then the format will be native
        return "Native";
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
            [](const AnonymousSinkDescriptor& anonymousDescriptor)
            { return Identifier::parse(fmt::format("{}", anonymousDescriptor.getSinkId())); }},
        underlying);
}

std::unordered_map<Identifier, std::string> SinkDescriptor::getOutputFormatterConfig() const
{
    return std::visit([](const auto& var) { return var.getOutputFormatterConfig(); }, underlying);
}

bool SinkDescriptor::isAnonymous() const
{
    return std::holds_alternative<AnonymousSinkDescriptor>(this->underlying);
}

Host SinkDescriptor::getHost() const
{
    return std::visit([](const auto& var) { return var.getHost(); }, underlying);
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
            [](const AnonymousSinkDescriptor& anonymousLhs, const AnonymousSinkDescriptor& anonymousRhs)
            { return anonymousLhs == anonymousRhs; },
            [](const auto&, const auto&) { return false; }},
        lhs.underlying,
        rhs.underlying);
}

Reflected Reflector<NamedSinkDescriptor>::operator()(const NamedSinkDescriptor& descriptor, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedNamedSinkDescriptor{
        .name = descriptor.getSinkName(),
        .schema = *descriptor.getSchema(),
        .sinkType = descriptor.getSinkType(),
        .host = descriptor.getHost(),
        .formatConfig = context.reflect(descriptor.getOutputFormatterConfig()),
        .config = descriptor.getReflectedConfig(context)});
}

NamedSinkDescriptor Unreflector<NamedSinkDescriptor>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    const auto [name, schema, sinkType, host, formatConfig, config] = context.unreflect<detail::ReflectedNamedSinkDescriptor>(reflected);
    const auto unreflectedFormatConfig = context.unreflect<std::unordered_map<Identifier, std::string>>(formatConfig);
    return NamedSinkDescriptor{name, schema, sinkType, host, unreflectedFormatConfig, Descriptor::unreflectConfig(config, context)};
}

Reflected Reflector<AnonymousSinkDescriptor>::operator()(const AnonymousSinkDescriptor& descriptor, const ReflectionContext& context) const
{
    using SchemaType = decltype(std::declval<detail::ReflectedAnonymousSinkDescriptor>().schema);
    auto schema = std::visit(
        Overloaded{
            [](const std::monostate&) { return SchemaType{std::monostate{}}; },
            [](const auto& schemaPtr) { return SchemaType{*schemaPtr}; }},
        descriptor.getSchema());

    return context.reflect(detail::ReflectedAnonymousSinkDescriptor{
        .sinkId = descriptor.getSinkId(),
        .schema = std::move(schema),
        .sinkType = descriptor.getSinkType(),
        .host = descriptor.getHost(),
        .formatConfig = context.reflect(descriptor.getOutputFormatterConfig()),
        .config = descriptor.getReflectedConfig(context)});
}

AnonymousSinkDescriptor Unreflector<AnonymousSinkDescriptor>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [sinkId, schema, sinkType, host, formatConfig, config] = context.unreflect<detail::ReflectedAnonymousSinkDescriptor>(reflected);
    auto unreflectedFormatConfig = context.unreflect<std::unordered_map<Identifier, std::string>>(formatConfig);
    return AnonymousSinkDescriptor{sinkId, schema, sinkType, host, unreflectedFormatConfig, Descriptor::unreflectConfig(config, context)};
}

Reflected Reflector<SinkDescriptor>::operator()(const SinkDescriptor& descriptor, const ReflectionContext& context) const
{
    return context.reflect(descriptor.underlying);
}

SinkDescriptor Unreflector<SinkDescriptor>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    using UnderlyingType = std::decay_t<decltype(std::declval<SinkDescriptor>().underlying)>;
    return SinkDescriptor{context.unreflect<UnderlyingType>(reflected)};
}

const std::variant<NamedSinkDescriptor, AnonymousSinkDescriptor>& SinkDescriptor::getUnderlying() const
{
    return underlying;
}
}
