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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

#include <Identifiers/Identifiers.hpp>
#include <Util/Overloaded.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>

#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <OutputFormatters/OutputFormatterDescriptor.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Util/Any.hpp>
#include <ErrorHandling.hpp>
#include <SinkConfigRegistry.hpp>

namespace NES
{

SinkDescriptor SinkDescriptor::createNamed(
    SinkId id,
    Identifier name,
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    GeneralSinkConfig general,
    PluginSinkConfiguration plugin,
    OutputFormatterDescriptor formatter)
{
    PRECONDITION(general.host != Host{Host::INVALID}, "A valid host must be set before creating a sink descriptor (see host policy)");
    return SinkDescriptor{NamedSinkDescriptor{
        id,
        std::move(name),
        schema,
        std::move(general.host),
        general.addTimestamp,
        general.backpressureUpperThreshold,
        general.backpressureLowerThreshold,
        std::move(plugin),
        std::move(formatter)}};
}

SinkDescriptor SinkDescriptor::createAnonymous(
    SinkId id, AnonymousSinkSchema schema, GeneralSinkConfig general, PluginSinkConfiguration plugin, OutputFormatterDescriptor formatter)
{
    PRECONDITION(general.host != Host{Host::INVALID}, "A valid host must be set before creating a sink descriptor (see host policy)");
    return SinkDescriptor{AnonymousSinkDescriptor{
        id,
        std::move(schema),
        std::move(general.host),
        general.addTimestamp,
        general.backpressureUpperThreshold,
        general.backpressureLowerThreshold,
        std::move(plugin),
        std::move(formatter)}};
}

std::ostream& operator<<(std::ostream& out, const NamedSinkDescriptor& sinkDescriptor)
{
    out << fmt::format(
        "SinkDescriptor: (name: {}, type: {}, formatter: {})",
        sinkDescriptor.name,
        sinkDescriptor.getSinkType(),
        sinkDescriptor.outputFormatterDescriptor);
    return out;
}

bool operator==(const NamedSinkDescriptor& lhs, const NamedSinkDescriptor& rhs)
{
    return lhs.name == rhs.name;
}

std::string NamedSinkDescriptor::getFormatType() const
{
    return outputFormatterDescriptor.getOutputFormatterType().asCanonicalString();
}

std::string NamedSinkDescriptor::getSinkType() const
{
    return pluginSinkConfig.getType().asCanonicalString();
}

std::shared_ptr<const Schema<UnqualifiedUnboundField, Ordered>> NamedSinkDescriptor::getSchema() const
{
    return schema;
}

SinkId NamedSinkDescriptor::getSinkId() const
{
    return sinkId;
}

Identifier NamedSinkDescriptor::getSinkName() const
{
    return name;
}

Host NamedSinkDescriptor::getHost() const
{
    return host;
}

const PluginSinkConfiguration& NamedSinkDescriptor::getPluginSinkConfiguration() const
{
    return pluginSinkConfig;
}

const OutputFormatterDescriptor& NamedSinkDescriptor::getOutputFormatterDescriptor() const
{
    return outputFormatterDescriptor;
}

NamedSinkDescriptor::NamedSinkDescriptor(
    SinkId sinkId,
    Identifier name,
    Schema<UnqualifiedUnboundField, Ordered> nameWithSchema,
    Host host,
    const bool addTimestamp,
    const size_t backpressureUpperThreshold,
    const size_t backpressureLowerThreshold,
    PluginSinkConfiguration pluginSinkConfig,
    OutputFormatterDescriptor outputFormatterDescriptor)
    : sinkId(sinkId)
    , name(std::move(name))
    , schema(std::make_shared<Schema<UnqualifiedUnboundField, Ordered>>(std::move(nameWithSchema)))
    , host(std::move(host))
    , addTimestamp(addTimestamp)
    , backpressureUpperThreshold(backpressureUpperThreshold)
    , backpressureLowerThreshold(backpressureLowerThreshold)
    , pluginSinkConfig(std::move(pluginSinkConfig))
    , outputFormatterDescriptor(std::move(outputFormatterDescriptor))
{
}

bool NamedSinkDescriptor::getAddTimestamp() const
{
    return addTimestamp;
}

size_t NamedSinkDescriptor::getBackpressureUpperThreshold() const
{
    return backpressureUpperThreshold;
}

size_t NamedSinkDescriptor::getBackpressureLowerThreshold() const
{
    return backpressureLowerThreshold;
}

AnonymousSinkDescriptor::AnonymousSinkDescriptor(
    SinkId sinkId,
    AnonymousSinkSchema schema,
    Host host,
    const bool addTimestamp,
    const size_t backpressureUpperThreshold,
    const size_t backpressureLowerThreshold,
    PluginSinkConfiguration pluginSinkConfig,
    OutputFormatterDescriptor outputFormatterDescriptor)
    : sinkId(sinkId)
    , schema(std::move(schema))
    , host(std::move(host))
    , addTimestamp(addTimestamp)
    , backpressureUpperThreshold(backpressureUpperThreshold)
    , backpressureLowerThreshold(backpressureLowerThreshold)
    , pluginSinkConfig(std::move(pluginSinkConfig))
    , outputFormatterDescriptor(std::move(outputFormatterDescriptor))
{
}

bool AnonymousSinkDescriptor::getAddTimestamp() const
{
    return addTimestamp;
}

size_t AnonymousSinkDescriptor::getBackpressureUpperThreshold() const
{
    return backpressureUpperThreshold;
}

size_t AnonymousSinkDescriptor::getBackpressureLowerThreshold() const
{
    return backpressureLowerThreshold;
}

std::ostream& operator<<(std::ostream& out, const AnonymousSinkDescriptor& sinkDescriptor)
{
    out << fmt::format(
        "SinkDescriptor: (name: {}, type: {}, host: {}, formatter: {})",
        sinkDescriptor.sinkId,
        sinkDescriptor.getSinkType(),
        sinkDescriptor.host,
        sinkDescriptor.outputFormatterDescriptor);
    return out;
}

bool operator==(const AnonymousSinkDescriptor& lhs, const AnonymousSinkDescriptor& rhs)
{
    return lhs.sinkId == rhs.sinkId;
}

std::string AnonymousSinkDescriptor::getFormatType() const
{
    return outputFormatterDescriptor.getOutputFormatterType().asCanonicalString();
}

std::string AnonymousSinkDescriptor::getSinkType() const
{
    return pluginSinkConfig.getType().asCanonicalString();
}

AnonymousSinkSchema AnonymousSinkDescriptor::getSchema() const
{
    return schema;
}

SinkId AnonymousSinkDescriptor::getSinkId() const
{
    return sinkId;
}

Host AnonymousSinkDescriptor::getHost() const
{
    return host;
}

const PluginSinkConfiguration& AnonymousSinkDescriptor::getPluginSinkConfiguration() const
{
    return pluginSinkConfig;
}

const OutputFormatterDescriptor& AnonymousSinkDescriptor::getOutputFormatterDescriptor() const
{
    return outputFormatterDescriptor;
}

SinkDescriptor::SinkDescriptor(std::variant<NamedSinkDescriptor, AnonymousSinkDescriptor> underlying) : underlying(std::move(underlying))
{
}

AnonymousSinkSchema SinkDescriptor::getSchema() const
{
    return std::visit(
        Overloaded{
            [](const NamedSinkDescriptor& named) { return AnonymousSinkSchema{named.getSchema()}; },
            [](const AnonymousSinkDescriptor& anonymous) { return anonymous.getSchema(); }},
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
    return std::visit([](const auto& var) { return var.getFormatType(); }, underlying);
}

std::string SinkDescriptor::getSinkType() const
{
    return std::visit([](const auto& var) { return var.getSinkType(); }, underlying);
}

SinkId SinkDescriptor::getSinkId() const
{
    return std::visit([](const auto& sink) { return sink.getSinkId(); }, underlying);
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

const ExplicitAny& SinkDescriptor::getPluginData() const
{
    return getPluginSinkConfiguration().getPluginData();
}

const PluginSinkConfiguration& SinkDescriptor::getPluginSinkConfiguration() const
{
    return std::visit([](const auto& var) -> const PluginSinkConfiguration& { return var.getPluginSinkConfiguration(); }, underlying);
}

const OutputFormatterDescriptor& SinkDescriptor::getOutputFormatterDescriptor() const
{
    return std::visit([](const auto& var) -> const OutputFormatterDescriptor& { return var.getOutputFormatterDescriptor(); }, underlying);
}

bool SinkDescriptor::isAnonymous() const
{
    return std::holds_alternative<AnonymousSinkDescriptor>(this->underlying);
}

Host SinkDescriptor::getHost() const
{
    return std::visit([](const auto& var) { return var.getHost(); }, underlying);
}

bool SinkDescriptor::getAddTimestamp() const
{
    return std::visit([](const auto& var) { return var.getAddTimestamp(); }, underlying);
}

size_t SinkDescriptor::getBackpressureUpperThreshold() const
{
    return std::visit([](const auto& var) { return var.getBackpressureUpperThreshold(); }, underlying);
}

size_t SinkDescriptor::getBackpressureLowerThreshold() const
{
    return std::visit([](const auto& var) { return var.getBackpressureLowerThreshold(); }, underlying);
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

namespace
{
struct ReflectedPluginSinkConfiguration
{
    Identifier type;
    Reflected pluginData;
};
}

Reflected Reflector<PluginSinkConfiguration>::operator()(const PluginSinkConfiguration& config, const ReflectionContext& context) const
{
    const auto configEntry = SinkConfigRegistry::instance().find(config.getType().asCanonicalString());
    INVARIANT(configEntry.has_value(), "Sink type {} has a descriptor but no SinkConfigRegistry entry", config.getType());
    return context.reflect(
        ReflectedPluginSinkConfiguration{.type = config.getType(), .pluginData = configEntry->reflect(config.getPluginData(), context)});
}

PluginSinkConfiguration Unreflector<PluginSinkConfiguration>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflectedConfig = context.unreflect<ReflectedPluginSinkConfiguration>(rfl);
    const auto configEntry = SinkConfigRegistry::instance().find(reflectedConfig.type.asCanonicalString());
    if (not configEntry.has_value())
    {
        throw UnknownSinkType("Cannot deserialize sink descriptor: type {} has no SinkConfigRegistry entry", reflectedConfig.type);
    }
    return PluginSinkConfiguration{
        std::move(reflectedConfig.type), ExplicitAny{configEntry->unreflect(reflectedConfig.pluginData, context)}};
}

Reflected Reflector<NamedSinkDescriptor>::operator()(const NamedSinkDescriptor& descriptor, const ReflectionContext& context) const
{
    return context.reflect(
        detail::ReflectedNamedSinkDescriptor{
            .sinkId = descriptor.getSinkId(),
            .name = descriptor.getSinkName(),
            .schema = *descriptor.getSchema(),
            .host = descriptor.getHost(),
            .addTimestamp = descriptor.getAddTimestamp(),
            .backpressureUpperThreshold = descriptor.getBackpressureUpperThreshold(),
            .backpressureLowerThreshold = descriptor.getBackpressureLowerThreshold(),
            .pluginSinkConfig = descriptor.getPluginSinkConfiguration(),
            .outputFormatterDescriptor = descriptor.getOutputFormatterDescriptor()});
}

NamedSinkDescriptor Unreflector<NamedSinkDescriptor>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto
        [sinkId,
         name,
         schema,
         host,
         addTimestamp,
         backpressureUpperThreshold,
         backpressureLowerThreshold,
         pluginSinkConfig,
         outputFormatterDescriptor] = context.unreflect<detail::ReflectedNamedSinkDescriptor>(reflected);
    return NamedSinkDescriptor{
        sinkId,
        std::move(name),
        std::move(schema),
        std::move(host),
        addTimestamp,
        backpressureUpperThreshold,
        backpressureLowerThreshold,
        std::move(pluginSinkConfig),
        std::move(outputFormatterDescriptor)};
}

Reflected Reflector<AnonymousSinkDescriptor>::operator()(const AnonymousSinkDescriptor& descriptor, const ReflectionContext& context) const
{
    using SchemaType = decltype(std::declval<detail::ReflectedAnonymousSinkDescriptor>().schema);
    auto schema = std::visit(
        Overloaded{
            [](const std::monostate&) { return SchemaType{std::monostate{}}; },
            [](const auto& schemaPtr) { return SchemaType{*schemaPtr}; }},
        descriptor.getSchema());

    return context.reflect(
        detail::ReflectedAnonymousSinkDescriptor{
            .sinkId = descriptor.getSinkId(),
            .schema = std::move(schema),
            .host = descriptor.getHost(),
            .addTimestamp = descriptor.getAddTimestamp(),
            .backpressureUpperThreshold = descriptor.getBackpressureUpperThreshold(),
            .backpressureLowerThreshold = descriptor.getBackpressureLowerThreshold(),
            .pluginSinkConfig = descriptor.getPluginSinkConfiguration(),
            .outputFormatterDescriptor = descriptor.getOutputFormatterDescriptor()});
}

AnonymousSinkDescriptor Unreflector<AnonymousSinkDescriptor>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto
        [sinkId,
         schema,
         host,
         addTimestamp,
         backpressureUpperThreshold,
         backpressureLowerThreshold,
         pluginSinkConfig,
         outputFormatterDescriptor] = context.unreflect<detail::ReflectedAnonymousSinkDescriptor>(reflected);
    auto sinkSchema = std::visit(
        [](auto&& arg) -> AnonymousSinkSchema
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
        std::move(schema));
    return AnonymousSinkDescriptor{
        sinkId,
        std::move(sinkSchema),
        std::move(host),
        addTimestamp,
        backpressureUpperThreshold,
        backpressureLowerThreshold,
        std::move(pluginSinkConfig),
        std::move(outputFormatterDescriptor)};
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
