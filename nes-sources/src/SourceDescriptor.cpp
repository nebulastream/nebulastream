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

#include <Sources/SourceDescriptor.hpp>

#include <any>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>
#include <SourceConfigRegistry.hpp>

namespace NES
{

SourceDescriptor::SourceDescriptor(
    const PhysicalSourceId physicalSourceId,
    Schema<UnqualifiedUnboundField, Ordered> schema,
    Host host,
    const std::optional<size_t> maxInflightBuffers,
    PluginSourceConfiguration pluginData,
    InputFormatterDescriptor inputFormatterDescriptor,
    std::optional<Identifier> logicalSourceName)
    : physicalSourceId(physicalSourceId)
    , schema(std::move(schema))
    , host(host)
    , maxInflightBuffers(maxInflightBuffers)
    , pluginSourceConfig(std::move(pluginData))
    , inputFormatterDescriptor(std::move(inputFormatterDescriptor))
    , logicalSourceName(std::move(logicalSourceName))
{
}

const Schema<UnqualifiedUnboundField, Ordered>& SourceDescriptor::getSchema() const
{
    return schema;
}

const Identifier& SourceDescriptor::getSourceType() const
{
    return pluginSourceConfig.getType();
}

InputFormatterDescriptor SourceDescriptor::getInputFormatterDescriptor() const
{
    return inputFormatterDescriptor;
}

const Identifier& SourceDescriptor::getInputFormatType() const
{
    return inputFormatterDescriptor.getInputFormatterType();
}

Host SourceDescriptor::getHost() const
{
    return host;
}

std::optional<size_t> SourceDescriptor::getMaxInflightBuffers() const
{
    return maxInflightBuffers;
}

const ExplicitAny& SourceDescriptor::getPluginData() const
{
    return pluginSourceConfig.getPluginData();
}

PhysicalSourceId SourceDescriptor::getPhysicalSourceId() const
{
    return physicalSourceId;
}

std::weak_ordering operator<=>(const SourceDescriptor& lhs, const SourceDescriptor& rhs)
{
    return lhs.physicalSourceId <=> rhs.physicalSourceId;
}

std::string SourceDescriptor::explain(ExplainVerbosity verbosity) const
{
    std::stringstream stringstream;
    if (verbosity == ExplainVerbosity::Debug)
    {
        stringstream << *this;
    }
    else if (verbosity == ExplainVerbosity::Short)
    {
        stringstream << logicalSourceName.transform([](const auto& name) { return fmtToString(name); })
                            .value_or(fmt::format("{}", physicalSourceId));
    }
    return stringstream.str();
}

std::ostream& operator<<(std::ostream& out, const SourceDescriptor& descriptor)
{
    return out << fmt::format(
               "SourceDescriptor(sourceId: {}, sourceType: {}, logicalSource:{}, host: {}, inputFormatterDescriptor: {})",
               descriptor.getPhysicalSourceId(),
               descriptor.getSourceType(),
               descriptor.logicalSourceName,
               descriptor.getHost(),
               descriptor.getInputFormatterDescriptor());
}

const std::optional<Identifier>& SourceDescriptor::getLogicalSourceName() const
{
    return logicalSourceName;
}

Reflected Reflector<PluginSourceConfiguration>::operator()(const PluginSourceConfiguration& config) const
{
    const auto* entry = SourceConfigRegistry::instance().find(config.getType().asCanonicalString());
    PRECONDITION(entry != nullptr, "Unknown source type: {}", config.getType());

    detail::ReflectedPluginSourceConfiguration pluginSourceConfig{
        .type = config.getType(),
        .pluginData = entry->reflect(config.getPluginData())
    };

    return reflect(std::move(pluginSourceConfig));
}

PluginSourceConfiguration Unreflector<PluginSourceConfiguration>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [type, configData] = context.unreflect<detail::ReflectedPluginSourceConfiguration>(rfl);
    const auto* entry = SourceConfigRegistry::instance().find(type.asCanonicalString());
    if (entry == nullptr) {
        throw CannotDeserialize("Unknown source type {}", type);
    }

    return PluginSourceConfiguration{std::move(type), ExplicitAny{entry->unreflect(configData, context)}};
}

Reflected Reflector<SourceDescriptor>::operator()(const SourceDescriptor& sourceDescriptor) const
{
    detail::ReflectedSourceDescriptor descriptor{
        .physicalSourceId = sourceDescriptor.physicalSourceId,
        .schema = sourceDescriptor.schema,
        .host = sourceDescriptor.host,
        .maxInflightBuffers = sourceDescriptor.maxInflightBuffers,
        .pluginSourceConfig = sourceDescriptor.pluginSourceConfig,
        .inputFormatterDescriptor = sourceDescriptor.inputFormatterDescriptor,
        .logicalSourceName = sourceDescriptor.logicalSourceName};

    return reflect(std::move(descriptor));
}

SourceDescriptor Unreflector<SourceDescriptor>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflectedSourceDescriptor = context.unreflect<detail::ReflectedSourceDescriptor>(rfl);

    return SourceDescriptor{
        PhysicalSourceId{reflectedSourceDescriptor.physicalSourceId},
        std::move(reflectedSourceDescriptor.schema),
        std::move(reflectedSourceDescriptor.host),
        reflectedSourceDescriptor.maxInflightBuffers,
        std::move(reflectedSourceDescriptor.pluginSourceConfig),
        std::move(reflectedSourceDescriptor.inputFormatterDescriptor),
        std::move(reflectedSourceDescriptor.logicalSourceName)};
}
}
