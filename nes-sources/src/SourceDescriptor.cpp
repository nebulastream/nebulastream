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
    LogicalSource logicalSource,
    std::string_view sourceType,
    Host host,
    const std::optional<size_t> maxInflightBuffers,
    std::any pluginData,
    const InputFormatterDescriptor& inputFormatterDescriptor)
    : physicalSourceId(physicalSourceId)
    , logicalSource(std::move(logicalSource))
    , sourceType(sourceType)
    , host(std::move(host))
    , maxInflightBuffers(maxInflightBuffers)
    , pluginData(std::move(pluginData))
    , inputFormatterDescriptor(inputFormatterDescriptor)
{
}

const Schema<UnqualifiedUnboundField, Ordered>& SourceDescriptor::getSchema() const
{
    return schema;
}

std::string SourceDescriptor::getSourceType() const
{
    return sourceType;
}

InputFormatterDescriptor SourceDescriptor::getInputFormatterDescriptor() const
{
    return inputFormatterDescriptor;
}

std::string SourceDescriptor::getInputFormatType() const
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

const std::any& SourceDescriptor::getPluginData() const
{
    return pluginData;
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
        stringstream << fmt::format("{}", logicalSource.getLogicalSourceName());
    }
    return stringstream.str();
}

std::ostream& operator<<(std::ostream& out, const SourceDescriptor& descriptor)
{
    return out << fmt::format(
               "SourceDescriptor(sourceId: {}, sourceType: {}, logicalSource:{}, host: {}, inputFormatterDescriptor: {})",
               descriptor.getPhysicalSourceId(),
               descriptor.getSourceType(),
               descriptor.getLogicalSource(),
               descriptor.getHost(),
               descriptor.getInputFormatterDescriptor());
}

Reflected Reflector<SourceDescriptor>::operator()(const SourceDescriptor& sourceDescriptor) const
{
    /// The wire format carries the source-defined config struct, serialized by the source's
    /// SourceConfigRegistry entry — the only place that knows the concrete type behind the std::any.
    const auto* configEntry = SourceConfigRegistry::instance().find(sourceDescriptor.sourceType);
    INVARIANT(
        configEntry != nullptr,
        "Source type {} has a descriptor but no SourceConfigRegistry entry",
        sourceDescriptor.sourceType);

    const detail::ReflectedSourceDescriptor descriptor{
        .physicalSourceId = sourceDescriptor.physicalSourceId.getRawValue(),
        .logicalSource = sourceDescriptor.logicalSource,
        .type = sourceDescriptor.sourceType,
        .host = sourceDescriptor.host,
        .maxInflightBuffers = sourceDescriptor.maxInflightBuffers,
        .inputFormatterDescriptor = sourceDescriptor.inputFormatterDescriptor,
        .config = configEntry->reflect(sourceDescriptor.pluginData)};

    return reflect(descriptor);
}

SourceDescriptor Unreflector<SourceDescriptor>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflectedSourceDescriptor = context.unreflect<detail::ReflectedSourceDescriptor>(rfl);

    const auto* configEntry = SourceConfigRegistry::instance().find(reflectedSourceDescriptor.type);
    if (configEntry == nullptr)
    {
        throw UnknownSourceType(
            "Cannot deserialize source descriptor: source type {} has no SourceConfigRegistry entry", reflectedSourceDescriptor.type);
    }

    return SourceDescriptor{
        PhysicalSourceId{reflectedSourceDescriptor.physicalSourceId},
        LogicalSource{std::move(reflectedSourceDescriptor.logicalSource)},
        reflectedSourceDescriptor.type,
        reflectedSourceDescriptor.host,
        reflectedSourceDescriptor.maxInflightBuffers,
        configEntry->unreflect(reflectedSourceDescriptor.config, context),
        reflectedSourceDescriptor.inputFormatterDescriptor};
}
}
