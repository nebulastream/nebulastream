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

#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeReflection.hpp> /// NOLINT(misc-include-cleaner)
#include <Sources/LogicalSource.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <InputFormatterDescriptor.hpp>

namespace NES
{

SourceDescriptor::SourceDescriptor(
    const PhysicalSourceId physicalSourceId,
    LogicalSource logicalSource,
    std::string_view sourceType,
    Host host,
    DescriptorConfig::Config config,
    const InputFormatterDescriptor& inputFormatterDescriptor)
    : Descriptor(std::move(config))
    , physicalSourceId(physicalSourceId)
    , logicalSource(std::move(logicalSource))
    , sourceType(std::move(sourceType))
    , host(std::move(host))
    , inputFormatterDescriptor(inputFormatterDescriptor)
{
}

LogicalSource SourceDescriptor::getLogicalSource() const
{
    return logicalSource;
}

std::string SourceDescriptor::getSourceType() const
{
    return sourceType;
}

InputFormatterDescriptor SourceDescriptor::getInputFormatterDescriptor() const
{
    return inputFormatterDescriptor;
}

Host SourceDescriptor::getHost() const
{
    return host;
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
    const detail::ReflectedSourceDescriptor descriptor{
        .physicalSourceId = sourceDescriptor.physicalSourceId.getRawValue(),
        .logicalSource = sourceDescriptor.logicalSource,
        .type = sourceDescriptor.sourceType,
        .host = sourceDescriptor.host,
        .inputFormatterDescriptor = sourceDescriptor.inputFormatterDescriptor,
        .config = sourceDescriptor.getReflectedConfig()};

    return reflect(descriptor);
}

SourceDescriptor Unreflector<SourceDescriptor>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto reflectedSourceDescriptor = context.unreflect<detail::ReflectedSourceDescriptor>(rfl);

    return SourceDescriptor{
        PhysicalSourceId{reflectedSourceDescriptor.physicalSourceId},
        LogicalSource{std::move(reflectedSourceDescriptor.logicalSource)},
        reflectedSourceDescriptor.type,
        reflectedSourceDescriptor.host,
        Descriptor::unreflectConfig(reflectedSourceDescriptor.config, context),
        reflectedSourceDescriptor.inputFormatterDescriptor};
}
}
