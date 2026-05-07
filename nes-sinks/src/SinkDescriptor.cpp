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
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeReflection.hpp> /// NOLINT(misc-include-cleaner)
#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <ProtobufHelper.hpp> /// NOLINT
#include <SinkValidationRegistry.hpp>

namespace NES
{

SinkDescriptor::SinkDescriptor(
    SinkId id,
    std::string name,
    const Schema& schema,
    const std::string_view sinkType,
    Host host,
    const std::unordered_map<std::string, std::string>& formatConfig,
    DescriptorConfig::Config config)
    : Descriptor(std::move(config))
    , id(id)
    , name(std::move(name))
    , schema(std::make_shared<Schema>(schema))
    , sinkType(sinkType)
    , host(std::move(host))
    , formatConfig(formatConfig)
{
}

std::shared_ptr<const Schema> SinkDescriptor::getSchema() const
{
    return schema;
}

std::string SinkDescriptor::getFormatType() const
{
    try
    {
        return getFromConfig(OUTPUT_FORMAT);
    }
    catch (std::out_of_range& e)
    {
        /// If no output format is set, then the format will be native
        return "Native";
    }
}

std::string SinkDescriptor::getSinkType() const
{
    return sinkType;
}

SinkId SinkDescriptor::getSinkId() const
{
    return id;
}

std::string SinkDescriptor::getSinkName() const
{
    return name;
}

std::unordered_map<std::string, std::string> SinkDescriptor::getOutputFormatterConfig() const
{
    return formatConfig;
}

Host SinkDescriptor::getHost() const
{
    return host;
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
        "SinkDescriptor: (id: {}, name: {}, type: {}, host: {}, Config: {}, FormatConfig: {{{}}})",
        sinkDescriptor.id,
        sinkDescriptor.name,
        sinkDescriptor.sinkType,
        sinkDescriptor.host,
        sinkDescriptor.toStringConfig(),
        fmt::join(sinkDescriptor.formatConfig, ","));
    return out;
}

bool operator==(const SinkDescriptor& lhs, const SinkDescriptor& rhs)
{
    return lhs.id == rhs.id;
}

Reflected Reflector<SinkDescriptor>::operator()(const SinkDescriptor& descriptor) const
{
    return reflect(detail::ReflectedSinkDescriptor{
        .id = descriptor.id.getRawValue(),
        .name = descriptor.name,
        .schema = *descriptor.schema,
        .sinkType = descriptor.sinkType,
        .host = descriptor.host,
        .formatConfig = descriptor.formatConfig,
        .config = descriptor.getReflectedConfig(),
    });
}

SinkDescriptor Unreflector<SinkDescriptor>::operator()(const Reflected& reflected) const
{
    auto [id, name, schema, type, host, formatConfig, config] = unreflect<detail::ReflectedSinkDescriptor>(reflected);
    return SinkDescriptor{SinkId{id}, std::move(name), schema, type, host, formatConfig, Descriptor::unreflectConfig(config)};
}

}
