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
#include <variant>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Util/Overloaded.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <magic_enum/magic_enum.hpp>

#include <Util/Logger/Logger.hpp>
#include <Util/Reflection.hpp>
#include <ErrorHandling.hpp>
#include <ProtobufHelper.hpp> /// NOLINT
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

Reflected Reflector<SinkDescriptor>::operator()(const SinkDescriptor& descriptor) const
{
    return reflect(detail::ReflectedSinkDescriptor{
        .sinkName = descriptor.sinkName,
        .schema = *descriptor.schema,
        .sinkType = descriptor.sinkType,
        .config = descriptor.getReflectedConfig(),
    });
}

SinkDescriptor Unreflector<SinkDescriptor>::operator()(const Reflected& reflected) const
{
    auto [name, schema, type, config] = unreflect<detail::ReflectedSinkDescriptor>(reflected);
    return SinkDescriptor{name, schema, type, Descriptor::unreflectConfig(config)};
}

}
