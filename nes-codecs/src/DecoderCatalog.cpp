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

#include <Codecs/DecoderCatalog.hpp>

#include <algorithm>
#include <expected>
#include <mutex>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <Codecs/DecoderDescriptor.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Configurations/ConfigResolution.hpp>
#include <Configurations/InstantiatedConfigValue.hpp>
#include <Identifiers/Identifier.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <DecoderConfigRegistry.hpp>
#include <DecoderConfigSchemaRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

std::expected<DecoderDescriptor, Exception>
DecoderCatalog::addDecoder(Identifier name, const Identifier& type, const std::unordered_map<Identifier, std::string>& options)
{
    const auto configSchema = DecoderConfigSchemaRegistry::instance().getSchema(type.asCanonicalString());
    const auto configEntry = DecoderConfigRegistry::instance().find(type.asCanonicalString());
    if (not configSchema.has_value() or not configEntry.has_value())
    {
        auto registered = DecoderConfigSchemaRegistry::instance().getRegisteredNames();
        std::ranges::sort(registered);
        return std::unexpected{UnknownDecoderType(
            "No decoder is registered for the codec '{}'. Registered codecs: {}. If it is a plugin, make sure you activated it.",
            type,
            registered.empty() ? std::string{"none"} : fmt::format("{}", fmt::join(registered, ", ")))};
    }

    std::vector<LiteralConfigValue> literals;
    literals.reserve(options.size());
    for (const auto& [key, value] : options)
    {
        auto literal = parseConfigLiteral(value);
        if (not literal.has_value())
        {
            return std::unexpected{std::move(literal).error()};
        }
        literals.emplace_back(QualifiedIdentifier::create(key), std::move(literal).value());
    }
    auto [resolved, errors] = resolveConfig(createConfigLiteralSchema(std::move(literals)), *configSchema);
    if (not errors.empty())
    {
        return std::unexpected{InvalidConfigParameter("Invalid options for decoder {} of codec {}: {}", name, type, errors)};
    }
    auto config = configEntry->instantiate(InstantiatedConfig{resolved});
    if (not config.has_value())
    {
        return std::unexpected{std::move(config).error()};
    }

    const std::scoped_lock lock(mutex);
    if (decoders.contains(name))
    {
        return std::unexpected{DecoderAlreadyExists("A decoder named {} already exists", name)};
    }
    DecoderDescriptor descriptor{name, type, std::move(config).value()};
    decoders.emplace(std::move(name), descriptor);
    return descriptor;
}

std::optional<DecoderDescriptor> DecoderCatalog::getDecoder(const Identifier& name) const
{
    const std::scoped_lock lock(mutex);
    if (const auto iter = decoders.find(name); iter != decoders.end())
    {
        return iter->second;
    }
    return std::nullopt;
}

std::optional<DecoderDescriptor> DecoderCatalog::removeDecoder(const Identifier& name)
{
    const std::scoped_lock lock(mutex);
    auto node = decoders.extract(name);
    if (node.empty())
    {
        return std::nullopt;
    }
    return std::move(node.mapped());
}

std::vector<DecoderDescriptor> DecoderCatalog::getAllDecoders() const
{
    const std::scoped_lock lock(mutex);
    auto all = decoders | std::views::values | std::ranges::to<std::vector>();
    std::ranges::sort(all, {}, [](const DecoderDescriptor& descriptor) { return descriptor.getName().asCanonicalString(); });
    return all;
}

}
