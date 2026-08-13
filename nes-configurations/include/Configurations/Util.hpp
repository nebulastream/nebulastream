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

#pragma once
#include <expected>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <unordered_map>
#include <utility>

#include <Configurations/ConfigField.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

#include <Configurations/ConfigResolution.hpp>
#include <Configurations/ConfigValue.hpp>

namespace NES
{

/// Print `--key: description (Default: x)` for every declared field, keys lowercased as the user
/// would type them on the command line.
inline void generateHelp(std::ostream& ostream, const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig)
{
    for (const auto& field : declaredConfig)
    {
        ostream << fmt::format("--{}: {}", toLowerCase(fmt::format("{}", field.getFullyQualifiedName())), field.getDescription());
        if (const auto& defaultDescription = field.getDefaultDescription())
        {
            ostream << fmt::format(" (Default: {})", *defaultDescription);
        }
        ostream << '\n';
    }
}

/// Render the effective configuration: every declared field with the value it resolves to — the
/// passed literal where one was passed, the declared default otherwise. Used for the startup
/// config dump of workers.
inline std::string formatEffectiveConfig(
    const Schema<LiteralConfigValue, Ordered>& passedConfig, const Schema<QualifiedErasedConfigField, Ordered>& declaredConfig)
{
    /// Exact-name lookup: Schema's own name lookup is suffix-addressable (see mergeConfigLiterals).
    const auto passedByName = passedConfig
        | std::views::transform([](const auto& value) { return std::pair{value.getFullyQualifiedName(), value}; })
        | std::ranges::to<std::unordered_map>();

    std::string rendered;
    for (const auto& field : declaredConfig)
    {
        const auto name = toLowerCase(fmt::format("{}", field.getFullyQualifiedName()));
        const auto passed = passedByName.find(field.getFullyQualifiedName());
        rendered += fmt::format(
            "  {}: {}\n",
            name,
            passed != passedByName.end() ? passed->second.getValue() : field.getDefaultDescription().value_or("<unset>"));
    }
    return rendered;
}

/// Resolve the passed literals against T's declared config schema (fully qualified only) and
/// instantiate the typed configuration struct.
template <typename T>
std::expected<T, InvalidConfigSpecification> resolveConfiguration(const Schema<LiteralConfigValue, Ordered>& passedConfig)
{
    return toExpected(resolveConfigFullyQualified(passedConfig, T::getConfigSchema()))
        .transform([](Schema<ConfigValue, Ordered> resolved) { return T::fromConfig(InstantiatedConfig{std::move(resolved)}); });
}

/// The configuration with every field at its declared default, produced through the ordinary
/// resolution path (an empty literal schema). This is the ONLY way to obtain a "default" config:
/// the structs carry no member initializers, so a default-constructed instance is uninitialized.
template <typename T>
T defaultConfiguration()
{
    return resolveConfiguration<T>(Schema<LiteralConfigValue, Ordered>{}).value();
}
}
