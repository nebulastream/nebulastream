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

#include <BridgeConfig.hpp>

#include <algorithm>
#include <array>
#include <charconv>
#include <iterator>
#include <ranges>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <rfl/json/read.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>

namespace NES::Bridge
{
namespace
{

/// A double is written in fixed notation and always with a decimal point, so that it is read back as a double.
std::string renderLiteral(const ConfigLiteral& literal)
{
    if (const auto* value = std::get_if<double>(&literal))
    {
        std::array<char, 512> buffer{};
        const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), *value, std::chars_format::fixed);
        INVARIANT(error == std::errc{}, "a double fits the buffer in fixed notation");
        std::string rendered{buffer.data(), end};
        return rendered.contains('.') ? rendered : rendered + ".0";
    }
    return fmt::format("{}", literal);
}

}

SplitConfig splitOptimizerConfig(const Schema<LiteralConfigValue, Ordered>& literals)
{
    std::vector<LiteralConfigValue> optimizer;
    std::vector<LiteralConfigValue> worker;
    std::ranges::partition_copy(
        literals,
        std::back_inserter(optimizer),
        std::back_inserter(worker),
        [](const LiteralConfigValue& literal) { return *literal.getFullyQualifiedName().begin() == Identifier::parse("optimizer"); });
    return {.optimizer = createConfigLiteralSchema(std::move(optimizer)), .worker = createConfigLiteralSchema(std::move(worker))};
}

Schema<LiteralConfigValue, Ordered> literalsFromJson(const std::string& json)
{
    if (json.empty())
    {
        return {};
    }
    auto parsed = rfl::json::read<std::unordered_map<std::string, std::string>>(json);
    if (!parsed)
    {
        throw CannotDeserialize("Failed to deserialize config from JSON: {}", parsed.error().what());
    }
    /// The command-line parser already handles dotted option names, so each entry is rewritten as `--name=value`.
    std::vector<std::string> arguments;
    arguments.reserve(parsed->size());
    for (const auto& [key, value] : *parsed)
    {
        arguments.push_back(fmt::format("--{}={}", key, value));
    }
    return parseCommandLineConfig(arguments);
}

std::string literalsToJson(const Schema<LiteralConfigValue, Ordered>& literals)
{
    std::unordered_map<std::string, std::string> settings;
    for (const auto& literal : literals)
    {
        settings.emplace(fmt::format("{}", literal.getFullyQualifiedName()), renderLiteral(literal.getValue()));
    }
    return rfl::json::write(settings);
}

std::string configOption(const LiteralConfigValue& literal)
{
    const auto path = literal.getFullyQualifiedName()
        | std::views::transform([](const Identifier& part) { return fmt::format(R"("{}")", part.getOriginalString()); });
    return fmt::format("'{}' AS {}", replaceAll(renderLiteral(literal.getValue()), "'", "''"), fmt::join(path, "."));
}

}
