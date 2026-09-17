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

#include <Sinks/SinkCatalog.hpp>

#include <algorithm>
#include <cctype>
#include <expected>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{

std::expected<SinkDescriptor, Exception> SinkCatalog::addSinkDescriptor(
    Identifier sinkName,
    const Schema<UnqualifiedUnboundField, Ordered>& schema,
    const Identifier& sinkType,
    Host host,
    std::unordered_map<Identifier, std::string> config,
    const std::unordered_map<Identifier, std::string>& formatConfig)
{
    if (std::ranges::all_of(fmt::format("{}", sinkName), [](const char character) { return std::isdigit(character); }))
    {
        return std::unexpected{InvalidConfigParameter("Sink name '{}' is invalid: only-digit names are reserved", sinkName)};
    }

    auto sinkDescriptor = SinkDescriptor::createNamed(
        SinkId{nextSinkId.fetch_add(1)}, sinkName, sinkType, schema, std::move(host), std::move(config), formatConfig);
    if (not sinkDescriptor.has_value())
    {
        return std::unexpected{InvalidConfigParameter("Invalid configuration for sink '{}' of type '{}'", sinkName, sinkType)};
    }

    const auto lockedSinks = sinks.wlock();
    /// TODO #1504: duplicate sinks are not registered
    lockedSinks->emplace(std::move(sinkName), *sinkDescriptor);
    return *sinkDescriptor;
}

std::optional<SinkDescriptor> SinkCatalog::getSinkDescriptor(const Identifier& sinkName) const
{
    const auto lockedSinks = sinks.rlock();
    const auto sinkDescriptorOpt = lockedSinks->find(sinkName);
    if (sinkDescriptorOpt == lockedSinks->end())
    {
        return std::nullopt;
    }
    return sinkDescriptorOpt->second;
}

std::optional<SinkDescriptor> SinkCatalog::getAnonymousSink(
    const std::optional<Schema<UnqualifiedUnboundField, Ordered>>& schema,
    const Identifier& sinkType,
    Host host,
    std::unordered_map<Identifier, std::string> config,
    const std::unordered_map<Identifier, std::string>& formatConfig) const
{
    return SinkDescriptor::createAnonymous(
        SinkId{nextSinkId.fetch_add(1)}, sinkType, schema, std::move(host), std::move(config), formatConfig);
}

bool SinkCatalog::removeSinkDescriptor(const Identifier& sinkName)
{
    const auto lockedSinks = sinks.wlock();
    return lockedSinks->erase(sinkName) == 1;
}

bool SinkCatalog::removeSinkDescriptor(const SinkDescriptor& sinkDescriptor)
{
    const auto lockedSinks = sinks.wlock();
    return lockedSinks->erase(sinkDescriptor.getSinkName()) == 1;
}

bool SinkCatalog::containsSinkDescriptor(const Identifier& sinkName) const
{
    const auto lockedSinks = sinks.rlock();
    return lockedSinks->contains(sinkName);
}

bool SinkCatalog::containsSinkDescriptor(const SinkDescriptor& sinkDescriptor) const
{
    const auto lockedSinks = sinks.rlock();
    return lockedSinks->contains(sinkDescriptor.getSinkName());
}

std::vector<SinkDescriptor> SinkCatalog::getAllSinkDescriptors() const
{
    const auto lockedSinks = sinks.rlock();
    return *lockedSinks | std::ranges::views::transform([](const auto& sinkDescriptor) { return sinkDescriptor.second; })
        | std::ranges::to<std::vector>();
}
}
