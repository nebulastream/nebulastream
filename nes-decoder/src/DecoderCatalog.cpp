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

#include <Decoders/DecoderCatalog.hpp>

#include <expected>
#include <mutex>
#include <optional>
#include <ostream>
#include <ranges>
#include <vector>

#include <Configurations/ConfigLiteral.hpp>
#include <Decoders/DecoderChain.hpp>
#include <Identifiers/Identifier.hpp>
#include <Schema/Schema.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

std::ostream& operator<<(std::ostream& out, const NamedDecoder& decoder)
{
    return out << fmt::format(
               "NamedDecoder(name: {}, stages: [{}])",
               decoder.name,
               fmt::join(decoder.stages | std::views::transform([](const auto& stage) { return stage.getStageType(); }), ", "));
}

std::expected<NamedDecoder, Exception> DecoderCatalog::addDecoder(
    const Identifier& name, const std::vector<Identifier>& stageTypes, const Schema<LiteralConfigValue, Ordered>& options)
{
    auto stages = DecoderChain::createStages(stageTypes, options);
    if (not stages.has_value())
    {
        return std::unexpected{stages.error()};
    }
    const std::scoped_lock lock(mutex);
    auto [iter, inserted] = decoders.try_emplace(name, NamedDecoder{.name = name, .stages = std::move(stages).value()});
    if (not inserted)
    {
        return std::unexpected{DecoderAlreadyExists("A decoder named '{}' already exists", name)};
    }
    return iter->second;
}

std::optional<NamedDecoder> DecoderCatalog::getDecoder(const Identifier& name) const
{
    const std::scoped_lock lock(mutex);
    if (const auto iter = decoders.find(name); iter != decoders.end())
    {
        return iter->second;
    }
    return std::nullopt;
}

std::vector<NamedDecoder> DecoderCatalog::getAllDecoders() const
{
    const std::scoped_lock lock(mutex);
    return decoders | std::views::values | std::ranges::to<std::vector>();
}

bool DecoderCatalog::removeDecoder(const Identifier& name)
{
    const std::scoped_lock lock(mutex);
    return decoders.erase(name) > 0;
}

}
