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

#include <Traits/OriginMappingTrait.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <ranges>
#include <string>
#include <string_view>
#include <typeinfo>
#include <utility>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>

namespace NES
{

OriginMappingTrait::OriginMappingTrait(std::vector<std::pair<OriginId, OriginId>> originMapping) : originMapping(std::move(originMapping))
{
}

const std::type_info& OriginMappingTrait::getType() const
{
    return typeid(OriginMappingTrait);
}

bool OriginMappingTrait::operator==(const OriginMappingTrait& other) const
{
    return originMapping == other.originMapping;
}

size_t OriginMappingTrait::hash() const
{
    /// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    size_t seed = 0;
    const auto combine = [&seed](const uint64_t value) { seed ^= std::hash<uint64_t>{}(value) + 0x9e3779b9 + (seed << 6) + (seed >> 2); };
    for (const auto& [upstreamOriginId, branchOriginId] : originMapping)
    {
        combine(upstreamOriginId.getRawValue());
        combine(branchOriginId.getRawValue());
    }
    return seed;
    /// NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
}

std::string OriginMappingTrait::explain(ExplainVerbosity) const
{
    return fmt::format(
        "OriginMappingTrait: [{}]",
        fmt::join(
            originMapping | std::views::transform([](const auto& pair) { return fmt::format("{}->{}", pair.first, pair.second); }), ", "));
}

std::string_view OriginMappingTrait::getName() const
{
    return NAME;
}

Reflected Reflector<OriginMappingTrait>::operator()(const OriginMappingTrait& trait, const ReflectionContext& context) const
{
    std::vector<uint64_t> upstreamOriginIds;
    std::vector<uint64_t> branchOriginIds;
    upstreamOriginIds.reserve(trait.originMapping.size());
    branchOriginIds.reserve(trait.originMapping.size());
    for (const auto& [upstreamOriginId, branchOriginId] : trait.originMapping)
    {
        upstreamOriginIds.push_back(upstreamOriginId.getRawValue());
        branchOriginIds.push_back(branchOriginId.getRawValue());
    }
    return context.reflect(detail::ReflectedOriginMappingTrait{
        .upstreamOriginIds = std::move(upstreamOriginIds), .branchOriginIds = std::move(branchOriginIds)});
}

OriginMappingTrait Unreflector<OriginMappingTrait>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [upstreamOriginIds, branchOriginIds] = context.unreflect<detail::ReflectedOriginMappingTrait>(reflected);
    INVARIANT(
        upstreamOriginIds.size() == branchOriginIds.size(),
        "An origin mapping needs one branch origin id per upstream origin id, but got {} and {}",
        upstreamOriginIds.size(),
        branchOriginIds.size());

    std::vector<std::pair<OriginId, OriginId>> originMapping;
    originMapping.reserve(upstreamOriginIds.size());
    for (size_t index = 0; index < upstreamOriginIds.size(); ++index)
    {
        originMapping.emplace_back(OriginId{upstreamOriginIds.at(index)}, OriginId{branchOriginIds.at(index)});
    }
    return OriginMappingTrait{std::move(originMapping)};
}
}
