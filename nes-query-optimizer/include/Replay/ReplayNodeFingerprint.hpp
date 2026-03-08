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

#include <cstdint>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <Operators/LogicalOperator.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/StoreLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryId.hpp>
#include <Traits/PlacementTrait.hpp>
#include <Traits/TraitSet.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

namespace NES
{
namespace Replay
{
namespace detail
{
constexpr uint64_t FNV_OFFSET_BASIS = 14695981039346656037ULL;
constexpr uint64_t FNV_PRIME = 1099511628211ULL;

inline uint64_t fnv1a64(std::string_view input)
{
    uint64_t hash = FNV_OFFSET_BASIS;
    for (const auto character : input)
    {
        hash ^= static_cast<uint8_t>(character);
        hash *= FNV_PRIME;
    }
    return hash;
}

inline LogicalOperator normalizeReplayNodeFingerprintOperator(const LogicalOperator& current)
{
    if (current.tryGetAs<StoreLogicalOperator>().has_value())
    {
        const auto children = current.getChildren();
        PRECONDITION(children.size() == 1, "Replay normalization expected store operators to have a single child");
        return normalizeReplayNodeFingerprintOperator(children.front());
    }

    auto normalizedChildren
        = current.getChildren() | std::views::transform(normalizeReplayNodeFingerprintOperator) | std::ranges::to<std::vector>();
    return current.withChildren(std::move(normalizedChildren));
}
}

inline std::string createStructuralReplayNodeFingerprint(const LogicalOperator& logicalOperator)
{
    const auto normalizedOperator = detail::normalizeReplayNodeFingerprintOperator(logicalOperator);
    const auto placementTrait = getTrait<PlacementTrait>(normalizedOperator.getTraitSet());
    if (!placementTrait.has_value())
    {
        return {};
    }

    const auto canonical = normalizedOperator.tryGetAs<SinkLogicalOperator>().has_value()
        ? fmt::format(
            "placement={}|plan={}",
            placementTrait.value()->onNode,
            explain(LogicalPlan(INVALID_QUERY_ID, {normalizedOperator}), ExplainVerbosity::Short))
        : fmt::format(
            "placement={}|schema={}|plan={}",
            placementTrait.value()->onNode,
            normalizedOperator.getOutputSchema(),
            explain(LogicalPlan(INVALID_QUERY_ID, {normalizedOperator}), ExplainVerbosity::Short));
    return fmt::format("{:016x}", detail::fnv1a64(canonical));
}
}
}
