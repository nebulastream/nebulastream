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

#include <LoweringRules/LowerToPhysical/TraceSourcesOfInputOrigins.hpp>

#include <algorithm>
#include <optional>
#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Operators/Windows/JoinLogicalOperator.hpp>
#include <Operators/Windows/WindowedAggregationLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <Watermark/WatermarkGapThrottle.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
std::optional<LogicalOperator> findProducer(const LogicalOperator& child, const OriginId origin)
{
    /// Keyed by operator id, as the BFS reaches an operator below a diamond once per path.
    std::unordered_map<OperatorId, LogicalOperator> producers;
    for (const auto& op : BFSRange<LogicalOperator>(child))
    {
        if (op.tryGetAs<OriginIdAssigner>().has_value() && std::ranges::contains(*op.getTraitSet().get<OutputOriginIdsTrait>(), origin))
        {
            producers.emplace(op.getId(), op);
        }
    }
    INVARIANT(producers.size() <= 1, "{} operators below the window assign the origin {}", producers.size(), origin);
    if (producers.empty())
    {
        return std::nullopt;
    }
    return producers.begin()->second;
}

std::vector<OriginId> getSourceOrigins(const LogicalOperator& producer)
{
    std::unordered_set<OriginId> sourceOrigins;
    for (const auto& op : BFSRange<LogicalOperator>(producer))
    {
        if (op.tryGetAs<SourceDescriptorLogicalOperator>().has_value())
        {
            /// Must be the origin id the source is lowered with, as it identifies the source's backpressure channel. LowerToPhysicalSource
            /// lowers a source with its first output origin; a network source carries all origins of its upstream plan.
            const auto& outputOrigins = *op.getTraitSet().get<OutputOriginIdsTrait>();
            INVARIANT(outputOrigins.size() > 0, "A source must have at least one output origin");
            sourceOrigins.insert(outputOrigins[0]);
        }
    }
    return {sourceOrigins.begin(), sourceOrigins.end()};
}

bool isWindow(const LogicalOperator& op)
{
    return op.tryGetAs<JoinLogicalOperator>().has_value() || op.tryGetAs<WindowedAggregationLogicalOperator>().has_value();
}

bool isBelow(const LogicalOperator& upper, const OperatorId lower)
{
    return std::ranges::any_of(BFSRange<LogicalOperator>(upper), [lower](const auto& op) { return op.getId() == lower; });
}
}

SourcesOfInputOrigins traceSourcesOfInputOrigins(const std::vector<LogicalOperator>& children)
{
    SourcesOfInputOrigins sourcesOfInputOrigins;
    for (const auto& child : children)
    {
        for (const auto inputOrigin : *child.getTraitSet().get<OutputOriginIdsTrait>())
        {
            const auto producer = findProducer(child, inputOrigin);
            INVARIANT(producer.has_value(), "No operator below the window produces its input origin {}", inputOrigin);
            sourcesOfInputOrigins[inputOrigin] = getSourceOrigins(*producer);
        }
    }
    return sourcesOfInputOrigins;
}

std::unordered_set<OperatorId> findWindowsSharingSourcesWithSiblings(const LogicalPlan& plan)
{
    /// Only windows with several input origins throttle, and each may throttle any source below it.
    std::unordered_map<OperatorId, std::pair<LogicalOperator, std::unordered_set<OriginId>>> throttlingWindows;
    for (const auto& root : plan.getRootOperators())
    {
        for (const auto& op : BFSRange<LogicalOperator>(root))
        {
            if (!isWindow(op) || throttlingWindows.contains(op.getId()))
            {
                continue;
            }
            if (const auto sourcesOfInputOrigins = traceSourcesOfInputOrigins(op.getChildren()); sourcesOfInputOrigins.size() > 1)
            {
                auto sources = sourcesOfInputOrigins | std::views::values | std::views::join | std::ranges::to<std::unordered_set>();
                throttlingWindows.emplace(op.getId(), std::pair{op, std::move(sources)});
            }
        }
    }

    std::unordered_set<OperatorId> windowsSharingSources;
    for (const auto& [id, window] : throttlingWindows)
    {
        for (const auto& [otherId, other] : throttlingWindows)
        {
            const bool siblings = id != otherId && !isBelow(window.first, otherId) && !isBelow(other.first, id);
            if (siblings && std::ranges::any_of(window.second, [&](const auto source) { return other.second.contains(source); }))
            {
                windowsSharingSources.insert(id);
                break;
            }
        }
    }
    return windowsSharingSources;
}

SourcesOfInputOrigins
traceThrottleableSourcesOfInputOrigins(const LogicalOperator& window, const std::unordered_set<OperatorId>& windowsWithoutThrottling)
{
    auto sourcesOfInputOrigins = traceSourcesOfInputOrigins(window.getChildren());
    if (windowsWithoutThrottling.contains(window.getId()))
    {
        for (auto& sources : sourcesOfInputOrigins | std::views::values)
        {
            sources.clear();
        }
    }
    return sourcesOfInputOrigins;
}

}
