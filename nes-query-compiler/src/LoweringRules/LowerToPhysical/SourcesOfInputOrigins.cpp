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

#include <LoweringRules/LowerToPhysical/SourcesOfInputOrigins.hpp>

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Iterators/BFSIterator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
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
            /// Must be the origin id the source is lowered with, as it identifies the source's backpressure channel.
            /// For a network source, this is the channel of the local network source, so throttling stops at this worker: the upstream
            /// producer keeps running until the network channel's bounded queue fills up.
            const auto& outputOrigins = *op.getTraitSet().get<OutputOriginIdsTrait>();
            INVARIANT(outputOrigins.size() == 1, "A source must have exactly one output origin, but has {}", outputOrigins.size());
            sourceOrigins.insert(outputOrigins[0]);
        }
    }
    return {sourceOrigins.begin(), sourceOrigins.end()};
}
}

SourcesOfInputOrigins getSourcesOfInputOrigins(const std::vector<LogicalOperator>& children)
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

}
