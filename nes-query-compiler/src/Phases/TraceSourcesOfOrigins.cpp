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

#include <Phases/TraceSourcesOfOrigins.hpp>

#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Operators/OriginIdAssigner.hpp>
#include <Operators/Sources/SourceDescriptorLogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Traits/OutputOriginIdsTrait.hpp>
#include <BackpressureManager.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
class SourcesOfOriginsTracer
{
public:
    /// Returns the sources at or below `op`.
    const std::unordered_set<OriginId>& visit(const LogicalOperator& op)
    {
        if (const auto visited = sourcesBelow.find(op); visited != sourcesBelow.end())
        {
            return visited->second;
        }

        std::unordered_set<OriginId> sources;
        for (const auto& child : op.getChildren())
        {
            const auto& childSources = visit(child);
            sources.insert(childSources.begin(), childSources.end());
        }

        const auto outputOrigins = op.getTraitSet().get<OutputOriginIdsTrait>();
        if (op.tryGetAs<SourceDescriptorLogicalOperator>().has_value())
        {
            INVARIANT(outputOrigins->size() > 0, "A source must have at least one output origin");
            sources = {*outputOrigins->begin()};
        }
        if (op.tryGetAs<OriginIdAssigner>().has_value())
        {
            for (const auto origin : *outputOrigins)
            {
                auto& sourcesOfOrigin = result[origin];
                sourcesOfOrigin.insert(sourcesOfOrigin.end(), sources.begin(), sources.end());
            }
        }
        /// References into an unordered_map stay valid when it grows.
        return sourcesBelow.emplace(op, std::move(sources)).first->second;
    }

    SourcesOfOrigins result;

private:
    std::unordered_map<LogicalOperator, std::unordered_set<OriginId>> sourcesBelow;
};
}

InputsOfOrigins traceInputsOfOrigins(const LogicalPlan& plan)
{
    InputsOfOrigins result;
    std::unordered_set<LogicalOperator> visited;
    std::vector<LogicalOperator> pending = plan.getRootOperators();
    while (!pending.empty())
    {
        const auto op = pending.back();
        pending.pop_back();
        if (!visited.insert(op).second)
        {
            continue;
        }
        const auto children = op.getChildren();
        pending.insert(pending.end(), children.begin(), children.end());
        if (!op.tryGetAs<OriginIdAssigner>().has_value() || op.tryGetAs<SourceDescriptorLogicalOperator>().has_value())
        {
            continue;
        }
        std::vector<OriginId> inputs;
        for (const auto& child : children)
        {
            const auto childOrigins = child.getTraitSet().get<OutputOriginIdsTrait>();
            inputs.insert(inputs.end(), childOrigins->begin(), childOrigins->end());
        }
        for (const auto origin : *op.getTraitSet().get<OutputOriginIdsTrait>())
        {
            result[origin] = inputs;
        }
    }
    return result;
}

SourcesOfOrigins traceSourcesOfOrigins(const LogicalPlan& plan)
{
    SourcesOfOriginsTracer tracer;
    for (const auto& root : plan.getRootOperators())
    {
        tracer.visit(root);
    }
    return std::move(tracer.result);
}

}
