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

#include <OperatorGraph.hpp>

#include <algorithm>
#include <ranges>
#include <set>
#include <unordered_set>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

OperatorGraph::OperatorGraph(const LogicalPlan& plan)
{
    std::vector<LogicalOperator> toVisit = plan.getRootOperators();
    while (not toVisit.empty())
    {
        auto current = toVisit.back();
        toVisit.pop_back();
        if (nodes.contains(current.getId()))
        {
            continue;
        }
        auto& node = nodes.try_emplace(current.getId(), Node{.op = current, .upstream = {}, .downstream = {}}).first->second;
        for (const auto& child : current.getChildren())
        {
            node.upstream.emplace_back(child.getId());
            toVisit.emplace_back(child);
        }
    }
    for (const auto& [id, node] : nodes)
    {
        for (const auto& upstream : node.upstream)
        {
            auto& downstream = nodes.at(upstream).downstream;
            if (not std::ranges::contains(downstream, id))
            {
                downstream.emplace_back(id);
            }
        }
    }
}

const OperatorGraph::Node& OperatorGraph::getNode(const OperatorId id) const
{
    const auto it = nodes.find(id);
    PRECONDITION(it != nodes.end(), "Operator {} is not part of the operator graph", id);
    return it->second;
}

bool OperatorGraph::contains(const OperatorId id) const
{
    return nodes.contains(id);
}

const LogicalOperator& OperatorGraph::getOperator(const OperatorId id) const
{
    return getNode(id).op;
}

const std::vector<OperatorId>& OperatorGraph::getUpstreamOperators(const OperatorId id) const
{
    return getNode(id).upstream;
}

const std::vector<OperatorId>& OperatorGraph::getDownstreamOperators(const OperatorId id) const
{
    return getNode(id).downstream;
}

std::vector<OperatorId> OperatorGraph::getRootOperators() const
{
    return nodes | std::views::filter([](const auto& entry) { return entry.second.downstream.empty(); }) | std::views::keys
        | std::ranges::to<std::vector>();
}

std::vector<OperatorId> OperatorGraph::getLeafOperators() const
{
    return nodes | std::views::filter([](const auto& entry) { return entry.second.upstream.empty(); }) | std::views::keys
        | std::ranges::to<std::vector>();
}

std::vector<OperatorId> OperatorGraph::getAllOperators() const
{
    return nodes | std::views::keys | std::ranges::to<std::vector>();
}

bool OperatorGraph::isDownstreamOf(const OperatorId candidate, const OperatorId id) const
{
    std::unordered_set<OperatorId> visited;
    std::vector<OperatorId> toVisit = getDownstreamOperators(id);
    while (not toVisit.empty())
    {
        const auto current = toVisit.back();
        toVisit.pop_back();
        if (current == candidate)
        {
            return true;
        }
        if (visited.insert(current).second)
        {
            std::ranges::copy(getDownstreamOperators(current), std::back_inserter(toVisit));
        }
    }
    return false;
}

std::set<OperatorId>
OperatorGraph::findAllOperatorsBetween(const std::set<OperatorId>& downstreamOperators, const std::set<OperatorId>& upstreamOperators) const
{
    /// An operator is between the two sets if it is reachable upstream from a downstream operator and downstream from an upstream operator.
    std::set<OperatorId> operatorsBetween;
    for (const auto& upstream : upstreamOperators)
    {
        for (const auto& downstream : downstreamOperators)
        {
            if (not isDownstreamOf(downstream, upstream))
            {
                continue;
            }
            /// Collect every operator downstream of `upstream` from which `downstream` can be reached.
            std::vector<OperatorId> toVisit = getDownstreamOperators(upstream);
            std::unordered_set<OperatorId> visited;
            while (not toVisit.empty())
            {
                const auto current = toVisit.back();
                toVisit.pop_back();
                if (current == downstream or not visited.insert(current).second)
                {
                    continue;
                }
                if (isDownstreamOf(downstream, current))
                {
                    operatorsBetween.insert(current);
                    std::ranges::copy(getDownstreamOperators(current), std::back_inserter(toVisit));
                }
            }
        }
    }
    for (const auto& id : upstreamOperators)
    {
        operatorsBetween.erase(id);
    }
    for (const auto& id : downstreamOperators)
    {
        operatorsBetween.erase(id);
    }
    return operatorsBetween;
}

}
