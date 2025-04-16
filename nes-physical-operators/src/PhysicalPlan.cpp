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

#include <memory>
#include <queue>
#include <utility>
#include <Abstract/PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <SinkPhysicalOperator.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>

namespace NES
{
PhysicalPlan::PhysicalPlan(QueryId id, std::vector<std::shared_ptr<PhysicalOperatorWrapper>> rootOperators)
    : queryId(id), rootOperators(std::move(rootOperators)), direction(PlanDirection::SinkToSource)
{
    for (auto rootOperator : rootOperators)
    {
        PRECONDITION(rootOperator->physicalOperator.tryGet<SinkPhysicalOperator>(), "Expects SinkOperators as roots");
    }
}

PhysicalPlan::PhysicalPlan(QueryId id, std::vector<std::shared_ptr<PhysicalOperatorWrapper>> rootOperators, PlanDirection dir)
    : queryId(id), rootOperators(std::move(rootOperators)), direction(dir)
{
}

std::string PhysicalPlan::toString() const
{
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler<PhysicalPlan, PhysicalOperatorWrapper>(ss);
    for (const auto& rootOperator : rootOperators)
    {
        dumpHandler.dump(rootOperator);
    }
    return ss.str();
}

using PhysicalOpPtr = std::shared_ptr<PhysicalOperatorWrapper>;

PhysicalPlan flip(PhysicalPlan plan)
{
    PRECONDITION(plan.direction == PhysicalPlan::PlanDirection::SinkToSource, "Only plans with SinkToSource direction can be flipped.");
    for (auto rootOperator : plan.rootOperators)
    {
        PRECONDITION(rootOperator->physicalOperator.tryGet<SinkPhysicalOperator>(), "Expects SinkOperators as roots");
    }
    PRECONDITION(plan.rootOperators.size() == 1, "For now we can only flip graphs with a single root");

    std::unordered_set<PhysicalOperatorWrapper*> visited;
    std::vector<PhysicalOpPtr> allNodes;

    std::function<void(const PhysicalOpPtr&)> collectNodes = [&](const PhysicalOpPtr& node)
    {
        if (!node || visited.count(node.get()) != 0)
            return;
        visited.insert(node.get());
        allNodes.push_back(node);
        for (const auto& child : node->getChildren())
        {
            collectNodes(child);
        }
    };
    collectNodes(plan.rootOperators[0]);

    std::unordered_map<PhysicalOperatorWrapper*, std::vector<PhysicalOpPtr>> reversedEdges;
    for (const auto& node : allNodes)
    {
        reversedEdges[node.get()] = {};
    }
    for (const auto& node : allNodes)
    {
        for (const auto& child : node->children)
        {
            reversedEdges[child.get()].push_back(node);
        }
        node->children.clear();
    }

    std::unordered_map<PhysicalOperatorWrapper*, int> indegree;
    for (const auto& node : allNodes)
    {
        indegree[node.get()] = 0;
    }
    for (const auto& pair : reversedEdges)
    {
        for (const auto& child : pair.second)
        {
            indegree[child.get()]++;
        }
    }

    std::vector<PhysicalOpPtr> newRoots;
    for (const auto& node : allNodes)
    {
        if (indegree[node.get()] == 0)
        {
            newRoots.push_back(node);
        }
    }

    for (const auto& node : allNodes)
    {
        node->children = reversedEdges[node.get()];
    }

    for (auto rootOperator : newRoots)
    {
        INVARIANT(
            rootOperator->physicalOperator.tryGet<SourcePhysicalOperator>(),
            "Expects SourceOperators as roots after flip but got {}",
            rootOperator->physicalOperator);
    }
    return PhysicalPlan(plan.queryId, newRoots, PhysicalPlan::PlanDirection::SourceToSink);
}
}
