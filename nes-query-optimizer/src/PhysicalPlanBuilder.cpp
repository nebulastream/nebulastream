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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/ExecutionMode.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <PhysicalPlanBuilder.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>

namespace NES
{

PhysicalPlanBuilder::PhysicalPlanBuilder(QueryId id) : queryId(id)
{
}

void PhysicalPlanBuilder::addSinkRoot(std::shared_ptr<PhysicalOperatorWrapper> sink)
{
    PRECONDITION(sink->getPhysicalOperator().tryGet<SinkPhysicalOperator>(), "Expects SinkOperators as roots");
    sinks.emplace_back(std::move(sink));
}

void PhysicalPlanBuilder::setExecutionMode(Nautilus::Configurations::ExecutionMode mode)
{
    executionMode = mode;
}

void PhysicalPlanBuilder::setOperatorBufferSize(uint64_t bufferSize)
{
    operatorBufferSize = bufferSize;
}

PhysicalPlan PhysicalPlanBuilder::finalize() &&
{
    auto sources = flip(sinks);
    return {queryId, std::move(sources), executionMode, operatorBufferSize};
}

using PhysicalOpPtr = std::shared_ptr<PhysicalOperatorWrapper>;

PhysicalPlanBuilder::Roots PhysicalPlanBuilder::flip(const Roots& rootOperators)
{
    PRECONDITION(rootOperators.size() == 1, "For now we can only flip graphs with a single root");

    std::unordered_set<PhysicalOperatorWrapper*> visited;
    std::vector<PhysicalOpPtr> allNodes;

    auto collectNodes = [&visited, &allNodes](const PhysicalOpPtr& node, auto&& self) -> void
    {
        if (node and not visited.contains(node.get()))
        {
            visited.insert(node.get());
            allNodes.push_back(node);
            for (const auto& child : node->getChildren())
            {
                self(child, self);
            }
        }
    };
    collectNodes(rootOperators[0], collectNodes);

    std::unordered_map<PhysicalOperatorWrapper*, std::vector<PhysicalOpPtr>> reversedEdges;
    std::unordered_map<PhysicalOperatorWrapper*, int> inDegree;
    for (const auto& node : allNodes)
    {
        reversedEdges.try_emplace(node.get(), std::vector<PhysicalOpPtr>{});
        for (const auto& child : node->getChildren())
        {
            reversedEdges.try_emplace(child.get(), std::vector<PhysicalOpPtr>{});
            reversedEdges[child.get()].push_back(node);
        }
        /// clear
        node->setChildren(std::vector<PhysicalOpPtr>{});
        inDegree[node.get()] = 0;
    }

    for (const auto& pair : reversedEdges)
    {
        for (const auto& child : pair.second)
        {
            inDegree[child.get()]++;
        }
    }

    std::vector<PhysicalOpPtr> newRoots;
    for (const auto& node : allNodes)
    {
        if (inDegree[node.get()] == 0)
        {
            newRoots.push_back(node);
        }
    }

    for (const auto& node : allNodes)
    {
        node->setChildren(reversedEdges[node.get()]);
    }

    for (const auto& rootOperator : newRoots)
    {
        INVARIANT(
            rootOperator->getPhysicalOperator().tryGet<SourcePhysicalOperator>(),
            "Expects SourceOperators as roots after flip but got {}",
            rootOperator->getPhysicalOperator());
    }
    return newRoots;
}
}
