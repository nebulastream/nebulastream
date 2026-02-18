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
#include <PhysicalPlanBuilder.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Util/ExecutionMode.hpp>
#include <ErrorHandling.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
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

void PhysicalPlanBuilder::setExecutionMode(ExecutionMode mode)
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

/// Custom hasher for shared_ptr keys — hashes the underlying raw pointer value.
struct SharedPtrHash
{
    size_t operator()(const PhysicalOpPtr& ptr) const { return std::hash<PhysicalOperatorWrapper*>{}(ptr.get()); }
};

/// Custom equality for shared_ptr keys — compares underlying raw pointer identity.
struct SharedPtrEqual
{
    bool operator()(const PhysicalOpPtr& lhs, const PhysicalOpPtr& rhs) const { return lhs.get() == rhs.get(); }
};

namespace
{
/// Verifies post-flip invariants: roots are sources, leaves are sinks, edge count preserved, all nodes reachable.
void verifyFlippedGraph(
    [[maybe_unused]] const std::vector<PhysicalOpPtr>& newRoots,
    [[maybe_unused]] const std::vector<PhysicalOpPtr>& allNodes,
    [[maybe_unused]] size_t edgeCountBefore)
{
    for (const auto& rootOperator : newRoots)
    {
        INVARIANT(
            rootOperator->getPhysicalOperator().tryGet<SourcePhysicalOperator>(),
            "Expects SourceOperators as roots after flip but got {}",
            rootOperator->getPhysicalOperator());
    }

    size_t edgeCountAfter = 0;
    for (const auto& node : allNodes)
    {
        edgeCountAfter += node->getChildren().size();
    }
    INVARIANT(edgeCountBefore == edgeCountAfter, "Edge count mismatch after flip: before={} after={}", edgeCountBefore, edgeCountAfter);

    std::unordered_set<PhysicalOpPtr, SharedPtrHash, SharedPtrEqual> reachable;
    std::queue<PhysicalOpPtr> bfsQueue;
    for (const auto& root : newRoots)
    {
        bfsQueue.push(root);
        reachable.insert(root);
    }
    while (!bfsQueue.empty())
    {
        auto current = bfsQueue.front();
        bfsQueue.pop();
        for (const auto& child : current->getChildren())
        {
            if (reachable.insert(child).second)
            {
                bfsQueue.push(child);
            }
        }
    }
    INVARIANT(
        reachable.size() == allNodes.size(),
        "Not all nodes reachable after flip: reachable={} total={}",
        reachable.size(),
        allNodes.size());

    for (const auto& node : allNodes)
    {
        if (node->getChildren().empty())
        {
            INVARIANT(
                node->getPhysicalOperator().tryGet<SinkPhysicalOperator>(),
                "Expects SinkOperators as leaves after flip but got {}",
                node->getPhysicalOperator());
        }
    }
}
}

PhysicalPlanBuilder::Roots PhysicalPlanBuilder::flip(const Roots& rootOperators)
{
    PRECONDITION(rootOperators.size() == 1, "For now we can only flip graphs with a single root");

    /// DFS visit states for cycle detection.
    enum class VisitState : uint8_t
    {
        Unvisited,
        InProgress,
        Completed
    };

    std::unordered_map<PhysicalOpPtr, VisitState, SharedPtrHash, SharedPtrEqual> visitStateMap;
    std::vector<PhysicalOpPtr> allNodes;

    auto collectNodes = [&visitStateMap, &allNodes](const PhysicalOpPtr& node, auto&& self) -> void
    {
        auto [it, inserted] = visitStateMap.try_emplace(node, VisitState::Unvisited);
        PRECONDITION(
            it->second != VisitState::InProgress,
            "Cycle detected in physical plan DAG during flip — node visited twice on the same DFS path");
        if (it->second == VisitState::Completed)
        {
            return; /// already fully processed
        }
        it->second = VisitState::InProgress;
        allNodes.push_back(node);
        for (const auto& child : node->getChildren())
        {
            self(child, self);
        }
        it->second = VisitState::Completed;
    };
    collectNodes(rootOperators[0], collectNodes);

    /// Count edges before clearing children.
    size_t edgeCountBefore = 0;
    std::unordered_map<PhysicalOpPtr, std::vector<PhysicalOpPtr>, SharedPtrHash, SharedPtrEqual> reversedEdges;
    std::unordered_map<PhysicalOpPtr, int, SharedPtrHash, SharedPtrEqual> inDegree;
    for (const auto& node : allNodes)
    {
        reversedEdges.try_emplace(node, std::vector<PhysicalOpPtr>{});
        for (const auto& child : node->getChildren())
        {
            reversedEdges.try_emplace(child, std::vector<PhysicalOpPtr>{});
            reversedEdges[child].push_back(node);
            ++edgeCountBefore;
        }
        /// clear
        node->setChildren(std::vector<PhysicalOpPtr>{});
        inDegree[node] = 0;
    }

    for (const auto& [parent, children] : reversedEdges)
    {
        for (const auto& child : children)
        {
            inDegree[child]++;
        }
    }

    std::vector<PhysicalOpPtr> newRoots;
    for (const auto& node : allNodes)
    {
        if (inDegree[node] == 0)
        {
            newRoots.push_back(node);
        }
    }

    for (const auto& node : allNodes)
    {
        node->setChildren(reversedEdges[node]);
    }

    verifyFlippedGraph(newRoots, allNodes, edgeCountBefore);
    return newRoots;
}
}
