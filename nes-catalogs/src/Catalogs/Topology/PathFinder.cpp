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

#include <Catalogs/Topology/PathFinder.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Node.hpp>
#include <Util/Logger/Logger.hpp>
#include <deque>

namespace NES {

PathFinder::PathFinder(std::vector<WorkerId> rootWorkerIds) : rootWorkerIds(rootWorkerIds) {}

TopologyNodePtr PathFinder::findCommonAncestor(std::vector<TopologyNodePtr> topologyNodes) {

    NES_DEBUG("Topology: find common node for a set of topology nodes.");

    if (topologyNodes.empty()) {
        NES_WARNING("Topology: Input topology node list was empty.");
        return nullptr;
    }

    //Check if one of the input node is a root node of the topology
    auto found = std::find_if(topologyNodes.begin(), topologyNodes.end(), [&](const TopologyNodePtr& topologyNode) {
        auto foundInRootWorkerIds = std::find(rootWorkerIds.begin(), rootWorkerIds.end(), topologyNode->getId());
        return foundInRootWorkerIds != rootWorkerIds.end();
    });

    // If a root node found in the input nodes then return the root topology node
    if (found != topologyNodes.end()) {
        return *found;
    }

    NES_DEBUG("Topology: Selecting a start node to identify the common ancestor.");
    TopologyNodePtr startNode = topologyNodes[0];
    bool foundAncestor = false;
    TopologyNodePtr resultAncestor;
    NES_TRACE("Topology: Adding selected node to the deque for further processing.");
    std::deque<NodePtr> nodesToProcess{startNode};
    while (!nodesToProcess.empty()) {
        TopologyNodePtr candidateNode = nodesToProcess.front()->as<TopologyNode>();
        nodesToProcess.pop_front();
        NES_TRACE(
            "Topology: Check if the children topology node of the node under consideration contains all input topology nodes.");
        std::vector<NodePtr> children = candidateNode->getAndFlattenAllChildren(false);
        for (auto& nodeToLook : topologyNodes) {
            auto found = std::find_if(children.begin(), children.end(), [&](const NodePtr& child) {
                return nodeToLook->getId() == child->as<TopologyNode>()->getId();
            });

            if (found == children.end()) {
                NES_TRACE("Topology: Unable to find the input topology node as child of the node under consideration.");
                foundAncestor = false;
                break;
            }
            foundAncestor = true;
        }

        if (foundAncestor) {
            NES_TRACE("Topology: The node under consideration contains all input node as its children.");
            return candidateNode;
        }

        NES_TRACE("Topology: Add parent of the the node under consideration to the deque for further processing.");
        auto parents = candidateNode->getParents();
        for (const auto& parent : parents) {
            if (!parent->as<TopologyNode>()->isUnderMaintenance())
                nodesToProcess.push_back(parent);
        }
    }

    NES_ERROR("Topology: Unable to find a common ancestor topology node for the input topology nodes.");
    return nullptr;
}

TopologyNodePtr PathFinder::findCommonChild(std::vector<TopologyNodePtr> topologyNodes) {
    NES_INFO("Topology: find common child node for a set of parent topology nodes.");

    if (topologyNodes.empty()) {
        NES_WARNING("Topology: Input topology node list was empty.");
        return nullptr;
    }

    NES_DEBUG("Topology: Selecting a start node to identify the common child.");
    TopologyNodePtr startNode = topologyNodes[0];
    bool foundDescendent = false;
    TopologyNodePtr resultDescendent;
    NES_TRACE("Topology: Adding selected node to the deque for further processing.");
    std::deque<NodePtr> nodesToProcess{startNode};
    while (!nodesToProcess.empty()) {
        TopologyNodePtr candidateNode = nodesToProcess.front()->as<TopologyNode>();
        nodesToProcess.pop_front();
        NES_TRACE(
            "Topology: Check if the parent topology node of the node under consideration contains all input topology nodes.");
        std::vector<NodePtr> parents = candidateNode->getAndFlattenAllAncestors();
        for (auto& nodeToLook : topologyNodes) {
            auto found = std::find_if(parents.begin(), parents.end(), [&](const NodePtr& parent) {
                return nodeToLook->getId() == parent->as<TopologyNode>()->getId();
            });

            if (found == parents.end()) {
                NES_TRACE("Topology: Unable to find the input topology node as parent of the node under consideration.");
                foundDescendent = false;
                break;
            }
            foundDescendent = true;
        }

        if (foundDescendent) {
            NES_TRACE("Topology: The node under consideration contains all input node as its parent.");
            return candidateNode;
        }

        NES_TRACE("Topology: Add children of the the node under consideration to the deque for further processing.");
        auto children = candidateNode->getChildren();
        for (const auto& child : children) {
            if (!child->as<TopologyNode>()->isUnderMaintenance()) {
                nodesToProcess.push_back(child);
            }
        }
    }
    NES_WARNING("Topology: Unable to find a common child topology node for the input topology nodes.");
    return nullptr;
}

TopologyNodePtr PathFinder::findCommonNodeBetween(std::vector<TopologyNodePtr> childNodes,
                                                  std::vector<TopologyNodePtr> parenNodes) {
    NES_DEBUG("Topology: Find a common ancestor node for the input children nodes.");
    TopologyNodePtr commonAncestorForChildren = findCommonAncestor(std::move(childNodes));
    if (!commonAncestorForChildren) {
        NES_WARNING("Topology: Unable to find a common ancestor node for the input child node.");
        return nullptr;
    }

    NES_DEBUG("Topology: Find a common child node for the input parent nodes.");
    TopologyNodePtr commonChildForParents = findCommonChild(std::move(parenNodes));
    if (!commonChildForParents) {
        NES_WARNING("Topology: Unable to find a common child node for the input parent nodes.");
        return nullptr;
    }

    if (commonChildForParents->getId() == commonAncestorForChildren->getId()) {
        NES_DEBUG("Topology: Both common child and ancestor are same node. Returning as result.");
        return commonChildForParents;
    }
    if (commonChildForParents->containAsChild(commonAncestorForChildren)) {
        NES_DEBUG("Topology: Returning the common children of the parent topology nodes");
        return commonChildForParents;
    } else if (!commonChildForParents->containAsParent(commonAncestorForChildren)) {
        NES_WARNING("Topology: Common child is not connected to the common ancestor.");
        return nullptr;
    }
    NES_DEBUG("Topology: Returning common ancestor as result.");
    return commonAncestorForChildren;
}

std::vector<TopologyNodePtr> PathFinder::findNodesBetween(const TopologyNodePtr& sourceNode,
                                                          const TopologyNodePtr& destinationNode) {

    NES_DEBUG("Topology: Find topology nodes between source and destination nodes.");
    if (sourceNode->getId() == destinationNode->getId()) {
        NES_DEBUG("Topology: Both source and destination are same node.");
        return {sourceNode};
    }
    if (!sourceNode->containAsParent(destinationNode)) {
        NES_WARNING("Topology: source node is not connected to the destination node.");
        return {};
    }

    std::vector<TopologyNodePtr> nodesBetween;
    NES_DEBUG("Topology: iterate over parent of the source node and find path between its parent and destination nodes.");
    auto parents = sourceNode->getParents();
    for (const auto& sourceParent : parents) {
        std::vector<TopologyNodePtr> foundBetweenNodes = findNodesBetween(sourceParent->as<TopologyNode>(), destinationNode);
        if (!foundBetweenNodes.empty()) {
            NES_TRACE("Topology: found a path between source nodes parent and destination nodes.");
            nodesBetween.push_back(sourceNode);
            nodesBetween.insert(nodesBetween.end(), foundBetweenNodes.begin(), foundBetweenNodes.end());
            return nodesBetween;
        }
    }
    NES_DEBUG("Topology: return the found path between source and destination nodes.");
    return nodesBetween;
}

std::vector<TopologyNodePtr> PathFinder::findNodesBetween(std::vector<TopologyNodePtr> sourceNodes,
                                                          std::vector<TopologyNodePtr> destinationNodes) {
    NES_DEBUG("Topology: Find a common ancestor node for the input children nodes.");
    TopologyNodePtr commonAncestorForChildren = findCommonAncestor(std::move(sourceNodes));
    if (!commonAncestorForChildren) {
        NES_WARNING("Topology: Unable to find a common ancestor node for the input child node.");
        return {};
    }

    NES_DEBUG("Topology: Find a common child node for the input parent nodes.");
    TopologyNodePtr commonChildForParents = findCommonChild(std::move(destinationNodes));
    if (!commonChildForParents) {
        NES_WARNING("Topology: Unable to find a common child node for the input parent nodes.");
        return {};
    }

    return findNodesBetween(commonAncestorForChildren, commonChildForParents);
}

}// namespace NES
