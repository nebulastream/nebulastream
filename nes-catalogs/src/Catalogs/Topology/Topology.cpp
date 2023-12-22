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

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <algorithm>
#include <deque>
#include <utility>

namespace NES {

Topology::Topology() : rootWorkerId(INVALID_WORKER_NODE_ID) {}

TopologyPtr Topology::create() { return std::shared_ptr<Topology>(new Topology()); }

bool Topology::registerTopologyNode(NES::TopologyNodePtr&& newTopologyNode) {

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    auto newWorkerId = newTopologyNode->getId();
    if (!lockedWorkerIdToTopologyNodeMap->contains(newWorkerId)) {
        NES_INFO("Adding New Node {} to the catalog of nodes.", newTopologyNode->toString());
        (*lockedWorkerIdToTopologyNodeMap)[newWorkerId] = newTopologyNode;
        return true;
    }
    NES_WARNING("Topology node with id {} already exists. Failed to register the new topology node.", newWorkerId);
    return false;
}

bool Topology::addTopologyNodeAsChild(WorkerId parentWorkerId, WorkerId childWorkerId) {

    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();

    if (!lockedWorkerIdToTopologyNodeMap->contains(parentWorkerId)) {
        NES_WARNING("No parent topology node with id {} registered.", parentWorkerId);
        return false;
    }

    if (!lockedWorkerIdToTopologyNodeMap->contains(childWorkerId)) {
        NES_WARNING("No child topology node with id {} registered.", childWorkerId);
        return false;
    }

    auto lockedParent = (*lockedWorkerIdToTopologyNodeMap)[parentWorkerId].rlock();
    auto children = (*lockedParent)->getChildren();
    for (const auto& child : children) {
        if (child->as<TopologyNode>()->getId() == childWorkerId) {
            NES_ERROR("TopologyManagerService::AddParent: nodes {} and {} already exists", childWorkerId, parentWorkerId);
            return false;
        }
    }
    auto lockedChild = (*lockedWorkerIdToTopologyNodeMap)[childWorkerId].rlock();
    NES_INFO("Adding Node {} as child to the node {}", (*lockedChild)->toString(), (*lockedParent)->toString());
    return (*lockedParent)->addChild((*lockedChild));
}

bool Topology::removeTopologyNode(const TopologyNodePtr& nodeToRemove) {

    NES_INFO("Removing Node {}", nodeToRemove->toString());

    WorkerId workerIdToRemove = nodeToRemove->getId();
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();

    if (!lockedWorkerIdToTopologyNodeMap->contains(workerIdToRemove)) {
        NES_WARNING("The physical node {} doesn't exists in the system.", workerIdToRemove);
        return false;
    }

    if (rootWorkerId == INVALID_WORKER_NODE_ID) {
        NES_WARNING("No root node exists in the topology");
        return false;
    }

    if (rootWorkerId == workerIdToRemove) {
        NES_WARNING("Attempt to remove the root node. Removing root node is not allowed.");
        return false;
    }

    nodeToRemove->removeAllParent();
    nodeToRemove->removeChildren();
    lockedWorkerIdToTopologyNodeMap->erase(workerIdToRemove);
    NES_DEBUG("Successfully removed the node.");
    return true;
}

bool Topology::nodeWithWorkerIdExists(WorkerId workerId) {
    auto lockedWorkerIdToTopologyNodeMap = workerIdToTopologyNode.wlock();
    return lockedWorkerIdToTopologyNodeMap->contains(workerId);
}

TopologyNodePtr Topology::getRoot() { return rootNode; }

TopologyNodePtr Topology::findWorkerWithId(WorkerId workerId) {

    NES_INFO("Finding a physical node with id {}", workerId);
    if (workerIdToTopologyNode.contains(workerId)) {
        NES_DEBUG("Found a physical node with id {}", workerId);
        return workerIdToTopologyNode[workerId];
    }
    NES_WARNING("Unable to find a physical node with id {}", workerId);
    return nullptr;
}

void Topology::setAsRoot(const TopologyNodePtr& physicalNode) {

    NES_INFO("Setting physical node {} as root to the topology.", physicalNode->toString());
    workerIdToTopologyNode[physicalNode->getId()] = physicalNode;
    rootNode = physicalNode;
}

bool Topology::removeNodeAsChild(const TopologyNodePtr& parentNode, const TopologyNodePtr& childNode) {

    NES_INFO("Removing node {} as child to the node {}", childNode->toString(), parentNode->toString());
    return parentNode->remove(childNode);
}

bool Topology::occupySlots(WorkerId workerId, uint16_t amountToOccupy) {

    NES_INFO("Reduce {} resources from node with id {}", amountToOccupy, workerId);
    if (workerIdToTopologyNode.contains(workerId)) {
        return workerIdToTopologyNode[workerId]->occupyResources(amountToOccupy);
    }
    NES_WARNING("Unable to find node with id {}", workerId);
    return false;
}

bool Topology::releaseSlots(WorkerId workerId, uint16_t amountToRelease) {

    NES_INFO("Increase {} resources from node with id {}", amountToRelease, workerId);
    if (workerIdToTopologyNode.contains(workerId)) {
        return workerIdToTopologyNode[workerId]->releaseSlots(amountToRelease);
    }
    NES_WARNING("Unable to find node with id {}", workerId);
    return false;
}

bool Topology::acquireLockOnTopologyNode(WorkerId workerId) {

    if (workerIdToTopologyNode.contains(workerId)) {
        return workerIdToTopologyNode[workerId]->acquireLock();
    }
    NES_WARNING("Unable to locate topology node with id {}", workerId);
    return false;
}

bool Topology::releaseLockOnTopologyNode(NES::WorkerId workerId) {

    if (workerIdToTopologyNode.contains(workerId)) {
        return workerIdToTopologyNode[workerId]->releaseLock();
    }
    NES_WARNING("Unable to locate topology node with id {}", workerId);
    return false;
}

std::string Topology::toString() {

    if (!rootNode) {
        NES_WARNING("No root node found");
        return "";
    }

    std::stringstream topologyInfo;
    topologyInfo << std::endl;

    // store pair of TopologyNodePtr and its depth in when printed
    std::deque<std::pair<TopologyNodePtr, uint64_t>> parentToPrint{std::make_pair(rootNode, 0)};

    // indent offset
    int indent = 2;

    // perform dfs traverse
    while (!parentToPrint.empty()) {
        std::pair<TopologyNodePtr, uint64_t> nodeToPrint = parentToPrint.front();
        parentToPrint.pop_front();
        for (std::size_t i = 0; i < indent * nodeToPrint.second; i++) {
            if (i % indent == 0) {
                topologyInfo << '|';
            } else {
                if (i >= indent * nodeToPrint.second - 1) {
                    topologyInfo << std::string(indent, '-');
                } else {
                    topologyInfo << std::string(indent, ' ');
                }
            }
        }
        topologyInfo << nodeToPrint.first->toString() << std::endl;

        for (const auto& child : nodeToPrint.first->getChildren()) {
            parentToPrint.emplace_front(child->as<TopologyNode>(), nodeToPrint.second + 1);
        }
    }
    return topologyInfo.str();
}

void Topology::print() { NES_DEBUG("Topology print:{}", toString()); }
}// namespace NES