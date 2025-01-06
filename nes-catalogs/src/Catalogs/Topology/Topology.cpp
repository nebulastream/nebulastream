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

#include <Catalogs/Topology/Index/LocationIndex.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/WorkerConfigurationKeys.hpp>
#include <Identifiers/NESStrongTypeJson.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Nodes/Iterators/DepthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/LogicalOperator.hpp>
#include <Runtime/OpenCLDeviceInfo.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Mobility/SpatialType.hpp>
#include <Util/Mobility/SpatialTypeUtility.hpp>
#include <Util/Placement/ElegantPayloadKeys.hpp>
#include <algorithm>
#include <deque>
#include <queue>
#include <utility>

namespace NES {

Topology::Topology() { locationIndex = std::make_shared<NES::Spatial::Index::Experimental::LocationIndex>(); }

TopologyPtr Topology::create() { return std::shared_ptr<Topology>(new Topology()); }

std::vector<WorkerId> Topology::getRootWorkerNodeIds() { return rootWorkerIds; }

void Topology::addAsRootWorkerId(WorkerId workerId) { rootWorkerIds.emplace_back(workerId); }

WorkerId Topology::registerWorkerAsRoot(WorkerId newRootWorkerId,
                                        const std::string& address,
                                        const int64_t grpcPort,
                                        const int64_t dataPort,
                                        const uint16_t numberOfSlots,
                                        std::map<std::string, std::any> workerProperties,
                                        uint32_t bandwidthInMbps,
                                        uint32_t latencyInMs) {

    NES_TRACE("Register Node as root address={} numberOfSlots={}", address, numberOfSlots);
    NES_DEBUG("Topology before insert");
    NES_DEBUG("", toString());

    // if worker is started with a workerId
    // then check if invalid worker id or an active worker with given workerId already exists
    if (newRootWorkerId == INVALID_WORKER_NODE_ID || nodeWithWorkerIdExists(newRootWorkerId)) {
        NES_WARNING("Worker id {} either invalid or a node with this id already "
                    "exists. A new worker id will be assigned.",
                    newRootWorkerId);
        newRootWorkerId = getNextWorkerId();
    }

    if (!workerIdToTopologyNode.contains(newRootWorkerId)) {
        TopologyNodePtr newTopologyNode =
            TopologyNode::create(newRootWorkerId, address, grpcPort, dataPort, numberOfSlots, workerProperties);
        NES_INFO("Adding New Node {} to the catalog of nodes.", newTopologyNode->toString());
        workerIdToTopologyNode[newRootWorkerId] = newTopologyNode;
        if (!rootWorkerIds.empty()) {
            //Get the first root worker id and then add all its upstream workers as the upstream worker to the new root
            auto firstRootWorkerId = rootWorkerIds[0];
            auto childWorkerIds = getChildTopologyNodeIds(firstRootWorkerId);
            for (const auto& childWorkerId : childWorkerIds) {
                NES_DEBUG("Add link between the root node {} and child {}", newRootWorkerId, childWorkerId);
                addTopologyNodeAsChild(newRootWorkerId, childWorkerId);
                addLinkProperty(newRootWorkerId, childWorkerId, bandwidthInMbps, latencyInMs);
            }
        }
        //Register as the root worker
        addAsRootWorkerId(newRootWorkerId);
        NES_DEBUG(" topology after insert {} ", toString());
        return newRootWorkerId;
    }
    NES_WARNING("Topology node with id {} already exists. Failed to register the new topology node.", newRootWorkerId);
    return INVALID_WORKER_NODE_ID;
}

WorkerId Topology::registerWorker(WorkerId workerId,
                                  const std::string& address,
                                  const int64_t grpcPort,
                                  const int64_t dataPort,
                                  const uint16_t numberOfSlots,
                                  std::map<std::string, std::any> workerProperties,
                                  uint32_t bandwidthInMbps,
                                  uint32_t latencyInMs) {

    NES_TRACE("TopologyManagerService: Register Node address={} numberOfSlots={}", address, numberOfSlots);
    NES_DEBUG(" topology before insert");
    NES_DEBUG("", toString());

    // if worker is started with a workerId
    // then check if invalid worker id or an active worker with given workerId already exists
    if (workerId == INVALID_WORKER_NODE_ID || nodeWithWorkerIdExists(workerId)) {
        NES_WARNING(" worker id {} either invalid or a node with this id already "
                    "exists. A new worker id will be assigned.",
                    workerId);
        workerId = getNextWorkerId();
    }


    if (!workerIdToTopologyNode.contains(workerId)) {
        TopologyNodePtr newTopologyNode =
            TopologyNode::create(workerId, address, grpcPort, dataPort, numberOfSlots, workerProperties);

        NES_INFO("Adding New Node {} to the catalog of nodes.", newTopologyNode->toString());
        workerIdToTopologyNode[workerId] = newTopologyNode;
        NES_DEBUG(" register node");
        auto isRoot = rootWorkerIds.empty();
        if (isRoot) {
            NES_DEBUG(" tree is empty so this becomes new root");
            addAsRootWorkerId(workerId);
        } else {
            for (const auto& rootWorkerId : rootWorkerIds) {
                NES_DEBUG(" add link to the root node {}", rootWorkerId);
                addTopologyNodeAsChild(rootWorkerId, workerId);
                addLinkProperty(rootWorkerId, workerId, bandwidthInMbps, latencyInMs);
            }
        }
        NES_DEBUG(" topology after insert {} ", toString());
        if (workerId != INVALID_WORKER_NODE_ID && !isRoot) {
            informNeighborsOfNode(workerId, newTopologyNode->getGrpcAddress());
        }
        return workerId;
    }
    NES_WARNING("Topology node with id {} already exists. Failed to register the new topology node.", workerId);

    return INVALID_WORKER_NODE_ID;
}

std::vector<WorkerId> Topology::getParentTopologyNodeIds(WorkerId nodeId) {

    if (!workerIdToTopologyNode.contains(nodeId)) {
        NES_WARNING("No topology node with id {} registered.", nodeId);
        return {};
    }

    std::vector<WorkerId> parentIds;
    auto lockedParent = workerIdToTopologyNode.at(nodeId).rlock();
    auto parents = (*lockedParent)->getParents();
    for (const auto& parent : parents) {
        parentIds.emplace_back(parent->as<TopologyNode>()->getId());
    }
    return parentIds;
}

std::vector<WorkerId> Topology::getChildTopologyNodeIds(WorkerId nodeId) {

    if (!workerIdToTopologyNode.contains(nodeId)) {
        NES_WARNING("No topology node with id {} registered.", nodeId);
        return {};
    }

    std::vector<WorkerId> childIds;
    auto lockedParent = workerIdToTopologyNode.at(nodeId).rlock();
    auto children = (*lockedParent)->getChildren();
    for (const auto& child : children) {
        childIds.emplace_back(child->as<TopologyNode>()->getId());
    }
    return childIds;
}

bool Topology::addTopologyNodeAsChild(WorkerId parentWorkerId, WorkerId childWorkerId) {

    if (parentWorkerId == childWorkerId) {
        NES_ERROR("Can not add link to itself.");
        return false;
    }

    if (!workerIdToTopologyNode.contains(parentWorkerId)) {
        NES_WARNING("No parent topology node with id {} registered.", parentWorkerId);
        return false;
    }
    if (!workerIdToTopologyNode.contains(childWorkerId)) {
        NES_WARNING("No child topology node with id {} registered.", childWorkerId);
        return false;
    }

    auto success = false;
    std::string parentAddress = "";
    std::string childAddress = "";
    {
        auto lockedParent = workerIdToTopologyNode.at(parentWorkerId).rlock();
        auto children = (*lockedParent)->getChildren();
        for (const auto& child : children) {
            if (child->as<TopologyNode>()->getId() == childWorkerId) {
                NES_ERROR("TopologyManagerService::AddParent: parent relationship between nodes {} and {} already exists",
                          childWorkerId,
                          parentWorkerId);
                return false;
            }
        }

        auto lockedChild = workerIdToTopologyNode.at(childWorkerId).rlock();
        NES_INFO("Adding Node {} as child to the node {}", (*lockedChild)->toString(), (*lockedParent)->toString());

        success = (*lockedParent)->addChild((*lockedChild));
        parentAddress = (*lockedParent)->getFullRPCAddress();
        childAddress = (*lockedChild)->getFullRPCAddress();
    }
    informNeighborsOfNode(parentWorkerId, parentAddress);
    informNeighborsOfNode(childWorkerId, childAddress);
    return success;
}

bool Topology::unregisterWorker(WorkerId topologyNodeId) {

    NES_DEBUG("TopologyManagerService::UnregisterNode: try to disconnect sensor with id  {}", topologyNodeId);

    if (!workerIdToTopologyNode.contains(topologyNodeId)) {
        NES_WARNING("The physical node {} doesn't exists in the system.", topologyNodeId);
        return false;
    }

    auto found = std::find(rootWorkerIds.begin(), rootWorkerIds.end(), topologyNodeId);
    if (found != rootWorkerIds.end()) {
        NES_WARNING("Removing the root node {}.", topologyNodeId);
        rootWorkerIds.erase(found);
    }
    std::string nodeAddress = "";
    {
        // Fetch topology node and clear parent child nodes
        auto lockedTopologyNode = workerIdToTopologyNode.at(topologyNodeId).wlock();
        if ((*lockedTopologyNode)->getSpatialNodeType() == NES::Spatial::Experimental::SpatialType::FIXED_LOCATION) {
            auto lockedLocationIndex = locationIndex.wlock();
            if (!(*lockedLocationIndex)->removeNodeFromSpatialIndex(topologyNodeId)) {
                NES_ERROR("Unable to remove the topology node from the spatial index.");
                return false;
            }
        }

        (*lockedTopologyNode)->removeAllParent();
        (*lockedTopologyNode)->removeChildren();
        nodeAddress = (*lockedTopologyNode)->getFullRPCAddress();
        // ULTRA IMPORTANT COMMON SENSE: Release the lock on the object before its deletion.
        lockedTopologyNode.unlock();
        // Delete the object
        workerIdToTopologyNode.erase(topologyNodeId);
        NES_DEBUG("Successfully removed the node {}.", topologyNodeId);
    }
    informNeighborsOfNode(topologyNodeId, nodeAddress);
    return true;
}

TopologyNodePtr Topology::getCopyOfTopologyNodeWithId(WorkerId workerId) const {

    NES_INFO("Finding a physical node with id {}", workerId);
    if (workerIdToTopologyNode.contains(workerId)) {
        NES_DEBUG("Found a physical node with id {}", workerId);
        return (*workerIdToTopologyNode.at(workerId).wlock())->copy();
    }
    NES_WARNING("Unable to find a physical node with id {}", workerId);
    return nullptr;
}

std::optional<std::string> Topology::getGrpcAddress(WorkerId workerId) const {
    if (workerIdToTopologyNode.contains(workerId)) {
        NES_DEBUG("Found a physical node with id {}", workerId);
        return (*workerIdToTopologyNode.at(workerId).rlock())->getGrpcAddress();
    }
    return std::nullopt;
}

bool Topology::nodeWithWorkerIdExists(WorkerId workerId) { return workerIdToTopologyNode.contains(workerId); }

bool Topology::setForMaintenance(WorkerId workerId, bool state) {

    if (!workerIdToTopologyNode.contains(workerId)) {
        NES_WARNING("Node with id {} does not exists.", workerId);
        return false;
    }

    auto lockedTopologyNode = workerIdToTopologyNode.at(workerId).wlock();
    (*lockedTopologyNode)->setForMaintenance(state);
    return true;
}

bool Topology::removeTopologyNodeAsChild(WorkerId parentWorkerId, WorkerId childWorkerId) {
    NES_INFO("Removing node {} as child to the node {}", childWorkerId, parentWorkerId);

    if (parentWorkerId == childWorkerId) {
        NES_WARNING("Can not remove self as child");
        return false;
    }

    if (!workerIdToTopologyNode.contains(parentWorkerId)) {
        NES_WARNING("The physical node {} doesn't exists in the system.", parentWorkerId);
        return false;
    }

    if (!workerIdToTopologyNode.contains(childWorkerId)) {
        NES_WARNING("The physical node {} doesn't exists in the system.", childWorkerId);
        return false;
    }

    bool success = false;
    std::string parentAddress = "";
    std::string childAddress = "";
    {
        auto [lockedParentTopologyNode, lockedChildTopologyNode] =
           folly::acquireLocked(workerIdToTopologyNode.at(parentWorkerId), workerIdToTopologyNode.at(childWorkerId));


        //Remove associated link property if exists
        (*lockedParentTopologyNode)->removeLinkProperty(childWorkerId);
        (*lockedChildTopologyNode)->removeLinkProperty(parentWorkerId);
        success = (*lockedParentTopologyNode)->removeChild((*lockedChildTopologyNode));
        parentAddress = (*lockedParentTopologyNode)->getFullRPCAddress();
        childAddress = (*lockedChildTopologyNode)->getFullRPCAddress();
    }
    informNeighborsOfNode(parentWorkerId, parentAddress);
    informNeighborsOfNode(childWorkerId, childAddress);
    return success;
}

bool Topology::addLinkProperty(WorkerId parentWorkerId, WorkerId childWorkerId, uint64_t bandwidthInMBPS, uint64_t latencyInMS) {
    NES_INFO("Adding link properties between parent node {} and child node {}", parentWorkerId, childWorkerId);

    if (parentWorkerId == childWorkerId) {
        NES_WARNING("Can not add link property to self.");
        return false;
    }

    //Check if the two nodes exists
    if (!workerIdToTopologyNode.contains(parentWorkerId)) {
        NES_WARNING("The physical node {} doesn't exists in the system.", parentWorkerId);
        return false;
    }
    if (!workerIdToTopologyNode.contains(childWorkerId)) {
        NES_WARNING("The physical node {} doesn't exists in the system.", childWorkerId);
        return false;
    }

    auto [lockedParentTopologyNode, lockedChildTopologyNode] =
        folly::acquireLocked(workerIdToTopologyNode.at(parentWorkerId), workerIdToTopologyNode.at(childWorkerId));

    if (!(*lockedParentTopologyNode)->containAsChild((*lockedChildTopologyNode))) {
        NES_WARNING("No link exists between parent and child topology node.", parentWorkerId, childWorkerId);
        return false;
    }

    // Add link properties to both parent and child links
    auto linkProperty = std::make_shared<LinkProperty>(bandwidthInMBPS, latencyInMS);
    (*lockedParentTopologyNode)->addLinkProperty(childWorkerId, linkProperty);
    (*lockedChildTopologyNode)->addLinkProperty(parentWorkerId, linkProperty);
    return true;
}

bool Topology::occupySlots(WorkerId workerId, uint16_t amountToOccupy) {

    if (workerIdToTopologyNode.contains(workerId)) {
        NES_INFO("Reduce {} resources from node with id {}", amountToOccupy, workerId);
        return (*workerIdToTopologyNode.at(workerId).wlock())->occupySlots(amountToOccupy);
    }
    NES_WARNING("Unable to occupy slots as the node with id {} do not exists.", workerId);
    return false;
}

bool Topology::releaseSlots(WorkerId workerId, uint16_t amountToRelease) {

    if (workerIdToTopologyNode.contains(workerId)) {
        NES_INFO("Increase {} resources from node with id {}", amountToRelease, workerId);
        return (*workerIdToTopologyNode.at(workerId).wlock())->releaseSlots(amountToRelease);
    }
    NES_WARNING("Unable to release slots as the node with id {} do not exists.", workerId);
    return false;
}

TopologyNodeWLock Topology::lockTopologyNode(WorkerId workerId) {

    if (workerIdToTopologyNode.contains(workerId)) {
        //Try to acquire a write lock on the topology node
        auto wLock = workerIdToTopologyNode.at(workerId).tryWLock();
        if (wLock) {
            NES_DEBUG("Got the lock on the topology node {}", workerId);
            return std::make_shared<folly::Synchronized<TopologyNodePtr>::WLockedPtr>(std::move(wLock));
        }
        NES_WARNING("Unable to acquire lock on topology node with id {}", workerId);
        return nullptr;
    }
    NES_WARNING("Unable to locate topology node with id {}", workerId);
    return nullptr;
}

std::vector<TopologyNodeWLock> Topology::lockTopologyNodes(std::vector<WorkerId> workerIds) {

    std::vector<TopologyNodeWLock> lockedTopologyNodes;
    for (const auto& workerId : workerIds) {
        if (workerIdToTopologyNode.contains(workerId)) {
            //Try to acquire a write lock on the topology node
            auto wLock = workerIdToTopologyNode.at(workerId).tryWLock();
            if (!wLock) {
                NES_WARNING("Unable to acquire lock on topology node with id {}", workerId);
                return {};
            }
            NES_DEBUG("Got the lock on the topology node {}", workerId);
            lockedTopologyNodes.emplace_back(
                std::make_shared<folly::Synchronized<TopologyNodePtr>::WLockedPtr>(std::move(wLock)));
        } else {
            NES_WARNING("Unable to locate topology node with id {}", workerId);
            return {};
        }
    }
    return lockedTopologyNodes;
}

std::vector<WorkerId> Topology::getAllRegisteredNodeIds() const {

    //Compute a vector of topology node ids
    std::vector<WorkerId> topologyNodeIds;
    for (const auto& [workerId, topologyNode] : workerIdToTopologyNode) {
        topologyNodeIds.emplace_back(workerId);
    }
    return topologyNodeIds;
}

std::vector<TopologyNodePtr> Topology::findPathBetween(const std::vector<WorkerId>& sourceTopologyNodeIds,
                                                       const std::vector<WorkerId>& destinationTopologyNodeIds) {

    //Fetch the source topology nodes
    std::vector<TopologyNodePtr> sourceTopologyNodes;
    for (auto sourceTopologyNodeId : sourceTopologyNodeIds) {
        auto readLockedTopologyNode = workerIdToTopologyNode.at(sourceTopologyNodeId).tryRLock();
        if (!readLockedTopologyNode) {
            NES_WARNING("Unable to acquire read lock on the topology node {} to process the find path between request",
                        sourceTopologyNodeId);
            return {};
        }
        sourceTopologyNodes.emplace_back((*readLockedTopologyNode));
    }

    //Fetch the destination topology nodes
    std::vector<TopologyNodePtr> destinationTopologyNodes;
    for (auto destinationTopologyNodeId : destinationTopologyNodeIds) {
        auto readLockedTopologyNode = workerIdToTopologyNode.at(destinationTopologyNodeId).tryRLock();
        if (!readLockedTopologyNode) {
            NES_WARNING("Unable to acquire read lock on the topology node {} to process the find path between request",
                        destinationTopologyNodeId);
            return {};
        }
        destinationTopologyNodes.emplace_back((*readLockedTopologyNode));
    }

    NES_INFO("Topology: Finding path between set of start and destination nodes");
    std::vector<TopologyNodePtr> startNodesOfGraph;
    for (const auto& sourceTopologyNode : sourceTopologyNodes) {
        NES_TRACE("Topology: Breadth First Search for a graph");
        std::unordered_map<WorkerId, bool> visited{};
        std::unordered_map<WorkerId, uint64_t> distances{};
        std::unordered_map<WorkerId, WorkerId> prevNodes{};

        breadthFirstSearch(sourceTopologyNode, visited, distances, prevNodes);

        for (const auto& destinationNode : destinationTopologyNodes) {
            NES_TRACE("Topology: Finding all paths between the source node {} and destination node {}",
                      sourceTopologyNode->toString(),
                      destinationNode->toString());
            std::unordered_map<WorkerId, TopologyNodePtr> mapOfUniqueNodes;
            TopologyNodePtr startNodeOfGraph = createSubGraphCopy(destinationNode, visited, prevNodes, mapOfUniqueNodes);
            NES_TRACE("Topology: Push the start node of the graph into a collection of start nodes");
            startNodesOfGraph.push_back(startNodeOfGraph);
        }
    }
    NES_TRACE("Topology: Merge all found sub-graphs together to create a single sub graph and return the set of start nodes of "
              "the merged graph.");
    return mergeSubGraphs(startNodesOfGraph);
}

std::vector<std::vector<TopologyNodePtr>> Topology::findAllPathsBetween(
    const std::vector<WorkerId>& sourceTopologyNodeIds,
    const std::vector<WorkerId>& destinationTopologyNodeIds) {

    std::vector<std::vector<TopologyNodePtr>> allPaths;
    // Map from node ID to a set of levels at which the node appears
    std::unordered_map<WorkerId, std::set<int>> nodeLevels;

    // Fetch the source and destination topology nodes (same as existing code)
    std::vector<TopologyNodePtr> sourceTopologyNodes;
    for (auto sourceTopologyNodeId : sourceTopologyNodeIds) {
        auto readLockedTopologyNode = workerIdToTopologyNode.at(sourceTopologyNodeId).tryRLock();
        if (!readLockedTopologyNode) {
            NES_WARNING("Unable to acquire read lock on the topology node {} to process the find paths between request",
                        sourceTopologyNodeId);
            continue;
        }
        sourceTopologyNodes.emplace_back((*readLockedTopologyNode));
    }

    std::vector<TopologyNodePtr> destinationTopologyNodes;
    for (auto destinationTopologyNodeId : destinationTopologyNodeIds) {
        auto readLockedTopologyNode = workerIdToTopologyNode.at(destinationTopologyNodeId).tryRLock();
        if (!readLockedTopologyNode) {
            NES_WARNING("Unable to acquire read lock on the topology node {} to process the find paths between request",
                        destinationTopologyNodeId);
            continue;
        }
        destinationTopologyNodes.emplace_back((*readLockedTopologyNode));
    }

    // For each source node, find all paths to any of the destination nodes
    for (const auto& sourceNode : sourceTopologyNodes) {
        std::vector<TopologyNodePtr> currentPath;
        std::set<WorkerId> visited;
        findAllPathsDFS(sourceNode, destinationTopologyNodes, visited, currentPath, allPaths, nodeLevels);
    }

    // After finding all paths, assign alternative nodes
    assignAlternativeNodes(nodeLevels);

    return allPaths;
}

void Topology::findAllPathsDFS(const TopologyNodePtr& currentNode,
                               const std::vector<TopologyNodePtr>& destinationNodes,
                               std::set<WorkerId>& visited,
                               std::vector<TopologyNodePtr>& currentPath,
                               std::vector<std::vector<TopologyNodePtr>>& allPaths,
                               std::unordered_map<WorkerId, std::set<int>>& nodeLevels) {

    visited.insert(currentNode->getId());
    currentPath.push_back(currentNode);

    int currentLevel = currentPath.size() - 1;
    WorkerId currentNodeId = currentNode->getId();

    // Record the level of the current node
    nodeLevels[currentNodeId].insert(currentLevel);

    // Check if currentNode is a destination node
    if (std::any_of(destinationNodes.begin(), destinationNodes.end(), [&](const TopologyNodePtr& destNode) {
            return destNode->getId() == currentNodeId;
        })) {
        // Found a path; add a copy of currentPath to allPaths
        allPaths.push_back(currentPath);
        } else {
            for (const auto& parentNode : currentNode->getParents()) {
                WorkerId parentId = parentNode->as<TopologyNode>()->getId();
                if (visited.find(parentId) == visited.end()) {
                    findAllPathsDFS(parentNode->as<TopologyNode>(), destinationNodes, visited, currentPath, allPaths, nodeLevels);
                }
            }
        }

    visited.erase(currentNodeId);
    currentPath.pop_back();
}

std::vector<TopologyNodePtr>
Topology::findPathThatIncludesNode(const std::set<WorkerId>& topologyNodesWithUpStreamPinnedOperators,
                                                            const std::set<WorkerId>& topologyNodesWithDownStreamPinnedOperators,
                                                            WorkerId targetWorkerId)
{

    auto allPaths = findAllPathsBetween(std::vector(topologyNodesWithUpStreamPinnedOperators.begin(), topologyNodesWithUpStreamPinnedOperators.end()), std::vector(topologyNodesWithDownStreamPinnedOperators.begin(), topologyNodesWithDownStreamPinnedOperators.end()));

    for (auto& path : allPaths) {
        bool foundTarget = std::any_of(path.begin(), path.end(), [&](const TopologyNodePtr& node) {
            return node->getId() == targetWorkerId;
        });
        if (foundTarget) {
            return path;
        }
    }

    return {};
}

void Topology::assignAlternativeNodes(const std::unordered_map<WorkerId, std::set<int>>& nodeLevels) {

    std::unordered_map<int, std::set<WorkerId>> levelToNodeIds;

    for (const auto& [nodeId, levels] : nodeLevels) {
        for (int level : levels) {
            levelToNodeIds[level].insert(nodeId);
        }
    }

    for (const auto& [nodeId, levels] : nodeLevels) {
        TopologyNodePtr node = workerIdToTopologyNode.at(nodeId).copy();
        std::set<WorkerId> alternativeNodeIds;

        for (int level : levels) {
            const auto& nodesAtLevel = levelToNodeIds[level];
            for (WorkerId altNodeId : nodesAtLevel) {
                if (altNodeId != nodeId) {
                    alternativeNodeIds.insert(altNodeId);
                }
            }
        }

        for (const auto& id : alternativeNodeIds) {
            node->setAlternativeNodeCandidate(id);
        }


        workerIdToTopologyNode[nodeId] = node;
    }
}



bool Topology::findAllDownstreamNodes(const WorkerId& startNode,
                                      std::set<WorkerId>& reachableDownstreamNodes,
                                      std::vector<WorkerId> targetNodes) {

    std::vector<WorkerId> found;
    auto readLockedTopologyNode = workerIdToTopologyNode.at(startNode).tryRLock();
    if (!readLockedTopologyNode) {
        NES_WARNING("Unable to acquire read lock on the topology node {} to process the find path between request", startNode);
        return false;
    }

    //bfs from start node in downstream direction
    std::queue<WorkerId> queue;
    queue.push((*readLockedTopologyNode)->getId());
    while (!targetNodes.empty() && !queue.empty()) {
        const auto currentNodeId = queue.front();
        queue.pop();

        //check if this node was visited before
        if (reachableDownstreamNodes.contains(currentNodeId)) {
            continue;
        }

        //record this node as reachable downstream of the start node
        reachableDownstreamNodes.insert(currentNodeId);

        //if this node is one of the target nodes, record that it was found and remove it from the list ofr target nodes
        auto targetNodeIterator = std::find(targetNodes.begin(), targetNodes.end(), currentNodeId);
        if (targetNodeIterator != targetNodes.end()) {
            found.push_back(*targetNodeIterator);
            targetNodes.erase(targetNodeIterator);
        }

        //if all target nodes were found, return them
        if (targetNodes.empty()) {
            return true;
        }

        //lock the topology node
        auto lockedNode = workerIdToTopologyNode.at(currentNodeId).tryRLock();
        if (!lockedNode) {
            NES_WARNING("Unable to acquire read lock on the topology node {} to process the find path between request",
                        currentNodeId);
            return false;
        }

        //insert the parents of the locked node into the queue
        for (const auto& parent : (*lockedNode)->getParents()) {
            auto parentId = parent->as<TopologyNode>()->getId();
            queue.push(parentId);
        }
    }
    return true;
}

std::vector<TopologyNodePtr> Topology::mergeSubGraphs(const std::vector<TopologyNodePtr>& startNodes) {
    NES_INFO("Topology: Merge {} sub-graphs to create a single sub-graph", startNodes.size());

    NES_DEBUG("Topology: Compute a map storing number of times a node occurred in different sub-graphs");
    std::map<WorkerId, uint32_t> nodeCountMap;
    for (const auto& startNode : startNodes) {

        if (!startNode) {
            NES_ERROR("Topology: Unable to find path between source node and destination node");
            return {};
        }

        NES_TRACE("Topology: Fetch all ancestor nodes of the given start node");
        const std::vector<NodePtr> family = startNode->getAndFlattenAllAncestors();
        NES_TRACE(
            "Topology: Iterate over the family members and add the information in the node count map about the node occurrence");
        for (const auto& member : family) {
            WorkerId workerId = member->as<TopologyNode>()->getId();
            if (nodeCountMap.find(workerId) != nodeCountMap.end()) {
                NES_TRACE("Topology: Family member already present increment the occurrence count");
                uint32_t count = nodeCountMap[workerId];
                nodeCountMap[workerId] = count + 1;
            } else {
                NES_TRACE("Topology: Add family member into the node count map");
                nodeCountMap[workerId] = 1;
            }
        }
    }

    NES_DEBUG("Topology: Iterate over each sub-graph and compute a single merged sub-graph");
    std::vector<TopologyNodePtr> result;
    std::map<WorkerId, TopologyNodePtr> mergedGraphNodeMap;
    for (const auto& startNode : startNodes) {
        NES_DEBUG(
            "Topology: Check if the node already present in the new merged graph and add a copy of the node if not present");
        if (mergedGraphNodeMap.find(startNode->getId()) == mergedGraphNodeMap.end()) {
            TopologyNodePtr copyOfStartNode = startNode->copy();
            NES_DEBUG("Topology: Add the start node to the list of start nodes for the new merged graph");
            result.push_back(copyOfStartNode);
            mergedGraphNodeMap[startNode->getId()] = copyOfStartNode;
        }
        NES_DEBUG("Topology: Iterate over the ancestry of the start node and add the eligible nodes to new merged graph");
        TopologyNodePtr childNode = startNode;
        while (childNode) {
            NES_TRACE("Topology: Get all parents of the child node to select the next parent to traverse.");
            std::vector<NodePtr> parents = childNode->getParents();
            TopologyNodePtr selectedParent;
            if (parents.size() > 1) {
                NES_TRACE("Topology: Found more than one parent for the node");
                NES_TRACE("Topology: Iterate over all parents and select the parent node that has the max cost value.");
                double maxCost = 0;
                for (auto& parent : parents) {

                    NES_TRACE("Topology: Get all ancestor of the node and aggregate their occurrence counts.");
                    std::vector<NodePtr> family = parent->getAndFlattenAllAncestors();
                    double occurrenceCount = 0;
                    for (auto& member : family) {
                        occurrenceCount = occurrenceCount + nodeCountMap[member->as<TopologyNode>()->getId()];
                    }

                    NES_TRACE("Topology: Compute cost by multiplying aggregate occurrence count with base multiplier and "
                              "dividing the result by the number of nodes in the path.");
                    double cost = (occurrenceCount * BASE_MULTIPLIER) / family.size();

                    if (cost > maxCost) {
                        NES_TRACE("Topology: The cost is more than max cost found till now.");
                        if (selectedParent) {
                            NES_TRACE("Topology: Remove the previously selected parent as parent to the current child node.");
                            childNode->removeParent(selectedParent);
                        }
                        maxCost = cost;
                        NES_TRACE("Topology: Mark this parent as next selected parent.");
                        selectedParent = parent->as<TopologyNode>();
                    } else {
                        NES_TRACE("Topology: The cost is less than max cost found till now.");
                        if (selectedParent) {
                            NES_TRACE("Topology: Remove this parent as parent to the current child node.");
                            childNode->removeParent(parent);
                        }
                    }
                }
            } else if (parents.size() == 1) {
                NES_TRACE("Topology: Found only one parent for the current child node");
                NES_TRACE("Topology: Set the parent as next parent to traverse");
                selectedParent = parents[0]->as<TopologyNode>();
            }

            if (selectedParent) {
                NES_TRACE("Topology: Found a new next parent to traverse");
                if (mergedGraphNodeMap.find(selectedParent->getId()) != mergedGraphNodeMap.end()) {
                    NES_TRACE("Topology: New next parent is already present in the new merged graph.");
                    TopologyNodePtr equivalentParentNode = mergedGraphNodeMap[selectedParent->getId()];
                    TopologyNodePtr equivalentChildNode = mergedGraphNodeMap[childNode->getId()];
                    NES_TRACE("Topology: Add the existing node, with id same as new next parent, as parent to the existing node "
                              "with id same as current child node");
                    equivalentChildNode->addParent(equivalentParentNode);
                } else {
                    NES_TRACE("Topology: New next parent is not present in the new merged graph.");
                    NES_TRACE(
                        "Topology: Add copy of new next parent as parent to the existing child node in the new merged graph.");
                    TopologyNodePtr copyOfSelectedParent = selectedParent->copy();
                    TopologyNodePtr equivalentChildNode = mergedGraphNodeMap[childNode->getId()];
                    equivalentChildNode->addParent(copyOfSelectedParent);
                    mergedGraphNodeMap[selectedParent->getId()] = copyOfSelectedParent;
                }
            }
            NES_TRACE("Topology: Assign new selected parent as next child node to traverse.");
            childNode = selectedParent;
        }
    }

    return result;
}

void Topology::breadthFirstSearch(TopologyNodePtr startNode,
                                  std::unordered_map<WorkerId, bool>& visited,
                                  std::unordered_map<WorkerId, uint64_t>& distances,
                                  std::unordered_map<WorkerId, WorkerId>& prevNodes) {
    NES_INFO("Topology: make bfs starting in {}", startNode->toString());
    // queue to store nodes next to be traversed
    auto nodesQueue = std::queue<TopologyNodePtr>();
    // put start node to the queue
    nodesQueue.push(startNode);

    // map to mark visited nodes, store distances and previous nodes
    auto startNodeId = startNode->getId();
    visited[startNodeId] = true;
    prevNodes.insert(std::pair<WorkerId, WorkerId>(startNodeId, INVALID_WORKER_NODE_ID));

    while (!nodesQueue.empty()) {
        auto currNode = nodesQueue.front();
        auto currNodeId = currNode->getId();
        nodesQueue.pop();

        // next nodes to be traversed: parents and children
        std::vector<NodePtr> candidates = currNode->getParents();
        candidates.insert(candidates.end(), currNode->getChildren().begin(), currNode->getChildren().end());

        // go over candidate nodes
        for (auto& candidate : candidates) {
            auto candidateNode = candidate->as<TopologyNode>();
            auto nextNodeId = candidateNode->getId();

            // Filter out all outgoing nodes that are marked for maintenance and that are already visited, as these should be ignored during path finding
            auto search = visited.find(nextNodeId);
            if (!candidateNode->isUnderMaintenance() && (search == visited.end() || !search->second)) {
                nodesQueue.push(candidateNode);
                visited[nextNodeId] = true;
                // next node has an edge with current, so the distance will be + 1, as graph is not weighted
                distances[nextNodeId] = distances[currNodeId] + 1;
                // set previous node to decode path later
                prevNodes.insert(std::pair<WorkerId, WorkerId>(nextNodeId, currNodeId));
            }
        }
    }
}

TopologyNodePtr Topology::createSubGraphCopy(TopologyNodePtr destinationNode,
                                             std::unordered_map<WorkerId, bool>& visited,
                                             std::unordered_map<WorkerId, WorkerId>& prevNodes,
                                             std::unordered_map<WorkerId, TopologyNodePtr>& mapOfUniqueNodes) {
    NES_INFO("Topology: create sub-graph copy to {}", destinationNode->toString());
    // if destination node is not reachable, there is not path
    if (auto isVisited = visited[destinationNode->getId()]; !isVisited) {
        NES_ERROR("Topology: Unable to find path between source node and destination node {}", destinationNode->toString());
        return nullptr;
    } else {
        // copy destination node
        if (!mapOfUniqueNodes.contains(destinationNode->getId())) {
            mapOfUniqueNodes[destinationNode->getId()] = destinationNode->copy();
        }
        TopologyNodePtr currNode = destinationNode;

        try {
            // traverse path from end to the start
            WorkerId nextNodeId = prevNodes.at(currNode->getId());
            while (nextNodeId != INVALID_WORKER_NODE_ID) {
                std::vector<NodePtr> nearestNodes = currNode->getParents();
                nearestNodes.insert(nearestNodes.end(), currNode->getChildren().begin(), currNode->getChildren().end());

                // find next node by id
                auto nextNode = std::find_if(nearestNodes.begin(), nearestNodes.end(), [&](const NodePtr& searchedNode) {
                    return searchedNode->as<TopologyNode>()->getId() == nextNodeId;
                });

                if (nextNode == nearestNodes.end()) {
                    NES_ERROR("Topology: Unable to copy path between source node and destination node");
                    return nullptr;
                } else {
                    // get next node
                    auto nextTopologyNode = nextNode->get()->as<TopologyNode>();
                    // copy next node
                    if (mapOfUniqueNodes.find(nextTopologyNode->getId()) == mapOfUniqueNodes.end()) {
                        mapOfUniqueNodes[nextTopologyNode->getId()] = nextTopologyNode->copy();
                    }
                    // add parent link to copies
                    mapOfUniqueNodes[nextTopologyNode->getId()]->addParent(mapOfUniqueNodes[currNode->getId()]);

                    currNode = nextTopologyNode;
                    nextNodeId = prevNodes.at(currNode->getId());
                }
            }

            return mapOfUniqueNodes[currNode->getId()];
        } catch (const std::out_of_range& ex) {
            NES_WARNING("No path found")
        }
    }
    return nullptr;
}

TopologyNodePtr Topology::depthFirstSearchOnlyParents(TopologyNodePtr testNode,
                                                      std::vector<TopologyNodePtr> searchedNodes,
                                                      std::unordered_map<WorkerId, TopologyNodePtr>& uniqueNodes) {

    NES_TRACE("Topology: check if test node is one of the searched node");
    auto found = std::find_if(searchedNodes.begin(), searchedNodes.end(), [&](const TopologyNodePtr& searchedNode) {
        return searchedNode->getId() == testNode->getId();
    });

    if (found != searchedNodes.end()) {
        NES_DEBUG("Topology: found the destination node");
        if (!uniqueNodes.contains(testNode->getId())) {
            NES_TRACE("Topology: Insert the information about the test node in the unique node map");
            const TopologyNodePtr copyOfTestNode = testNode->copy();
            uniqueNodes[testNode->getId()] = copyOfTestNode;
        }
        NES_TRACE("Topology: Return start of sub-graph copy");
        return uniqueNodes[testNode->getId()];
    }

    std::vector<NodePtr> parents = testNode->getParents();
    std::vector<NodePtr> updatedParents;
    //filters out all parents that are marked for maintenance, as these should be ignored during path finding
    for (auto& parent : parents) {
        if (!parent->as<TopologyNode>()->isUnderMaintenance()) {
            updatedParents.push_back(parent);
        }
    }

    if (updatedParents.empty()) {
        NES_WARNING("Topology: reached end of the tree but destination node not found.");
        return nullptr;
    }

    TopologyNodePtr foundNode = nullptr;
    for (auto& parent : updatedParents) {
        TopologyNodePtr foundInParent = depthFirstSearchOnlyParents(parent->as<TopologyNode>(), searchedNodes, uniqueNodes);
        if (foundInParent) {
            NES_TRACE("Topology: found the destination node as the parent of the physical node.");
            if (!foundNode) {
                if (uniqueNodes.find(testNode->getId()) == uniqueNodes.end()) {
                    const TopologyNodePtr copyOfTestNode = testNode->copy();
                    uniqueNodes[testNode->getId()] = copyOfTestNode;
                }
                foundNode = uniqueNodes[testNode->getId()];
            }
            NES_TRACE("Topology: Adding found node as parent to the copy of testNode.");
            foundNode->addParent(foundInParent);
        }
    }
    return foundNode;
}

nlohmann::json Topology::toJson() {

    nlohmann::json topologyJson{};

    if (rootWorkerIds.empty()) {
        NES_WARNING("No root node found");
        return topologyJson;
    }

    auto lockedLocationIndex = locationIndex.wlock();
    std::vector<nlohmann::json> nodes = {};
    std::vector<nlohmann::json> edges = {};

    std::deque<TopologyNodePtr> nodesToProcess{};
    for (const auto& rootWorkerId : rootWorkerIds) {
        auto rootNode = workerIdToTopologyNode.at(rootWorkerId).rlock();
        nodesToProcess.emplace_back((*rootNode));
    }

    std::set<WorkerId> visitedNodes;
    std::deque<TopologyNodePtr> childrenToProcess;
    while (!nodesToProcess.empty()) {
        // Current topology node to add to the JSON
        TopologyNodePtr currentNode = nodesToProcess.front();
        nlohmann::json currentNodeJsonValue{};

        nodesToProcess.pop_front();
        // Add properties for current topology node
        currentNodeJsonValue["id"] = currentNode->getId();
        currentNodeJsonValue["available_resources"] = currentNode->getAvailableResources();
        currentNodeJsonValue["ip_address"] = currentNode->getIpAddress();
        if (currentNode->getSpatialNodeType() != NES::Spatial::Experimental::SpatialType::MOBILE_NODE) {
            auto geoLocation = (*lockedLocationIndex)->getGeoLocationForNode(currentNode->getId());
            auto locationInfo = nlohmann::json{};
            if (geoLocation.has_value() && geoLocation.value().isValid()) {
                locationInfo["latitude"] = geoLocation.value().getLatitude();
                locationInfo["longitude"] = geoLocation.value().getLongitude();
            }
            currentNodeJsonValue["location"] = locationInfo;
        }
        currentNodeJsonValue["nodeType"] = Spatial::Util::SpatialTypeUtility::toString(currentNode->getSpatialNodeType());

        auto children = currentNode->getChildren();
        for (const auto& child : children) {
            // Add edge information for current topology node
            nlohmann::json currentEdgeJsonValue{};
            currentEdgeJsonValue["source"] = child->as<TopologyNode>()->getId();
            currentEdgeJsonValue["target"] = currentNode->getId();
            edges.push_back(currentEdgeJsonValue);
            // Record the child for further processing
            childrenToProcess.push_back(child->as<TopologyNode>());
        }

        // Once all parent nodes are processed, we add the observed children nodes to be the next processing queue
        if (nodesToProcess.empty()) {
            for (const auto& childToProcess : childrenToProcess) {
                WorkerId childWorkerId = childToProcess->getId();
                if (!visitedNodes.contains(childWorkerId)) {
                    nodesToProcess.emplace_back(childToProcess);
                    visitedNodes.emplace(childWorkerId);
                }
            }
            // Clear the children queue
            childrenToProcess.clear();
        }
        nodes.push_back(currentNodeJsonValue);
    }

    NES_INFO("TopologyController: no more topology node to add");
    // add `nodes` and `edges` JSON array to the final JSON result
    topologyJson["nodes"] = nodes;
    topologyJson["edges"] = edges;
    return topologyJson;
}

std::string Topology::toString() {

    if (rootWorkerIds.empty()) {
        NES_WARNING("No root node found");
        return "";
    }

    std::stringstream topologyInfo;
    topologyInfo << std::endl;

    // store pair of TopologyNodePtr and its depth in when printed
    std::deque<std::pair<TopologyNodePtr, uint64_t>> parentToPrint;
    for (const auto& rootWorkerId : rootWorkerIds) {
        auto root = workerIdToTopologyNode.at(rootWorkerId).rlock();
        parentToPrint.emplace_back(std::make_pair((*root), 0));
    }

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

//TODO #2498 add functions here, that do not only search in a circular area, but make sure, that there are nodes found in every possible direction of future movement
std::vector<std::pair<WorkerId, NES::Spatial::DataTypes::Experimental::GeoLocation>>
Topology::getTopologyNodeIdsInRange(NES::Spatial::DataTypes::Experimental::GeoLocation center, double radius) {
    auto lockedLocationIndex = locationIndex.wlock();
    return (*lockedLocationIndex)->getNodeIdsInRange(center, radius);
}

bool Topology::addGeoLocation(WorkerId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {

    if (!workerIdToTopologyNode.contains(workerId)) {
        NES_ERROR("Unable to find node with id {}", workerId);
        return false;
    }

    auto lockedTopologyNode = workerIdToTopologyNode.at(workerId).wlock();

    if (geoLocation.isValid()
        && (*lockedTopologyNode)->getSpatialNodeType() == Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        NES_DEBUG("added node with geographical location: {}, {}", geoLocation.getLatitude(), geoLocation.getLongitude());
        auto lockedLocationIndex = locationIndex.wlock();
        (*lockedLocationIndex)->initializeFieldNodeCoordinates(workerId, std::move(geoLocation));
    } else {
        NES_DEBUG("added node is a non field node");
        if ((*lockedTopologyNode)->getSpatialNodeType() == Spatial::Experimental::SpatialType::MOBILE_NODE) {
            auto lockedLocationIndex = locationIndex.wlock();
            (*lockedLocationIndex)->addMobileNode(workerId, std::move(geoLocation));
            NES_DEBUG("added node is a mobile node");
        } else {
            NES_DEBUG("added node is a non mobile node");
        }
    }
    return true;
}

bool Topology::updateGeoLocation(WorkerId workerId, NES::Spatial::DataTypes::Experimental::GeoLocation&& geoLocation) {

    if (!workerIdToTopologyNode.contains(workerId)) {
        NES_ERROR("Unable to find node with id {}", workerId);
        return false;
    }

    auto lockedTopologyNode = workerIdToTopologyNode.at(workerId).wlock();

    if (geoLocation.isValid()
        && (*lockedTopologyNode)->getSpatialNodeType() == Spatial::Experimental::SpatialType::FIXED_LOCATION) {
        NES_DEBUG("added node with geographical location: {}, {}", geoLocation.getLatitude(), geoLocation.getLongitude());
        auto lockedLocationIndex = locationIndex.wlock();
        (*lockedLocationIndex)->updateFieldNodeCoordinates(workerId, std::move(geoLocation));
    } else {
        NES_DEBUG("added node is a non field node");
        if ((*lockedTopologyNode)->getSpatialNodeType() == Spatial::Experimental::SpatialType::MOBILE_NODE) {
            auto lockedLocationIndex = locationIndex.wlock();
            (*lockedLocationIndex)->addMobileNode(workerId, std::move(geoLocation));
            NES_DEBUG("added node is a mobile node");
        } else {
            NES_DEBUG("added node is a non mobile node");
        }
    }
    return true;
}

Spatial::Experimental::SpatialType Topology::getSpatialType(WorkerId workerId) {
    if (!workerIdToTopologyNode.contains(workerId)) {
        NES_ERROR("Unable to find node with id {}", workerId);
        return Spatial::Experimental::SpatialType::INVALID;
    }
    auto lockedTopologyNode = workerIdToTopologyNode.at(workerId).wlock();
    return (*lockedTopologyNode)->getSpatialNodeType();
}

std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation> Topology::getGeoLocationForNode(WorkerId nodeId) {
    auto lockedLocationIndex = locationIndex.wlock();
    return (*lockedLocationIndex)->getGeoLocationForNode(nodeId);
}

void Topology::getElegantPayload(nlohmann::json& payload) {

    NES_DEBUG("Getting the json representation of available nodes");
    auto lockedLocationIndex = locationIndex.wlock();
    std::vector<nlohmann::json> nodes = {};
    std::vector<nlohmann::json> edges = {};

    std::deque<TopologyNodePtr> nodesToProcess{};
    for (const auto& rootWorkerId : rootWorkerIds) {
        auto rootNode = workerIdToTopologyNode.at(rootWorkerId).rlock();
        nodesToProcess.emplace_back((*rootNode));
    }

    std::set<WorkerId> visitedNodes;
    std::deque<TopologyNodePtr> childrenToProcess;
    while (!nodesToProcess.empty()) {
        // Current topology node to add to the JSON
        TopologyNodePtr currentNode = nodesToProcess.front();
        nlohmann::json currentNodeJsonValue{};
        nodesToProcess.pop_front();

        // Add properties for current topology node
        currentNodeJsonValue[NODE_ID_KEY] = currentNode->getId();
        currentNodeJsonValue[NODE_TYPE_KEY] = "stationary";// always set to stationary
        currentNodeJsonValue[DEVICES_KEY] = std::any_cast<std::vector<NES::Runtime::OpenCLDeviceInfo>>(
            currentNode->getNodeProperty(NES::Worker::Configuration::OPENCL_DEVICES));

        auto children = currentNode->getChildren();
        for (const auto& child : children) {
            // Add edge information for current topology node
            nlohmann::json currentEdgeJsonValue{};
            WorkerId childNodeId = child->as<TopologyNode>()->getId();
            currentEdgeJsonValue[LINK_ID_KEY] = fmt::format("{}-{}", childNodeId, currentNode->getId());
            auto linkProperty = currentNode->getLinkProperty(childNodeId);
            currentEdgeJsonValue[TRANSFER_RATE_KEY] = std::to_string(linkProperty->bandwidth);
            edges.push_back(currentEdgeJsonValue);
            // Record the child for further processing
            childrenToProcess.push_back(child->as<TopologyNode>());
        }

        // Once all parent nodes are processed, we add the observed children nodes to be the next processing queue
        if (nodesToProcess.empty()) {
            for (const auto& childToProcess : childrenToProcess) {
                WorkerId childWorkerId = childToProcess->getId();
                if (!visitedNodes.contains(childWorkerId)) {
                    nodesToProcess.emplace_back(childToProcess);
                    visitedNodes.emplace(childWorkerId);
                }
            }
            // Clear the children queue
            childrenToProcess.clear();
        }

        nodes.push_back(currentNodeJsonValue);
    }
    NES_INFO("no more topology nodes to add");

    payload[AVAILABLE_NODES_KEY] = nodes;
    payload[NETWORK_DELAYS_KEY] = edges;
}

nlohmann::json Topology::requestNodeLocationDataAsJson(WorkerId workerId) {

    if (!nodeWithWorkerIdExists(workerId)) {
        return nullptr;
    }
    auto lockedLocationIndex = locationIndex.wlock();
    auto geoLocation = (*lockedLocationIndex)->getGeoLocationForNode(workerId);
    if (geoLocation.has_value()) {
        return convertNodeLocationInfoToJson(workerId, geoLocation.value());
    } else {
        nlohmann::json nodeInfo;
        nodeInfo["id"] = workerId;
        return nodeInfo;
    }
}

nlohmann::json Topology::requestLocationAndParentDataFromAllMobileNodes() {

    auto lockedLocationIndex = locationIndex.wlock();
    auto nodeVector = (*lockedLocationIndex)->getAllNodeLocations();
    auto locationMapJson = nlohmann::json::array();
    auto mobileEdgesJson = nlohmann::json::array();
    uint32_t count = 0;
    uint32_t edgeCount = 0;

    for (const auto& [sourceWorkerId, location] : nodeVector) {
        const auto& lockedTopologyNode = workerIdToTopologyNode.at(sourceWorkerId).rlock();
        auto topologyNode = (*lockedTopologyNode);
        if (topologyNode && topologyNode->getSpatialNodeType() == Spatial::Experimental::SpatialType::MOBILE_NODE) {
            nlohmann::json nodeInfo = convertNodeLocationInfoToJson(sourceWorkerId, location);
            locationMapJson[count] = nodeInfo;
            for (const auto& parent : topologyNode->getParents()) {
                const auto& parentWorkerId = parent->as<TopologyNode>()->getId();
                auto linkProperty = topologyNode->getLinkProperty(parentWorkerId);
                if (linkProperty) {
                    const nlohmann::json edge{{"source", sourceWorkerId},
                                              {"target", parentWorkerId},
                                              {"bandwidth", linkProperty->bandwidth},
                                              {"latency", linkProperty->latency}};
                    mobileEdgesJson[edgeCount] = edge;
                } else {
                    const nlohmann::json edge{{"source", sourceWorkerId}, {"target", parentWorkerId}};
                    mobileEdgesJson[edgeCount] = edge;
                }
                ++edgeCount;
            }
            ++count;
        }
    }
    nlohmann::json response;
    response["nodes"] = locationMapJson;
    response["edges"] = mobileEdgesJson;
    return response;
}

nlohmann::json Topology::convertLocationToJson(NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation) {
    nlohmann::json locJson;
    if (geoLocation.isValid()) {
        locJson["latitude"] = geoLocation.getLatitude();
        locJson["longitude"] = geoLocation.getLongitude();
    }
    return locJson;
}

nlohmann::json Topology::convertNodeLocationInfoToJson(WorkerId workerId,
                                                       NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation) {
    nlohmann::json nodeInfo;
    nodeInfo["id"] = workerId;
    nlohmann::json locJson = convertLocationToJson(std::move(geoLocation));
    nodeInfo["location"] = locJson;
    return nodeInfo;
}

WorkerId Topology::getNextWorkerId() { return WorkerId(topologyNodeIdNextIndex++); }
void Topology::setNeighborUpdateCallback(std::function<void(WorkerId, std::string, std::vector<std::pair<WorkerId, std::string>> neighborInfo)> callback) {
    neighborUpdateCallback = std::move(callback);
}

void Topology::informNeighborsOfNode(WorkerId nodeId, std::string targetAddress) {
    if (!neighborUpdateCallback) {
        NES_DEBUG("No neighbor update callback set, skipping informNeighborsOfNode for {}", nodeId);
        return;
    }

    std::vector<std::pair<WorkerId, std::string>> neighborInfo;

    auto parentIds = getParentTopologyNodeIds(nodeId);
    auto childIds = getChildTopologyNodeIds(nodeId);
    auto addNeighbor = [&](WorkerId wid) {
        auto node = getCopyOfTopologyNodeWithId(wid);
        if (node) {
            neighborInfo.emplace_back(wid, node->getFullRPCAddress());
        }
    };

    for (auto pid : parentIds) {
        addNeighbor(pid);
    }
    for (auto cid : childIds) {
        addNeighbor(cid);
    }
    neighborUpdateCallback(nodeId, targetAddress, neighborInfo);
}

WorkerId Topology::getNextWorkerId() { return WorkerId(topologyNodeIdCounter++); }
}// namespace NES
