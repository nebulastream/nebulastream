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

#include <Catalogs/Topology/TopologyNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <algorithm>

namespace NES {

GlobalExecutionPlanPtr GlobalExecutionPlan::create() { return std::make_shared<GlobalExecutionPlan>(); }

bool GlobalExecutionPlan::checkIfExecutionNodeExists(ExecutionNodeId id) {
    NES_DEBUG("Checking if Execution node with id {} exists", id);
    return executionNodeIdToExecutionNodeMap.find(id) != executionNodeIdToExecutionNodeMap.end();
}

ExecutionNodePtr GlobalExecutionPlan::getExecutionNodeById(ExecutionNodeId id) {
    if (checkIfExecutionNodeExists(id)) {
        NES_DEBUG("Returning execution node with id  {}", id);
        return executionNodeIdToExecutionNodeMap[id];
    }
    NES_WARNING("Execution node doesn't exists with the id {}", id);
    return nullptr;
}

bool GlobalExecutionPlan::addExecutionNodeAsRoot(const ExecutionNodePtr& executionNode) {
    NES_DEBUG("Added Execution node as root node");
    auto found = std::find(rootNodes.begin(), rootNodes.end(), executionNode);
    if (found == rootNodes.end()) {
        rootNodes.push_back(executionNode);
        NES_DEBUG("Added Execution node with id  {}", executionNode->getId());
        executionNodeIdToExecutionNodeMap[executionNode->getId()] = executionNode;
    } else {
        NES_WARNING("Execution node already present in the root node list");
    }
    return true;
}

bool GlobalExecutionPlan::addExecutionNode(const ExecutionNodePtr& executionNode) {
    NES_DEBUG("Added Execution node with id  {}", executionNode->getId());
    auto topologyNode = executionNode->getTopologyNode();
    // Add child execution nodes
    for (const auto& childTopologyNode : topologyNode->getChildren()) {
        auto childExecutionNode = getExecutionNodeById(childTopologyNode->as<TopologyNode>()->getId());
        if (childExecutionNode) {
            executionNode->addChild(childExecutionNode);
        }
    }

    //Add as root execution node
    if (topologyNode->getParents().empty()) {
        addExecutionNodeAsRoot(executionNode);
    } else {
        // Add parent execution nodes
        for (const auto& parentTopologyNode : topologyNode->getParents()) {
            auto parentExecutionNode = getExecutionNodeById(parentTopologyNode->as<TopologyNode>()->getId());
            if (parentExecutionNode) {
                executionNode->addParent(parentExecutionNode);
            }
        }
    }
    executionNodeIdToExecutionNodeMap[executionNode->getId()] = executionNode;
    scheduleExecutionNode(executionNode);
    return true;
}

bool GlobalExecutionPlan::removeExecutionNode(ExecutionNodeId id) {
    NES_DEBUG("Removing Execution node with id  {}", id);
    if (checkIfExecutionNodeExists(id)) {
        NES_DEBUG("Removed execution node with id  {}", id);
        auto found = std::find_if(rootNodes.begin(), rootNodes.end(), [id](const ExecutionNodePtr& rootNode) {
            return rootNode->getId() == id;
        });
        if (found != rootNodes.end()) {
            rootNodes.erase(found);
        }
        return executionNodeIdToExecutionNodeMap.erase(id) == 1;
    }
    NES_DEBUG("Failed to remove Execution node with id  {}", id);
    return false;
}

bool GlobalExecutionPlan::removeQuerySubPlans(SharedQueryId sharedQueryId) {
    if (!sharedQueryIdToExecutionNodeMap.contains(sharedQueryId)) {
        NES_DEBUG("No query with id {} exists in the system", sharedQueryId);
        return false;
    }

    std::vector<ExecutionNodePtr> executionNodes = sharedQueryIdToExecutionNodeMap[sharedQueryId];
    NES_DEBUG("Found {} Execution node for shared query with id {}", executionNodes.size(), sharedQueryId);
    for (const auto& executionNode : executionNodes) {
        uint64_t executionNodeId = executionNode->getId();
        if (!executionNode->removeQuerySubPlans(sharedQueryId)) {
            NES_ERROR("Unable to remove query sub plan with id {} from execution node with id {}",
                      sharedQueryId,
                      executionNodeId);
            return false;
        }
        if (executionNode->getAllQuerySubPlans().empty()) {
            removeExecutionNode(executionNodeId);
        }
    }
    sharedQueryIdToExecutionNodeMap.erase(sharedQueryId);
    NES_DEBUG("Removed all Execution nodes for the shared query with id {}", sharedQueryId);
    return true;
}

std::vector<ExecutionNodePtr> GlobalExecutionPlan::getExecutionNodesByQueryId(SharedQueryId sharedQueryId) {

    if (sharedQueryIdToExecutionNodeMap.contains(sharedQueryId)) {
        NES_DEBUG("Returning vector of Execution nodes for the shared query with id  {}", sharedQueryId);
        return sharedQueryIdToExecutionNodeMap[sharedQueryId];
    }
    NES_WARNING("unable to find the Execution nodes for the shared query with id {}", sharedQueryId);
    return {};
}

bool GlobalExecutionPlan::removeQuerySubPlanFromNode(ExecutionNodeId executionNodeId,
                                                     SharedQueryId sharedQueryId,
                                                     DecomposedQueryPlanId subPlanId) {
    auto nodeIterator = executionNodeIdToExecutionNodeMap.find(executionNodeId);

    //return false if no node with the given id could be found
    if (nodeIterator == executionNodeIdToExecutionNodeMap.end()) {
        return false;
    }
    auto executionNode = nodeIterator->second;

    //return false if no query sub plan with the given id was found at the node
    if (!executionNode->removeQuerySubPlan(sharedQueryId, subPlanId)) {
        return false;
    }

    /* Check if the node still hosts query sub plans belonging the shared query with the given id. If not, remove
     * the node from the vector of nodes associated with this shared query*/
    if (executionNode->getQuerySubPlans(sharedQueryId).empty()) {
        auto& mappedNodes = sharedQueryIdToExecutionNodeMap[sharedQueryId];
        if (mappedNodes.size() == 1) {
            /* if this was the only node associated with this shared query id, remove the entry for this shared query
             * from the index */
            sharedQueryIdToExecutionNodeMap.erase(sharedQueryId);
        } else {
            /* if other nodes are still hosting sub queries of this shared query, remove only this node, from the list
             * of nodes which host sub query plans of this shared query */
            mappedNodes.erase(std::find(mappedNodes.begin(), mappedNodes.end(), executionNode));
        }
    }

    // if the node does not host any query sub plans anymore, remove it
    if (executionNode->getAllQuerySubPlans().empty()) {
        removeExecutionNode(executionNodeId);
    }
    return true;
}

std::string GlobalExecutionPlan::getAsString() {
    NES_DEBUG("Get Execution plan as string");
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler::create(ss);
    for (const auto& rootNode : rootNodes) {
        dumpHandler->multilineDump(rootNode);
    }
    return ss.str();
}

void GlobalExecutionPlan::scheduleExecutionNode(const ExecutionNodePtr& executionNode) {
    NES_DEBUG("Schedule execution node for deployment");
    auto found = std::find(executionNodesToSchedule.begin(), executionNodesToSchedule.end(), executionNode);
    if (found != executionNodesToSchedule.end()) {
        NES_DEBUG("Execution node {} marked as to be scheduled", executionNode->getId());
        executionNodesToSchedule.push_back(executionNode);
    } else {
        NES_WARNING("Execution node {} already scheduled", executionNode->getId());
    }
    mapExecutionNodeToSharedQueryId(executionNode);
}

void GlobalExecutionPlan::mapExecutionNodeToSharedQueryId(const ExecutionNodePtr& executionNode) {
    NES_DEBUG("Mapping execution node {} to the query Id index.", executionNode->getId());
    auto querySubPlanMap = executionNode->getAllQuerySubPlans();
    for (const auto& [sharedQueryId, querySubPlans] : querySubPlanMap) {
        if (!sharedQueryIdToExecutionNodeMap.contains(sharedQueryId)) {
            NES_DEBUG("Query Id {} does not exists adding a new entry with execution node {}",
                      sharedQueryId,
                      executionNode->getId());
            sharedQueryIdToExecutionNodeMap[sharedQueryId] = {executionNode};
        } else {
            std::vector<ExecutionNodePtr> executionNodes = sharedQueryIdToExecutionNodeMap[sharedQueryId];
            auto found = std::find(executionNodes.begin(), executionNodes.end(), executionNode);
            if (found == executionNodes.end()) {
                NES_DEBUG("Adding execution node {} to the query Id {}", executionNode->getId(), sharedQueryId);
                executionNodes.push_back(executionNode);
                sharedQueryIdToExecutionNodeMap[sharedQueryId] = executionNodes;
            } else {
                NES_DEBUG("Skipping as execution node {} already mapped to the query Id {}",
                          executionNode->getId(),
                          sharedQueryId);
            }
        }
    }
}

std::map<WorkerId, uint32_t> GlobalExecutionPlan::getMapOfWorkerIdToOccupiedResource(SharedQueryId sharedQueryId) {

    NES_INFO("Get a map of occupied resources for the shared query {}", sharedQueryId);
    std::map<WorkerId, uint32_t> mapOfWorkerIdToOccupiedResources;
    std::vector<ExecutionNodePtr> executionNodes = sharedQueryIdToExecutionNodeMap[sharedQueryId];
    NES_DEBUG("Found {} Execution node for the shared query with id {}", executionNodes.size(), sharedQueryId);
    for (auto& executionNode : executionNodes) {
        uint32_t occupiedResource = executionNode->getOccupiedResources(sharedQueryId);
        mapOfWorkerIdToOccupiedResources[executionNode->getTopologyNode()->getId()] = occupiedResource;
    }
    NES_DEBUG("returning the map of occupied resources for the shared query  {}", sharedQueryId);
    return mapOfWorkerIdToOccupiedResources;
}

}// namespace NES
