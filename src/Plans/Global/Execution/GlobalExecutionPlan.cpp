#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

namespace NES {
GlobalExecutionPlan::GlobalExecutionPlan() {
    NES_DEBUG("GlobalExecutionPlan()");
}

GlobalExecutionPlan::~GlobalExecutionPlan() {
    NES_DEBUG("~GlobalExecutionPlan()");
}

GlobalExecutionPlanPtr GlobalExecutionPlan::create() {
    return std::make_shared<GlobalExecutionPlan>(GlobalExecutionPlan());
}

bool GlobalExecutionPlan::checkIfExecutionNodeExists(uint64_t id) {
    NES_DEBUG("GlobalExecutionPlan: Checking if Execution node with id " << id << " exists");
    return nodeIdIndex.find(id) != nodeIdIndex.end();
}

ExecutionNodePtr GlobalExecutionPlan::getExecutionNodeByNodeId(uint64_t id) {
    if (checkIfExecutionNodeExists(id)) {
        NES_DEBUG("GlobalExecutionPlan: Returning execution node with id " << id);
        return nodeIdIndex[id];
    }
    NES_WARNING("GlobalExecutionPlan: Execution node doesn't exists with the id " << id);
    return nullptr;
}

bool GlobalExecutionPlan::addExecutionNodeAsParentTo(uint64_t childId, ExecutionNodePtr parentExecutionNode) {
    ExecutionNodePtr childNode = getExecutionNodeByNodeId(childId);
    if (childNode) {
        NES_DEBUG("GlobalExecutionPlan: Adding Execution node as parent to the execution node with id " << childId);
        if (childNode->containAsParent(parentExecutionNode)) {
            NES_DEBUG("GlobalExecutionPlan: Execution node is already a parent to the node with id " << childId);
            return true;
        }

        if (childNode->addParent(parentExecutionNode)) {
            NES_DEBUG("GlobalExecutionPlan: Added Execution node with id " << parentExecutionNode->getId());
            nodeIdIndex[parentExecutionNode->getId()] = parentExecutionNode;
            return true;
        }
        NES_WARNING("GlobalExecutionPlan: Failed to add Execution node as parent to the execution node with id " << childId);
        return false;
    }
    NES_WARNING("GlobalExecutionPlan: Child node doesn't exists with the id " << childId);
    return false;
}

bool GlobalExecutionPlan::addExecutionNodeAsRoot(ExecutionNodePtr executionNode) {
    NES_DEBUG("GlobalExecutionPlan: Added Execution node as root node");
    auto found = std::find(rootNodes.begin(), rootNodes.end(), executionNode);
    if (found == rootNodes.end()) {
        rootNodes.push_back(executionNode);
        NES_DEBUG("GlobalExecutionPlan: Added Execution node with id " << executionNode->getId());
        nodeIdIndex[executionNode->getId()] = executionNode;
    } else {
        NES_WARNING("GlobalExecutionPlan: Execution node already present in the root node list");
    }
    return true;
}

bool GlobalExecutionPlan::addExecutionNode(ExecutionNodePtr executionNode) {
    NES_DEBUG("GlobalExecutionPlan: Added Execution node with id " << executionNode->getId());
    nodeIdIndex[executionNode->getId()] = executionNode;
    scheduleExecutionNode(executionNode);
    return true;
}

bool GlobalExecutionPlan::removeExecutionNode(uint64_t id) {
    NES_DEBUG("GlobalExecutionPlan: Removing Execution node with id " << id);
    if (checkIfExecutionNodeExists(id)) {
        NES_DEBUG("GlobalExecutionPlan: Removed execution node with id " << id);
        return nodeIdIndex.erase(id) == 1;
    }
    NES_DEBUG("GlobalExecutionPlan: Failed to remove Execution node with id " << id);
    return false;
}

bool GlobalExecutionPlan::removeQuerySubPlans(QueryId queryId) {
    auto itr = queryIdIndex.find(queryId);
    if (itr == queryIdIndex.end()) {
        NES_WARNING("GlobalExecutionPlan: No query with id " << queryId << " exists in the system");
        return false;
    }

    std::vector<ExecutionNodePtr> executionNodes = queryIdIndex[queryId];
    NES_DEBUG("GlobalExecutionPlan: Found " << executionNodes.size() << " Execution node for query with id " << queryId);
    for (auto executionNode : executionNodes) {
        uint64_t executionNodeId = executionNode->getId();
        if (!executionNode->removeQuerySubPlans(queryId)) {
            NES_ERROR("GlobalExecutionPlan: Unable to remove query sub plan with id " << queryId << " from execution node with id " << executionNodeId);
            return false;
        }
        if (executionNode->getAllQuerySubPlans().empty()) {
            removeExecutionNode(executionNodeId);
        }
    }
    queryIdIndex.erase(queryId);
    NES_DEBUG("GlobalExecutionPlan: Removed all Execution nodes for Query with id " << queryId);
    return true;
}

std::vector<ExecutionNodePtr> GlobalExecutionPlan::getExecutionNodesByQueryId(QueryId queryId) {
    auto itr = queryIdIndex.find(queryId);
    if (itr != queryIdIndex.end()) {
        NES_DEBUG("GlobalExecutionPlan: Returning vector of Execution nodes for the query with id " << queryId);
        return itr->second;
    }
    NES_WARNING("GlobalExecutionPlan: unable to find the Execution nodes for the query with id " << queryId);
    return {};
}

std::vector<ExecutionNodePtr> GlobalExecutionPlan::getAllExecutionNodes() {
    NES_INFO("GlobalExecutionPlan: get all execution nodes");
    std::vector<ExecutionNodePtr> executionNodes;
    for (auto& [nodeId, executionNode] : nodeIdIndex) {
        executionNodes.push_back(executionNode);
    }
    return executionNodes;
}

std::vector<ExecutionNodePtr> GlobalExecutionPlan::getExecutionNodesToSchedule() {
    NES_DEBUG("GlobalExecutionPlan: Returning vector of Execution nodes to be scheduled");
    return executionNodesToSchedule;
}

std::vector<ExecutionNodePtr> GlobalExecutionPlan::getRootNodes() {
    NES_DEBUG("GlobalExecutionPlan: Get root nodes of the execution plan");
    return rootNodes;
}

std::string GlobalExecutionPlan::getAsString() {
    NES_DEBUG("GlobalExecutionPlan: Get Execution plan as string");
    std::stringstream ss;
    auto dumpHandler = ConsoleDumpHandler::create();
    for (auto rootNode : rootNodes) {
        dumpHandler->multilineDump(rootNode, ss);
    }
    return ss.str();
}

void GlobalExecutionPlan::scheduleExecutionNode(ExecutionNodePtr executionNode) {
    NES_DEBUG("GlobalExecutionPlan: Schedule execution node for deployment");
    auto found = std::find(executionNodesToSchedule.begin(), executionNodesToSchedule.end(), executionNode);
    if (found != executionNodesToSchedule.end()) {
        NES_DEBUG("GlobalExecutionPlan: Execution node " << executionNode->getId() << " marked as to be scheduled");
        executionNodesToSchedule.push_back(executionNode);
    } else {
        NES_WARNING("GlobalExecutionPlan: Execution node " << executionNode->getId() << " already scheduled");
    }
    mapExecutionNodeToQueryId(executionNode);
}

void GlobalExecutionPlan::mapExecutionNodeToQueryId(ExecutionNodePtr executionNode) {
    NES_DEBUG("GlobalExecutionPlan: Mapping execution node " << executionNode->getId() << " to the query Id index.");
    auto querySubPlans = executionNode->getAllQuerySubPlans();
    for (auto pair : querySubPlans) {
        QueryId queryId = pair.first;
        if (queryIdIndex.find(queryId) == queryIdIndex.end()) {
            NES_DEBUG("GlobalExecutionPlan: Query Id " << queryId << " does not exists adding a new entry with execution node " << executionNode->getId());
            queryIdIndex[queryId] = {executionNode};
        } else {
            std::vector<ExecutionNodePtr> executionNodes = queryIdIndex[queryId];
            auto found = std::find(executionNodes.begin(), executionNodes.end(), executionNode);
            if (found == executionNodes.end()) {
                NES_DEBUG("GlobalExecutionPlan: Adding execution node " << executionNode->getId() << " to the query Id " << queryId);
                executionNodes.push_back(executionNode);
                queryIdIndex[queryId] = executionNodes;
            } else {
                NES_DEBUG("GlobalExecutionPlan: Skipping as execution node " << executionNode->getId() << " already mapped to the query Id " << queryId);
            }
        }
    }
}

std::map<uint64_t, uint32_t> GlobalExecutionPlan::getMapOfTopologyNodeIdToOccupiedResource(QueryId queryId) {

    NES_INFO("GlobalExecutionPlan: Get a map of occupied resources for the query " << queryId);
    std::map<uint64_t, uint32_t> mapOfTopologyNodeIdToOccupiedResources;
    std::vector<ExecutionNodePtr> executionNodes = queryIdIndex[queryId];
    NES_DEBUG("GlobalExecutionPlan: Found " << executionNodes.size() << " Execution node for query with id " << queryId);
    for (auto& executionNode : executionNodes) {
        uint32_t occupiedResource = executionNode->getOccupiedResources(queryId);
        mapOfTopologyNodeIdToOccupiedResources[executionNode->getTopologyNode()->getId()] = occupiedResource;
    }
    NES_DEBUG("GlobalExecutionPlan: returning the map of occupied resources for the query " << queryId);
    return mapOfTopologyNodeIdToOccupiedResources;
}

}// namespace NES
