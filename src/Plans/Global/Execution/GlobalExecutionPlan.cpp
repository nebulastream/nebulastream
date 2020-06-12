#include <Nodes/Util/ConsoleDumpHandler.hpp>
#include <Nodes/Util/DumpContext.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Util/Logger.hpp>

namespace NES {

GlobalExecutionPlanPtr GlobalExecutionPlan::create() {
    return std::make_shared<GlobalExecutionPlan>(GlobalExecutionPlan());
}

bool GlobalExecutionPlan::executionNodeExists(uint64_t id) {
    return nodeIdIndex.find(id) != nodeIdIndex.end();
}

ExecutionNodePtr GlobalExecutionPlan::getExecutionNodeByNodeId(uint64_t id) {
    if (executionNodeExists(id)) {
        return nodeIdIndex[id];
    }
    NES_WARNING("GlobalExecutionPlan: Execution node doesn't exists with the id " << id);
    return nullptr;
}

bool GlobalExecutionPlan::addExecutionNodeAsParentTo(uint64_t childId, ExecutionNodePtr parentExecutionNode) {
    ExecutionNodePtr childNode = getExecutionNodeByNodeId(childId);
    if (childId) {
        NES_DEBUG("GlobalExecutionPlan: Execution node added as parent to the execution node with id " << childId);
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
    return true;
}

bool GlobalExecutionPlan::removeExecutionNode(uint64_t id) {
    if (executionNodeExists(id)) {
        NES_DEBUG("GlobalExecutionPlan: Removed execution node with id " << id);
        return nodeIdIndex.erase(id) == 1;
    }
    return false;
}

bool GlobalExecutionPlan::removeQuerySubPlans(std::string queryId) {
    auto itr = queryIdIndex.find(queryId);
    if (itr == queryIdIndex.end()) {
        NES_WARNING("GlobalExecutionPlan: No query with id " << queryId << " exists in the system");
        return false;
    }

    std::vector<ExecutionNodePtr> executionNodes = queryIdIndex[queryId];
    for (auto executionNode : executionNodes) {

        uint64_t executionNodeId = executionNode->getId();
        if (!executionNode->removeQuerySubPlan(queryId)) {
            NES_ERROR("GlobalExecutionPlan: Unable to remove query sub plan with id " << queryId << " from execution node with id " << executionNodeId);
            return false;
        }
        if (executionNode->getAllQuerySubPlans().empty()) {
            removeExecutionNode(executionNodeId);
        }
    }
    return true;
}

std::vector<ExecutionNodePtr> GlobalExecutionPlan::getExecutionNodesByQueryId(std::string queryId) {
    auto itr = queryIdIndex.find(queryId);
    if (itr != queryIdIndex.end()) {
        NES_DEBUG("GlobalExecutionPlan: Returning vector of Execution nodes for the query with id " << queryId);
        return itr->second;
    }
    NES_WARNING("GlobalExecutionPlan: unable to find the Execution nodes for the query with id " << queryId);
    return {};
}

std::vector<ExecutionNodePtr> GlobalExecutionPlan::getExecutionNodesToSchedule() {
    return executionNodesToSchedule;
}

std::vector<ExecutionNodePtr> GlobalExecutionPlan::getRootNodes() {
    return rootNodes;
}

std::string GlobalExecutionPlan::getAsString() {
    std::stringstream ss;
    auto dumpHandler = ConsoleDumpHandler::create();
    for (auto rootNode : rootNodes) {
        dumpHandler->dump(rootNode, ss);
    }
    return ss.str();
}

void GlobalExecutionPlan::scheduleExecutionNode(ExecutionNodePtr executionNode) {
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
        std::string queryId = pair.first;
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

}// namespace NES
