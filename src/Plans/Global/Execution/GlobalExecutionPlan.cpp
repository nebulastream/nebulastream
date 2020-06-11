#include <Nodes/Util/DumpContext.hpp>
#include <Nodes/Util/ConsoleDumpHandler.hpp>
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
            NES_DEBUG("GlobalExecutionPlan: Execution node marked as to be scheduled");
            executionNodesToSchedule.push_back(parentExecutionNode);
            return true;
        }
        NES_WARNING("GlobalExecutionPlan: Failed to add Execution node as parent to the execution node with id " << childId);
        return false;
    }
    NES_WARNING("GlobalExecutionPlan: Child node doesn't exists with the id " << childId);
    return false;
}

bool GlobalExecutionPlan::addExecutionNodeAsRootAndParentTo(uint64_t childId, ExecutionNodePtr parentExecutionNode) {

    if (addExecutionNodeAsParentTo(childId, parentExecutionNode)) {
        NES_DEBUG("GlobalExecutionPlan: Added Execution node as root node");
        auto found = std::find(rootNodes.begin(), rootNodes.end(), parentExecutionNode);
        if (found == rootNodes.end()) {
            rootNodes.push_back(parentExecutionNode);
            NES_DEBUG("GlobalExecutionPlan: Added Execution node with id " << parentExecutionNode->getId());
            nodeIdIndex[parentExecutionNode->getId()] = parentExecutionNode;
            NES_DEBUG("GlobalExecutionPlan: Execution node marked as to be scheduled");
            executionNodesToSchedule.push_back(parentExecutionNode);
        } else {
            NES_WARNING("GlobalExecutionPlan: Execution node already present in the root node list");
        }
        return true;
    }
    return false;
}

bool GlobalExecutionPlan::addExecutionNodeAsRoot(ExecutionNodePtr executionNode) {
    NES_DEBUG("GlobalExecutionPlan: Added Execution node as root node");
    auto found = std::find(rootNodes.begin(), rootNodes.end(), executionNode);
    if (found == rootNodes.end()) {
        rootNodes.push_back(executionNode);
        NES_DEBUG("GlobalExecutionPlan: Added Execution node with id " << executionNode->getId());
        nodeIdIndex[executionNode->getId()] = executionNode;
        NES_DEBUG("GlobalExecutionPlan: Execution node marked as to be scheduled");
        executionNodesToSchedule.push_back(executionNode);
    } else {
        NES_WARNING("GlobalExecutionPlan: Execution node already present in the root node list");
    }
    return true;
}

bool GlobalExecutionPlan::addExecutionNode(ExecutionNodePtr parentExecutionNode) {
    NES_DEBUG("GlobalExecutionPlan: Added Execution node with id " << parentExecutionNode->getId());
    nodeIdIndex[parentExecutionNode->getId()] = parentExecutionNode;
    NES_DEBUG("GlobalExecutionPlan: Execution node marked as to be scheduled");
    executionNodesToSchedule.push_back(parentExecutionNode);
    return true;
}

bool GlobalExecutionPlan::removeExecutionNode(uint64_t id) {
    if (executionNodeExists(id)) {
        NES_DEBUG("GlobalExecutionPlan: Removed execution node with id " + id);
        return nodeIdIndex.erase(id) == 1;
    }
    return false;
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
}// namespace NES
