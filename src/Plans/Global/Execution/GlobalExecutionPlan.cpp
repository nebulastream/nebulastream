#include "Plans/Global/Execution/GlobalExecutionPlan.hpp"
#include "Plans/Global/Execution/ExecutionNode.hpp"
#include "Util/Logger.hpp"

namespace NES {

GlobalExecutionPlanPtr GlobalExecutionPlan::create() {
    return std::make_shared<GlobalExecutionPlan>(GlobalExecutionPlan());
}

bool GlobalExecutionPlan::executionNodeExists(uint64_t id) {
    return nodeIdIndex.find(id) != nodeIdIndex.end();
}

ExecutionNodePtr GlobalExecutionPlan::getExecutionNode(uint64_t id) {
    if (executionNodeExists(id)) {
        return nodeIdIndex[id];
    }
    NES_WARNING("GlobalExecutionPlan: Execution node doesn't exists with the id " << id);
    return nullptr;
}

bool GlobalExecutionPlan::addExecutionNodeAsChildTo(uint64_t parentId, ExecutionNodePtr childExecutionNode) {
    ExecutionNodePtr parentNode = getExecutionNode(parentId);
    if (parentNode) {
        NES_DEBUG("GlobalExecutionPlan: Adding Execution node as child to the execution node with id " << parentId);
        if (parentNode->addChild(childExecutionNode)) {
            NES_DEBUG("GlobalExecutionPlan: Added Execution node with id " << childExecutionNode->getId());
            nodeIdIndex[childExecutionNode->getId()] = childExecutionNode;
            NES_DEBUG("GlobalExecutionPlan: Execution node marked as to be scheduled");
            executionNodesToBeScheduled.push_back(childExecutionNode);
            return true;
        }
        NES_WARNING("GlobalExecutionPlan: Failed to add Execution node as child to the execution node with id " << parentId);
        return false;
    }
    NES_WARNING("GlobalExecutionPlan: Parent node doesn't exists with the id " << parentId);
    return false;
}

bool GlobalExecutionPlan::addExecutionNodeAsParentTo(uint64_t childId, ExecutionNodePtr parentExecutionNode) {
    ExecutionNodePtr childNode = getExecutionNode(childId);
    if (childId) {
        NES_DEBUG("GlobalExecutionPlan: Execution node added as parent to the execution node with id " << childId);
        if (childNode->addParent(parentExecutionNode)) {
            NES_DEBUG("GlobalExecutionPlan: Added Execution node with id " << parentExecutionNode->getId());
            nodeIdIndex[parentExecutionNode->getId()] = parentExecutionNode;
            NES_DEBUG("GlobalExecutionPlan: Execution node marked as to be scheduled");
            executionNodesToBeScheduled.push_back(parentExecutionNode);
            return true;
        }
        NES_WARNING("GlobalExecutionPlan: Failed to add Execution node as parent to the execution node with id " << childId);
        return false;
    }
    NES_WARNING("GlobalExecutionPlan: Child node doesn't exists with the id " << childId);
    return false;
}

bool GlobalExecutionPlan::addExecutionNodeAsRootAndParentTo(uint64_t childId, ExecutionNodePtr parentExecutionNode) {

    if(addExecutionNodeAsParentTo(childId, parentExecutionNode)){
        NES_DEBUG("GlobalExecutionPlan: Added Execution node as root node");
        rootNodes.push_back(parentExecutionNode);
        return true;
    }
    return false;
}

bool GlobalExecutionPlan::addExecutionNode(ExecutionNodePtr parentExecutionNode) {
    NES_DEBUG("GlobalExecutionPlan: Added Execution node with id " << parentExecutionNode->getId());
    nodeIdIndex[parentExecutionNode->getId()] = parentExecutionNode;
    NES_DEBUG("GlobalExecutionPlan: Execution node marked as to be scheduled");
    executionNodesToBeScheduled.push_back(parentExecutionNode);
    return true;
}

bool GlobalExecutionPlan::removeExecutionNode(uint64_t id) {
    if (executionNodeExists(id)) {
        NES_DEBUG("GlobalExecutionPlan: Removed execution node with id " + id);
        return nodeIdIndex.erase(id) == 1;
    }
    return false;
}

}// namespace NES
