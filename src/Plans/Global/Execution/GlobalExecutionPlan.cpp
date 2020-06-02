#include "Plans/Global/Execution/GlobalExecutionPlan.hpp"
#include "Plans/Global/Execution/ExecutionNode.hpp"
#include "Util/Logger.hpp"

namespace NES {

GlobalExecutionPlanPtr GlobalExecutionPlan::create() {
    static std::shared_ptr<GlobalExecutionPlan> instance{new GlobalExecutionPlan};
    return instance;
}

bool GlobalExecutionPlan::executionNodeExists(uint64_t id) {
    return nodeIdIndex.find(id) != nodeIdIndex.end();
}

ExecutionNodePtr GlobalExecutionPlan::getExecutionNode(uint64_t id) {
    if (executionNodeExists(id)) {
        return nodeIdIndex[id];
    }
    NES_WARNING("GlobalExecutionPlan: Execution node doesn't exists with the id " + id);
    return nullptr;
}

bool GlobalExecutionPlan::addExecutionNodeAsChildTo(uint64_t parentId, ExecutionNodePtr childExecutionNode) {
    ExecutionNodePtr parentNode = getExecutionNode(parentId);
    if (parentNode) {
        NES_DEBUG("GlobalExecutionPlan: Execution node added as child to the execution node with id " + parentId);
        return parentNode->addChild(childExecutionNode);
    }
    return false;
}

bool GlobalExecutionPlan::addExecutionNodeAsParentTo(uint64_t childId, ExecutionNodePtr parentExecutionNode) {
    ExecutionNodePtr childNode = getExecutionNode(childId);
    if (childId) {
        NES_DEBUG("GlobalExecutionPlan: Execution node added as parent to the execution node with id " + childId);
        return childNode->addParent(parentExecutionNode);
    }
    return false;
}

bool GlobalExecutionPlan::removeExecutionNode(uint64_t id) {
    if (executionNodeExists(id)) {
        NES_DEBUG("GlobalExecutionPlan: Removed execution node with id " + id);
        return nodeIdIndex.erase(id) == 1;
    }
    return false;
}

}// namespace NES
