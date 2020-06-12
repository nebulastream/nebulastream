#include <Nodes/Operators/OperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Util/Logger.hpp>

namespace NES {

ExecutionNode::ExecutionNode(NESTopologyEntryPtr nesNode, std::string subPlanId, OperatorNodePtr operatorNode) : id(nesNode->getId()), nesNode(nesNode) {
    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->appendPreExistingOperator(operatorNode);
    queryPlan->setQueryId(subPlanId);
    mapOfQuerySubPlans.emplace(subPlanId, queryPlan);
}

ExecutionNode::ExecutionNode(NESTopologyEntryPtr nesNode) : id(nesNode->getId()), nesNode(nesNode) {}

bool ExecutionNode::hasQuerySubPlan(std::string subPlanId) {
    return mapOfQuerySubPlans.find(subPlanId) != mapOfQuerySubPlans.end();
}

bool ExecutionNode::querySubPlanContainsOperator(std::string subPlanId, OperatorNodePtr operatorNode) {
    if (hasQuerySubPlan(subPlanId)) {
        QueryPlanPtr querySubPlan = mapOfQuerySubPlans[subPlanId];
        return querySubPlan->hasOperator(operatorNode);
    }
    NES_WARNING("ExecutionNode: Not able to find " << operatorNode->toString() << " in sub plan with id : " << subPlanId);
    return false;
}

QueryPlanPtr ExecutionNode::getQuerySubPlan(std::string subPlanId) {
    if (hasQuerySubPlan(subPlanId)) {
        return mapOfQuerySubPlans[subPlanId];
    }
    return nullptr;
}

bool ExecutionNode::removeQuerySubPlan(std::string subPlanId) {
    if (mapOfQuerySubPlans.erase(subPlanId) == 1) {
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to remove query sub plan with id : " + subPlanId);
    return false;
}

bool ExecutionNode::createNewQuerySubPlan(std::string subPlanId, OperatorNodePtr operatorNode) {
    QueryPlanPtr querySubPlan = QueryPlan::create();
    querySubPlan->appendPreExistingOperator(operatorNode);
    querySubPlan->setQueryId(subPlanId);
    NES_DEBUG("ExecutionNode: Appending the query sub plan with id : " << subPlanId << " to the collection of query sub plans");
    auto emplace = mapOfQuerySubPlans.emplace(subPlanId, querySubPlan);
    if (emplace.second) {
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to add query sub plan with id : " + subPlanId);
    return false;
}

bool ExecutionNode::createNewQuerySubPlan(std::string subPlanId, QueryPlanPtr querySubPlan) {
    querySubPlan->setQueryId(subPlanId);
    NES_DEBUG("ExecutionNode: Adding the query sub plan with id : " << subPlanId << " to the collection of query sub plans");
    auto emplace = mapOfQuerySubPlans.emplace(subPlanId, querySubPlan);
    if (emplace.second) {
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to add query sub plan with id : " + subPlanId);
    return false;
}

bool ExecutionNode::updateQuerySubPlan(std::string subPlanId, QueryPlanPtr querySubPlan) {
    NES_DEBUG("ExecutionNode: Updating the query sub plan with id : " << subPlanId << " to the collection of query sub plans");
    if (hasQuerySubPlan(subPlanId)) {
        mapOfQuerySubPlans[subPlanId] = querySubPlan;
        NES_DEBUG("ExecutionNode: Updated the query sub plan with id : " << subPlanId << " to the collection of query sub plans");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to find " << querySubPlan->toString() << " in sub plan with id : " << subPlanId);
    return false;
}

bool ExecutionNode::appendOperatorToQuerySubPlan(std::string subPlanId, OperatorNodePtr operatorNode) {
    if (hasQuerySubPlan(subPlanId)) {
        QueryPlanPtr querySubPlan = mapOfQuerySubPlans[subPlanId];
        NES_DEBUG("ExecutionNode: Appending operator " << operatorNode->toString() << " in the query sub plan with id : " << subPlanId);
        querySubPlan->appendOperator(operatorNode);
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to find " << operatorNode->toString() << " in sub plan with id : " << subPlanId);
    return false;
}

const std::string ExecutionNode::toString() const {
    return "ExecutionNode(" + std::to_string(id) + ")";
}

ExecutionNodePtr ExecutionNode::createExecutionNode(NESTopologyEntryPtr nesNode, std::string subPlanId, OperatorNodePtr operatorNode) {
    return std::make_shared<ExecutionNode>(ExecutionNode(nesNode, subPlanId, operatorNode));
}

ExecutionNodePtr ExecutionNode::createExecutionNode(NESTopologyEntryPtr nesNode) {
    return std::make_shared<ExecutionNode>(ExecutionNode(nesNode));
}

uint64_t ExecutionNode::getId() {
    return id;
}

NESTopologyEntryPtr ExecutionNode::getNesNode() {
    return nesNode;
}

std::map<std::string, QueryPlanPtr> ExecutionNode::getAllQuerySubPlans() {
    return mapOfQuerySubPlans;
}

}// namespace NES