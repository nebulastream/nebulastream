#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Operators/QueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Util/Logger.hpp>

namespace NES {

ExecutionNode::ExecutionNode(NESTopologyEntryPtr nesNode, std::string subPlanId, OperatorNodePtr operatorNode) : nesNode(nesNode), id(nesNode->getId()) {
    QueryPlanPtr queryPlan = QueryPlan::create(operatorNode);
    queryPlan->setQueryId(subPlanId);
    mapOfQuerySubPlans.emplace(subPlanId, queryPlan);
}

bool ExecutionNode::querySubPlanExists(std::string subPlanId) {
    return mapOfQuerySubPlans.find(subPlanId) == mapOfQuerySubPlans.end();
}

bool ExecutionNode::querySubPlanContainsOperator(std::string subPlanId, OperatorNodePtr operatorNode) {
    if (querySubPlanExists(subPlanId)) {
        QueryPlanPtr querySubPlan = mapOfQuerySubPlans[subPlanId];
        return querySubPlan->hasOperator(operatorNode);
    }
    NES_WARNING("ExecutionNode: Not able to find " << operatorNode->toString() << " in sub plan with id : " << subPlanId);
    return false;
}

QueryPlanPtr ExecutionNode::getQuerySubPlan(std::string subPlanId) {
    if(querySubPlanExists(subPlanId)){
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
    QueryPlanPtr querySubPlan = QueryPlan::create(operatorNode);
    querySubPlan->setQueryId(subPlanId);
    NES_DEBUG("ExecutionNode: Appending the query sub plan with id : " << subPlanId << " to the collection of query sub plans");
    auto emplace = mapOfQuerySubPlans.emplace(subPlanId, querySubPlan);
    if (emplace.second) {
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to add query sub plan with id : " + subPlanId);
    return false;
}

bool ExecutionNode::appendOperatorToQuerySubPlan(std::string subPlanId, OperatorNodePtr operatorNode) {
    if (querySubPlanExists(subPlanId)) {
        QueryPlanPtr querySubPlan = mapOfQuerySubPlans[subPlanId];
        NES_DEBUG("ExecutionNode: Appending operator " << operatorNode->toString() << " in the query sub plan with id : " << subPlanId);
        querySubPlan->appendOperator(operatorNode);
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to find " << operatorNode->toString() << " in sub plan with id : " << subPlanId);
    return false;
}

QueryPlanPtr ExecutionNode::getSubPlan(std::string subPlanId) {
    auto itr = mapOfQuerySubPlans.find(subPlanId);
    if (itr != mapOfQuerySubPlans.end()) {
        return itr->second;
    }
    NES_WARNING("ExecutionNode: Not able to find a query sub plan with id: " + subPlanId);
    return nullptr;
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

uint64_t ExecutionNode::getId() const {
    return id;
}
}// namespace NES