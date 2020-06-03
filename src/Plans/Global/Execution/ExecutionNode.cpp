#include "Plans/Global/Execution/ExecutionNode.hpp"
#include "Topology/NESTopologyEntry.hpp"
#include "Util/Logger.hpp"

namespace NES {

ExecutionNode::ExecutionNode(NESTopologyEntryPtr nesNode, uint64_t subPlanId, QueryPlanPtr querySubPlan) : nesNode(nesNode), id(nesNode->getId()) {
    mapOfQuerySubPlans.emplace(subPlanId, querySubPlan);
}

bool ExecutionNode::removeSubPlan(uint64_t subPlanId) {
    if (mapOfQuerySubPlans.erase(subPlanId) == 1) {
        scheduled = false;
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to remove query sub plan with id : " + subPlanId);
    return false;
}

bool ExecutionNode::addNewSubPlan(uint64_t subPlanId, QueryPlanPtr querySubPlan) {
    auto emplace = mapOfQuerySubPlans.emplace(subPlanId, querySubPlan);
    if (emplace.second) {
        scheduled = false;
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to add query sub plan with id : " + std::to_string(subPlanId) + " and plan " + querySubPlan->toString());
    return false;
}

QueryPlanPtr ExecutionNode::getSubPlan(uint64_t subPlanId) {
    auto itr = mapOfQuerySubPlans.find(subPlanId);
    if (itr != mapOfQuerySubPlans.end()) {
        return itr->second;
    }
    NES_WARNING("ExecutionNode: Not able to find a query sub plan with id: " + subPlanId);
    return nullptr;
}

bool ExecutionNode::addConfiguration(std::string configName, std::string configValue) {
    auto emplace = configurations.emplace(configName, configValue);
    if (emplace.second) {
        scheduled = false;
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to insert configuration: " + configName + "," + configValue);
    return false;
}

bool ExecutionNode::updateConfiguration(std::string configName, std::string updatedValue) {
    auto itr = configurations.find(configName);
    if (itr != configurations.end()) {
        configurations[configName] = updatedValue;
        scheduled = false;
        return true;
    }
    NES_WARNING("ExecutionNode: Unable to find the configuration to update in the map : " + configName);
    return false;
}

const std::string ExecutionNode::toString() const {
    return "ExecutionNode(" + std::to_string(id) + ")";
}

bool ExecutionNode::isScheduled() const {
    return scheduled;
}

void ExecutionNode::setScheduled(bool scheduled) {
    ExecutionNode::scheduled = scheduled;
}

ExecutionNodePtr createExecutionNode(NESTopologyEntryPtr nesNode, uint64_t subPlanId, QueryPlanPtr querySubPlan) {
    return std::make_shared<ExecutionNode>(nesNode, subPlanId, querySubPlan);
}
}// namespace NES