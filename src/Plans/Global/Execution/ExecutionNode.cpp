#include <Components/NesWorker.hpp>
#include <CoordinatorEngine/CoordinatorEngine.hpp>
#include <Deployer/QueryDeployer.hpp>
#include <Nodes/Operators/LogicalOperators/LogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/NESTopologyEntry.hpp>
#include <Util/Logger.hpp>
#include <set>

namespace NES {

ExecutionNode::ExecutionNode(NESTopologyEntryPtr nesNode, std::string queryId, OperatorNodePtr operatorNode) : id(nesNode->getId()), nesNode(nesNode) {
    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->appendPreExistingOperator(operatorNode);
    queryPlan->setQueryId(queryId);
    mapOfQuerySubPlans.emplace(queryId, queryPlan);
}

ExecutionNode::ExecutionNode(NESTopologyEntryPtr nesNode) : id(nesNode->getId()), nesNode(nesNode) {}

bool ExecutionNode::hasQuerySubPlan(std::string queryId) {
    NES_DEBUG("ExecutionNode : Checking if a query sub plan exists with id " << queryId);
    return mapOfQuerySubPlans.find(queryId) != mapOfQuerySubPlans.end();
}

bool ExecutionNode::checkIfQuerySubPlanContainsOperator(std::string queryId, OperatorNodePtr operatorNode) {
    NES_DEBUG("ExecutionNode : Checking if operator " << operatorNode->toString() << " exists in the query sub plan with id " << queryId);
    if (hasQuerySubPlan(queryId)) {
        QueryPlanPtr querySubPlan = mapOfQuerySubPlans[queryId];
        return querySubPlan->hasOperator(operatorNode);
    }
    NES_WARNING("ExecutionNode: Not able to find " << operatorNode->toString() << " in sub plan with id : " << queryId);
    return false;
}

QueryPlanPtr ExecutionNode::getQuerySubPlan(std::string queryId) {
    if (hasQuerySubPlan(queryId)) {
        NES_DEBUG("ExecutionNode : Found query sub plan with id " << queryId);
        return mapOfQuerySubPlans[queryId];
    }
    NES_WARNING("ExecutionNode : Unable to find query sub plan with id " << queryId);
    return nullptr;
}

bool ExecutionNode::removeQuerySubPlan(std::string queryId) {
    QueryPlanPtr querySubPlan = mapOfQuerySubPlans[queryId];
    if (mapOfQuerySubPlans.erase(queryId) == 1) {
        NES_INFO("ExecutionNode: Releasing resources occupied by the query sub plan with id  " + queryId << " from node with id " << id);
        freeOccupiedResources(querySubPlan);
        NES_DEBUG("ExecutionNode: Successfully removed query sub plan and released the resources");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to remove query sub plan with id : " + queryId);
    return false;
}

void ExecutionNode::freeOccupiedResources(QueryPlanPtr querySubPlan) {

    // In this method we iterate for root operators to all their child operator within a query sub plan
    // and count the number of resources occupied by them. While iterating the operator trees, we keep a list
    // of visited operators so that we count each visited operator only once.

    NES_DEBUG("ExecutionNode : calculate the number of resources occupied by the query sub plan and release them");
    int32_t resourceToFree = 0;
    auto roots = querySubPlan->getRootOperators();
    // vector keeping track of already visited nodes.
    std::set<u_int64_t> visitedOpIds;
    NES_DEBUG("ExecutionNode : Iterate over all root nodes in the query sub graph to calculate occupied resources");
    for (auto root : roots) {
        NES_DEBUG("ExecutionNode : Iterate the root node using BFS");
        auto bfsIterator = BreadthFirstNodeIterator(root);
        for (auto itr = bfsIterator.begin(); itr != bfsIterator.end(); ++itr) {
            auto visitingOp = (*itr)->as<OperatorNode>();
            if (visitedOpIds.find(visitingOp->getId()) != visitedOpIds.end()) {
                NES_TRACE("ExecutionNode : Found already visited operator skipping rest of the path traverse.");
                break;
            }
            // If the visiting operator is not a system operator then count the resource and add it to the visited operator list.
            if (visitingOp->getId() != SYS_SOURCE_OPERATOR_ID && visitingOp->getId() != SYS_SINK_OPERATOR_ID) {
                // increase the resource count
                resourceToFree++;
                // add operator id to the already visited operator id collection
                visitedOpIds.insert(visitingOp->getId());
            }
        }
    }
    NES_INFO("ExecutionNode: Releasing " << resourceToFree << " CPU resources from the node with id " << id);
    nesNode->increaseCpuCapacity(resourceToFree);
}

bool ExecutionNode::createNewQuerySubPlan(std::string queryId, OperatorNodePtr operatorNode) {
    NES_DEBUG("ExecutionNode: Creating a new query sub plan with id : " << queryId << " and assigning root operator as " << operatorNode->toString());
    QueryPlanPtr querySubPlan = QueryPlan::create();
    querySubPlan->appendPreExistingOperator(operatorNode);
    querySubPlan->setQueryId(queryId);
    NES_DEBUG("ExecutionNode: Appending the query sub plan with id : " << queryId << " to the collection of query sub plans");
    auto emplace = mapOfQuerySubPlans.emplace(queryId, querySubPlan);
    if (emplace.second) {
        NES_DEBUG("ExecutionNode: Successfully created new query sub plan");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to add query sub plan with id : " + queryId);
    return false;
}

bool ExecutionNode::createNewQuerySubPlan(std::string queryId, QueryPlanPtr querySubPlan) {
    NES_DEBUG("ExecutionNode: Adding a new entry to the collection query sub plans after assigning the id : " << queryId);
    querySubPlan->setQueryId(queryId);
    NES_DEBUG("ExecutionNode: Adding the query sub plan with id : " << queryId << " to the collection of query sub plans");
    auto emplace = mapOfQuerySubPlans.emplace(queryId, querySubPlan);
    if (emplace.second) {
        NES_DEBUG("ExecutionNode: Successfully created new query sub plan");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to add query sub plan with id : " + queryId);
    return false;
}

bool ExecutionNode::updateQuerySubPlan(std::string queryId, QueryPlanPtr querySubPlan) {
    NES_DEBUG("ExecutionNode: Updating the query sub plan with id : " << queryId << " to the collection of query sub plans");
    if (hasQuerySubPlan(queryId)) {
        mapOfQuerySubPlans[queryId] = querySubPlan;
        NES_DEBUG("ExecutionNode: Updated the query sub plan with id : " << queryId << " to the collection of query sub plans");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to find " << querySubPlan->toString() << " in sub plan with id : " << queryId);
    return false;
}

bool ExecutionNode::appendOperatorToQuerySubPlan(std::string queryId, OperatorNodePtr operatorNode) {
    if (hasQuerySubPlan(queryId)) {
        QueryPlanPtr querySubPlan = mapOfQuerySubPlans[queryId];
        NES_DEBUG("ExecutionNode: Appending operator " << operatorNode->toString() << " in the query sub plan with id : " << queryId);
        querySubPlan->appendOperator(operatorNode);
        NES_DEBUG("ExecutionNode: Successfully appended operator to the query sub plan");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to find " << operatorNode->toString() << " in sub plan with id : " << queryId);
    return false;
}

const std::string ExecutionNode::toString() const {
    return "ExecutionNode(" + std::to_string(id) + ")";
}

ExecutionNodePtr ExecutionNode::createExecutionNode(NESTopologyEntryPtr nesNode, std::string queryId, OperatorNodePtr operatorNode) {
    return std::make_shared<ExecutionNode>(ExecutionNode(nesNode, queryId, operatorNode));
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

bool ExecutionNode::equal(NodePtr rhs) const {
    return rhs->as<ExecutionNode>()->getId() == id;
}

}// namespace NES