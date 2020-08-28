#include <Components/NesWorker.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Nodes/Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Nodes/Operators/OperatorNode.hpp>
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <set>

namespace NES {

ExecutionNode::ExecutionNode(TopologyNodePtr physicalNode, QueryId queryId, OperatorNodePtr operatorNode) : id(physicalNode->getId()), topologyNode(physicalNode) {
    QueryPlanPtr queryPlan = QueryPlan::create();
    queryPlan->appendPreExistingOperator(operatorNode);
    queryPlan->setQueryId(queryId);
    std::vector<QueryPlanPtr> querySubPlans{queryPlan};
    mapOfQuerySubPlans.emplace(queryId, querySubPlans);
}

ExecutionNode::ExecutionNode(TopologyNodePtr physicalNode) : id(physicalNode->getId()), topologyNode(physicalNode) {}

bool ExecutionNode::hasQuerySubPlans(QueryId queryId) {
    NES_DEBUG("ExecutionNode : Checking if a query sub plan exists with id " << queryId);
    return mapOfQuerySubPlans.find(queryId) != mapOfQuerySubPlans.end();
}

//bool ExecutionNode::checkIfQuerySubPlanContainsOperator(QueryId queryId, OperatorNodePtr operatorNode) {
//    NES_DEBUG("ExecutionNode : Checking if operator " << operatorNode->toString() << " exists in the query sub plan with id " << queryId);
//    if (hasQuerySubPlans(queryId)) {
//        QueryPlanPtr querySubPlan = mapOfQuerySubPlans[queryId];
//        return querySubPlan->hasOperator(operatorNode);
//    }
//    NES_WARNING("ExecutionNode: Not able to find " << operatorNode->toString() << " in sub plan with id : " << queryId);
//    return false;
//}

std::vector<QueryPlanPtr> ExecutionNode::getQuerySubPlans(QueryId queryId) {
    if (hasQuerySubPlans(queryId)) {
        NES_DEBUG("ExecutionNode : Found query sub plan with id " << queryId);
        return mapOfQuerySubPlans[queryId];
    }
    NES_WARNING("ExecutionNode : Unable to find query sub plan with id " << queryId);
    return {};
}

bool ExecutionNode::removeQuerySubPlans(QueryId queryId) {
    std::vector<QueryPlanPtr> querySubPlans = mapOfQuerySubPlans[queryId];
    if (mapOfQuerySubPlans.erase(queryId) == 1) {
        NES_DEBUG("ExecutionNode: Successfully removed query sub plan and released the resources");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to remove query sub plan with id : " << queryId);
    return false;
}

uint32_t ExecutionNode::getOccupiedResources(QueryId queryId) {

    // In this method we iterate from the root operators to all their child operator within a query sub plan
    // and count the amount of resources occupied by them. While iterating the operator trees, we keep a list
    // of visited operators so that we count each visited operator only once.

    std::vector<QueryPlanPtr> querySubPlans = getQuerySubPlans(queryId);
    uint32_t occupiedResources = 0;
    for (auto querySubPlan : querySubPlans) {
        NES_DEBUG("ExecutionNode : calculate the number of resources occupied by the query sub plan and release them");
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
                if (visitingOp->instanceOf<SourceLogicalOperatorNode>()) {
                    auto srcOperator = visitingOp->as<SourceLogicalOperatorNode>();
                    if (!srcOperator->getSourceDescriptor()->instanceOf<Network::NetworkSourceDescriptor>()) {
                        // increase the resource count
                        occupiedResources++;
                        // add operator id to the already visited operator id collection
                        visitedOpIds.insert(visitingOp->getId());
                    }
                } else if (visitingOp->instanceOf<SinkLogicalOperatorNode>()) {
                    auto sinkOperator = visitingOp->as<SinkLogicalOperatorNode>();
                    if (!sinkOperator->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
                        // increase the resource count
                        occupiedResources++;
                        // add operator id to the already visited operator id collection
                        visitedOpIds.insert(visitingOp->getId());
                    }
                } else {
                    // increase the resource count
                    occupiedResources++;
                    // add operator id to the already visited operator id collection
                    visitedOpIds.insert(visitingOp->getId());
                }
            }
        }
        NES_INFO("ExecutionNode: Releasing " << occupiedResources << " CPU resources from the node with id " << id);
    }
    return occupiedResources;
}

bool ExecutionNode::addNewQuerySubPlan(QueryId queryId, QueryPlanPtr querySubPlan) {
    if (hasQuerySubPlans(queryId)) {
        NES_DEBUG("ExecutionNode: Adding a new entry to the collection of query sub plans after assigning the id : " << queryId);
        std::vector<QueryPlanPtr> querySubPlans = mapOfQuerySubPlans[queryId];
        querySubPlans.push_back(querySubPlan);
        mapOfQuerySubPlans[queryId] = querySubPlans;
    } else {
        NES_DEBUG("ExecutionNode: Creating a new entry of query sub plans and assigning to the id : " << queryId);
        std::vector<QueryPlanPtr> querySubPlans{querySubPlan};
        mapOfQuerySubPlans[queryId] = querySubPlans;
    }
    return true;
}

bool ExecutionNode::updateQuerySubPlans(QueryId queryId, std::vector<QueryPlanPtr> querySubPlans) {
    NES_DEBUG("ExecutionNode: Updating the query sub plan with id : " << queryId << " to the collection of query sub plans");
    if (hasQuerySubPlans(queryId)) {
        mapOfQuerySubPlans[queryId] = querySubPlans;
        NES_DEBUG("ExecutionNode: Updated the query sub plan with id : " << queryId << " to the collection of query sub plans");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to find query sub plan with id : " << queryId << " creating a new entry");
    return false;
}

//bool ExecutionNode::appendOperatorToQuerySubPlan(QueryId queryId, OperatorNodePtr operatorNode) {
//    if (hasQuerySubPlans(queryId)) {
//        QueryPlanPtr querySubPlan = mapOfQuerySubPlans[queryId];
//        NES_DEBUG("ExecutionNode: Appending operator " << operatorNode->toString() << " in the query sub plan with id : " << queryId);
//        querySubPlan->appendOperator(operatorNode);
//        NES_DEBUG("ExecutionNode: Successfully appended operator to the query sub plan");
//        return true;
//    }
//    NES_WARNING("ExecutionNode: Not able to find " << operatorNode->toString() << " in sub plan with id : " << queryId);
//    return false;
//}

const std::string ExecutionNode::toString() const {
    return "ExecutionNode(" + std::to_string(id) + ")";
}

ExecutionNodePtr ExecutionNode::createExecutionNode(TopologyNodePtr physicalNode, QueryId queryId, OperatorNodePtr operatorNode) {
    return std::make_shared<ExecutionNode>(ExecutionNode(physicalNode, queryId, operatorNode));
}

ExecutionNodePtr ExecutionNode::createExecutionNode(TopologyNodePtr physicalNode) {
    return std::make_shared<ExecutionNode>(ExecutionNode(physicalNode));
}

uint64_t ExecutionNode::getId() {
    return id;
}

TopologyNodePtr ExecutionNode::getTopologyNode() {
    return topologyNode;
}

std::map<QueryId, std::vector<QueryPlanPtr>> ExecutionNode::getAllQuerySubPlans() {
    return mapOfQuerySubPlans;
}

bool ExecutionNode::equal(NodePtr rhs) const {
    return rhs->as<ExecutionNode>()->getId() == id;
}

}// namespace NES