/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <set>
#include <utility>

namespace NES {

ExecutionNode::ExecutionNode(const TopologyNodePtr& physicalNode) : id(physicalNode->getId()), topologyNode(physicalNode) {}

bool ExecutionNode::hasQuerySubPlans(QueryId sharedQueryId) {
    NES_DEBUG("ExecutionNode : Checking if a query sub plan exists with id  {}", sharedQueryId);
    return mapOfQuerySubPlans.find(sharedQueryId) != mapOfQuerySubPlans.end();
}

std::vector<QueryPlanPtr> ExecutionNode::getQuerySubPlans(QueryId sharedQueryId) {
    if (hasQuerySubPlans(sharedQueryId)) {
        NES_DEBUG("ExecutionNode : Found query sub plan with id  {}", sharedQueryId);
        return mapOfQuerySubPlans[sharedQueryId];
    }
    NES_WARNING("ExecutionNode : Unable to find query sub plan with id {}", sharedQueryId);
    return {};
}

bool ExecutionNode::removeQuerySubPlans(QueryId sharedQueryId) {
    if (mapOfQuerySubPlans.erase(sharedQueryId) == 1) {
        NES_DEBUG("ExecutionNode: Successfully removed query sub plan and released the resources");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to remove query sub plan with id : {}", sharedQueryId);
    return false;
}

uint32_t ExecutionNode::getOccupiedResources(QueryId sharedQueryId) {

    // In this method we iterate from the root operators to all their child operator within a query sub plan
    // and count the amount of resources occupied by them. While iterating the operator trees, we keep a list
    // of visited operators so that we count each visited operator only once.

    std::vector<QueryPlanPtr> querySubPlans = getQuerySubPlans(sharedQueryId);
    uint32_t occupiedResources = 0;
    for (const auto& querySubPlan : querySubPlans) {
        NES_DEBUG("ExecutionNode : calculate the number of resources occupied by the query sub plan and release them");
        auto roots = querySubPlan->getRootOperators();
        // vector keeping track of already visited nodes.
        std::set<uint64_t> visitedOpIds;
        NES_DEBUG("ExecutionNode : Iterate over all root nodes in the query sub graph to calculate occupied resources");
        for (const auto& root : roots) {
            NES_DEBUG("ExecutionNode : Iterate the root node using BFS");
            auto bfsIterator = BreadthFirstNodeIterator(root);
            for (auto itr = bfsIterator.begin(); itr != NES::BreadthFirstNodeIterator::end(); ++itr) {
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
        NES_INFO("ExecutionNode: Releasing {} CPU resources from the node with id {}", occupiedResources, id);
    }
    return occupiedResources;
}

bool ExecutionNode::addNewQuerySubPlan(QueryId sharedQueryId, const QueryPlanPtr& querySubPlan) {
    if (hasQuerySubPlans(sharedQueryId)) {
        NES_DEBUG("ExecutionNode: Adding a new entry to the collection of query sub plans after assigning the id :  {}",
                  sharedQueryId);
        std::vector<QueryPlanPtr> querySubPlans = mapOfQuerySubPlans[sharedQueryId];
        querySubPlans.push_back(querySubPlan);
        mapOfQuerySubPlans[sharedQueryId] = querySubPlans;
    } else {
        NES_DEBUG("ExecutionNode: Creating a new entry of query sub plans and assigning to the id :  {}", sharedQueryId);
        std::vector<QueryPlanPtr> querySubPlans{querySubPlan};
        mapOfQuerySubPlans[sharedQueryId] = querySubPlans;
    }
    return true;
}

bool ExecutionNode::updateQuerySubPlans(QueryId sharedQueryId, std::vector<QueryPlanPtr> querySubPlans) {
    NES_DEBUG("ExecutionNode: Updating the query sub plan with id :{} to the collection of query sub plans", sharedQueryId);
    if (hasQuerySubPlans(sharedQueryId)) {
        mapOfQuerySubPlans[sharedQueryId] = std::move(querySubPlans);
        NES_DEBUG("ExecutionNode: Updated the query sub plan with id : {} to the collection of query sub plans", sharedQueryId);
        return true;
    }
    NES_DEBUG("ExecutionNode: Not able to find query sub plan with id : {} creating a new entry", sharedQueryId);
    return false;
}

std::string ExecutionNode::toString() const {
    return "ExecutionNode(id:" + std::to_string(id) + ", ip:" + topologyNode->getIpAddress()
        + ", topologyId:" + std::to_string(topologyNode->getId()) + ")";
}

ExecutionNodePtr ExecutionNode::createExecutionNode(TopologyNodePtr physicalNode) {
    return std::make_shared<ExecutionNode>(ExecutionNode(physicalNode));
}

uint64_t ExecutionNode::getId() const { return id; }

TopologyNodePtr ExecutionNode::getTopologyNode() { return topologyNode; }

std::map<QueryId, std::vector<QueryPlanPtr>> ExecutionNode::getAllQuerySubPlans() { return mapOfQuerySubPlans; }

bool ExecutionNode::equal(NodePtr const& rhs) const { return rhs->as<ExecutionNode>()->getId() == id; }

std::vector<std::string> ExecutionNode::toMultilineString() {
    std::vector<std::string> lines;
    lines.push_back(toString());

    for (const auto& mapOfQuerySubPlan : mapOfQuerySubPlans) {
        for (const auto& queryPlan : mapOfQuerySubPlan.second) {
            lines.push_back("QuerySubPlan(queryId:" + std::to_string(mapOfQuerySubPlan.first)
                            + ", querySubPlanId:" + std::to_string(queryPlan->getQuerySubPlanId()) + ")");

            // Split the string representation of the queryPlan into multiple lines
            std::string s = queryPlan->toString();
            std::string delimiter = "\n";
            uint64_t pos = 0;
            std::string token;
            while ((pos = s.find(delimiter)) != std::string::npos) {
                token = s.substr(0, pos);
                lines.push_back(' ' + token);
                s.erase(0, pos + delimiter.length());
            }
        }
    }

    return lines;
}

std::set<SharedQueryId> ExecutionNode::getPlacedSharedQueryPlanIds() {

    //iterate over all placed plans to fetch the shared query plan ids
    std::set<SharedQueryId> sharedQueryIds;
    for (const auto& item : mapOfQuerySubPlans) {
        sharedQueryIds.insert(item.first);
    }
    return sharedQueryIds;
}

}// namespace NES