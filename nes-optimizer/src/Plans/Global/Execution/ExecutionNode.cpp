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

#include <Catalogs/Topology/TopologyNode.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <set>
#include <utility>

namespace NES::Optimizer {

ExecutionNode::ExecutionNode(const TopologyNodePtr& physicalNode) : id(physicalNode->getId()), topologyNode(physicalNode) {}

bool ExecutionNode::registerNewDecomposedQueryPlan(SharedQueryId sharedQueryId,
                                                   const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    NES_DEBUG("Adding a new sub query plan to the collection for the shared query plan with id {}", sharedQueryId);
    DecomposedQueryPlanId decomposedQueryId = decomposedQueryPlan->getDecomposedQueryPlanId();
    mapOfSharedQueryToDecomposedQueryPlans[sharedQueryId][decomposedQueryId] = decomposedQueryPlan;
    return true;
}

void ExecutionNode::updateDecomposedQueryPlans(NES::SharedQueryId sharedQueryId,
                                               std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans) {
    NES_DEBUG("ExecutionNode: Updating the decomposed query plan with id :{} to the collection of query sub plans",
              sharedQueryId);
    for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
        mapOfSharedQueryToDecomposedQueryPlans[sharedQueryId][decomposedQueryPlan->getDecomposedQueryPlanId()] =
            decomposedQueryPlan->copy();
    }
    NES_DEBUG("ExecutionNode: Updated the decomposed query plan with id : {} to the collection of query sub plans",
              sharedQueryId);
}

std::vector<DecomposedQueryPlanPtr> ExecutionNode::getAllDecomposedQueryPlans(SharedQueryId sharedQueryId) const {
    std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans;
    if (mapOfSharedQueryToDecomposedQueryPlans.contains(sharedQueryId)) {
        NES_DEBUG("ExecutionNode : Found query sub plan with id  {}", sharedQueryId);
        auto decomposedQueryPlanMap = mapOfSharedQueryToDecomposedQueryPlans.at(sharedQueryId);
        for (const auto& [decomposedQueryPlanId, decomposedQueryPlan] : decomposedQueryPlanMap) {
            decomposedQueryPlans.emplace_back(decomposedQueryPlan);
        }
    }
    NES_WARNING("ExecutionNode : Unable to find shared query plan with id {}", sharedQueryId);
    return decomposedQueryPlans;
}

DecomposedQueryPlanPtr ExecutionNode::getCopyOfDecomposedQueryPlan(SharedQueryId sharedQueryId,
                                                                   DecomposedQueryPlanId decomposedQueryPlanId) const {
    if (mapOfSharedQueryToDecomposedQueryPlans.contains(sharedQueryId)) {
        NES_DEBUG("ExecutionNode : Found shared query plan with id  {}", sharedQueryId);
        auto decomposedQueryPlanMap = mapOfSharedQueryToDecomposedQueryPlans.at(sharedQueryId);
        if (decomposedQueryPlanMap.contains(decomposedQueryPlanId)) {
            return decomposedQueryPlanMap[decomposedQueryPlanId];
        }
        NES_WARNING("ExecutionNode: Unable to find decomposed query plan with id {}", decomposedQueryPlanId);
    }
    NES_WARNING("ExecutionNode: Unable to find shared query plan with id {}", sharedQueryId);
    return nullptr;
}

bool ExecutionNode::removeDecomposedQueryPlans(SharedQueryId sharedQueryId) {
    if (mapOfSharedQueryToDecomposedQueryPlans.erase(sharedQueryId) == 1) {
        NES_DEBUG("ExecutionNode: Successfully removed query sub plan and released the resources");
        return true;
    }
    NES_WARNING("ExecutionNode: Not able to remove query sub plan with id : {}", sharedQueryId);
    return false;
}

bool ExecutionNode::hasRegisteredDecomposedQueryPlans(NES::SharedQueryId sharedQueryId) {
    return mapOfSharedQueryToDecomposedQueryPlans.contains(sharedQueryId);
}

bool ExecutionNode::removeDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryPlanId decomposedQueryPlanId) {

    //Check if the map contains an entry for the shared query id
    if (mapOfSharedQueryToDecomposedQueryPlans.contains(sharedQueryId)) {
        auto querySubPlanMap = mapOfSharedQueryToDecomposedQueryPlans[sharedQueryId];
        //Check if query sub plan exists in the map
        if (querySubPlanMap.contains(decomposedQueryPlanId)) {
            querySubPlanMap.erase(decomposedQueryPlanId);
            //If no query sub plan exist then remove the entry from the mapOfSharedQueryToQuerySubPlans
            if (querySubPlanMap.empty()) {
                mapOfSharedQueryToDecomposedQueryPlans.erase(sharedQueryId);
            }
            return true;
        }
    }
    return false;
}

uint32_t ExecutionNode::getOccupiedResources(SharedQueryId SharedQueryId) {

    // In this method we iterate from the root operators to all their child operator within a query sub plan
    // and count the amount of resources occupied by them. While iterating the operator trees, we keep a list
    // of visited operators so that we count each visited operator only once.

    std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans = getAllDecomposedQueryPlans(SharedQueryId);
    uint32_t occupiedResources = 0;
    for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
        NES_DEBUG("ExecutionNode : calculate the number of resources occupied by the query sub plan and release them");
        occupiedResources += getOccupiedResourcesForDecomposedQueryPlan(decomposedQueryPlan);
        NES_INFO("ExecutionNode: Releasing {} CPU resources from the node with id {}", occupiedResources, id);
    }
    return occupiedResources;
}

uint32_t ExecutionNode::getOccupiedResourcesForDecomposedQueryPlan(const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    uint32_t occupiedResources = 0;
    auto roots = decomposedQueryPlan->getRootOperators();
    // vector keeping track of already visited nodes.
    std::set<uint64_t> visitedOpIds;
    NES_DEBUG("ExecutionNode : Iterate over all root nodes in the query sub graph to calculate occupied resources");
    for (const auto& root : roots) {
        NES_DEBUG("ExecutionNode : Iterate the root node using BFS");
        auto bfsIterator = BreadthFirstNodeIterator(root);
        //for (auto itr = bfsIterator.begin(); itr != BreadthFirstNodeIterator::end(); ++itr) {
        for (auto itr : bfsIterator) {
            auto visitingOp = (*itr).as<OperatorNode>();
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
    return occupiedResources;
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

PlacedDecomposedQueryPlans ExecutionNode::getAllQuerySubPlans() { return mapOfSharedQueryToDecomposedQueryPlans; }

bool ExecutionNode::equal(NodePtr const& rhs) const { return rhs->as<ExecutionNode>()->getId() == id; }

std::vector<std::string> ExecutionNode::toMultilineString() {
    std::vector<std::string> lines;
    lines.push_back(toString());

    for (const auto& mapOfQuerySubPlan : mapOfSharedQueryToDecomposedQueryPlans) {
        for (const auto& [querySubPlanId, querySubPlan] : mapOfQuerySubPlan.second) {
            lines.push_back("QuerySubPlan(SharedQueryId:" + std::to_string(mapOfQuerySubPlan.first)
                            + ", DecomposedQueryPlanId:" + std::to_string(querySubPlan->getDecomposedQueryPlanId())
                            + ", queryState:" + std::string(magic_enum::enum_name(querySubPlan->getState())) + ")");

            // Split the string representation of the queryPlan into multiple lines
            std::string s = querySubPlan->toString();
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
    for (const auto& item : mapOfSharedQueryToDecomposedQueryPlans) {
        sharedQueryIds.insert(item.first);
    }
    return sharedQueryIds;
}

}// namespace NES::Optimizer