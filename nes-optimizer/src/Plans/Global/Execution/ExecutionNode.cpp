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

ExecutionNodePtr ExecutionNode::create(ExecutionNodeId executionNodeId) {
    return std::make_shared<ExecutionNode>(executionNodeId);
}

ExecutionNode::ExecutionNode(ExecutionNodeId executionNodeId) : executionNodeId(executionNodeId) {}

bool ExecutionNode::registerDecomposedQueryPlan(const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    auto sharedQueryId = decomposedQueryPlan->getSharedQueryId();
    auto decomposedQueryId = decomposedQueryPlan->getDecomposedQueryPlanId();
    NES_DEBUG("Adding a new decomposed query plan with id {} to the collection for the shared query plan with id {}",
              decomposedQueryId,
              sharedQueryId);
    mapOfSharedQueryToDecomposedQueryPlans[sharedQueryId][decomposedQueryId] = decomposedQueryPlan;
    return true;
}

void ExecutionNode::updateDecomposedQueryPlans(NES::SharedQueryId sharedQueryId,
                                               std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans) {
    NES_DEBUG("ExecutionNode: Updating the decomposed query plan with id :{} to the collection of query sub plans",
              sharedQueryId);
    for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
        auto copiedDecomposedPlan = decomposedQueryPlan->copy();
        mapOfSharedQueryToDecomposedQueryPlans[sharedQueryId][decomposedQueryPlan->getDecomposedQueryPlanId()] =
            copiedDecomposedPlan;
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
        return decomposedQueryPlans;
    }
    NES_ERROR("ExecutionNode : Unable to find shared query plan with id {}", sharedQueryId);
    return decomposedQueryPlans;
}

DecomposedQueryPlanPtr ExecutionNode::getDecomposedQueryPlan(SharedQueryId sharedQueryId,
                                                             DecomposedQueryPlanId decomposedQueryPlanId) const {
    if (mapOfSharedQueryToDecomposedQueryPlans.contains(sharedQueryId)) {
        NES_DEBUG("ExecutionNode : Found shared query plan with id  {}", sharedQueryId);
        auto decomposedQueryPlanMap = mapOfSharedQueryToDecomposedQueryPlans.at(sharedQueryId);
        if (decomposedQueryPlanMap.contains(decomposedQueryPlanId)) {
            return decomposedQueryPlanMap[decomposedQueryPlanId];
        }
        NES_ERROR("ExecutionNode: Unable to find decomposed query plan with id {}", decomposedQueryPlanId);
        return nullptr;
    }
    NES_ERROR("ExecutionNode: Unable to find shared query plan with id {}", sharedQueryId);
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

std::string ExecutionNode::toString() const { return "ExecutionNode(id:" + std::to_string(executionNodeId) + ")"; }

uint64_t ExecutionNode::getId() const { return executionNodeId; }

PlacedDecomposedQueryPlans ExecutionNode::getAllQuerySubPlans() { return mapOfSharedQueryToDecomposedQueryPlans; }

bool ExecutionNode::equal(NodePtr const& rhs) const { return rhs->as<ExecutionNode>()->getId() == executionNodeId; }

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

std::set<SharedQueryId> ExecutionNode::getPlacedSharedQueryPlanIds() const {

    //iterate over all placed plans to fetch the shared query plan ids
    std::set<SharedQueryId> sharedQueryIds;
    for (const auto& item : mapOfSharedQueryToDecomposedQueryPlans) {
        sharedQueryIds.insert(item.first);
    }
    return sharedQueryIds;
}

}// namespace NES::Optimizer