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
#include <Identifiers/Identifiers.hpp>
#include <Nodes/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperator.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperator.hpp>
#include <Operators/Operator.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <set>
#include <utility>

namespace NES::Optimizer {

ExecutionNodePtr ExecutionNode::create(WorkerId workerId) { return std::make_shared<ExecutionNode>(workerId); }

ExecutionNode::ExecutionNode(WorkerId workerId) : workerId(workerId) {}

bool ExecutionNode::registerDecomposedQueryPlan(const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    auto sharedQueryId = decomposedQueryPlan->getSharedQueryId();
    auto decomposedQueryId = decomposedQueryPlan->getDecomposedQueryId();
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
        mapOfSharedQueryToDecomposedQueryPlans[sharedQueryId][decomposedQueryPlan->getDecomposedQueryId()] = copiedDecomposedPlan;
    }
    NES_DEBUG("ExecutionNode: Updated the decomposed query plan with id : {} to the collection of query sub plans",
              sharedQueryId);
}

std::vector<DecomposedQueryPlanPtr> ExecutionNode::getAllDecomposedQueryPlans(SharedQueryId sharedQueryId) const {
    std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans;
    if (mapOfSharedQueryToDecomposedQueryPlans.contains(sharedQueryId)) {
        NES_DEBUG("ExecutionNode : Found query sub plan with id  {}", sharedQueryId);
        auto decomposedQueryPlanMap = mapOfSharedQueryToDecomposedQueryPlans.at(sharedQueryId);
        for (const auto& [decomposedQueryId, decomposedQueryPlan] : decomposedQueryPlanMap) {
            decomposedQueryPlans.emplace_back(decomposedQueryPlan);
        }
        return decomposedQueryPlans;
    }
    NES_ERROR("ExecutionNode : Unable to find shared query plan with id {}", sharedQueryId);
    return decomposedQueryPlans;
}

DecomposedQueryPlanPtr ExecutionNode::getDecomposedQueryPlan(SharedQueryId sharedQueryId,
                                                             DecomposedQueryId decomposedQueryId) const {
    if (mapOfSharedQueryToDecomposedQueryPlans.contains(sharedQueryId)) {
        NES_DEBUG("ExecutionNode : Found shared query plan with id  {}", sharedQueryId);
        auto decomposedQueryPlanMap = mapOfSharedQueryToDecomposedQueryPlans.at(sharedQueryId);
        if (decomposedQueryPlanMap.contains(decomposedQueryId)) {
            return decomposedQueryPlanMap[decomposedQueryId];
        }
        NES_ERROR("ExecutionNode: Unable to find decomposed query plan with id {}", decomposedQueryId);
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

bool ExecutionNode::removeDecomposedQueryPlan(SharedQueryId sharedQueryId, DecomposedQueryId decomposedQueryId) {

    //Check if the map contains an entry for the shared query id
    if (mapOfSharedQueryToDecomposedQueryPlans.contains(sharedQueryId)) {
        auto decomposedQueryPlanMap = mapOfSharedQueryToDecomposedQueryPlans[sharedQueryId];
        //Check if query sub plan exists in the map
        if (decomposedQueryPlanMap.contains(decomposedQueryId)) {
            decomposedQueryPlanMap.erase(decomposedQueryId);
            //If no query sub plan exist then remove the entry from the mapOfSharedQueryToQuerySubPlans
            if (decomposedQueryPlanMap.empty()) {
                mapOfSharedQueryToDecomposedQueryPlans.erase(sharedQueryId);
            }
            mapOfSharedQueryToDecomposedQueryPlans[sharedQueryId] = decomposedQueryPlanMap;
            return true;
        }
    }
    return false;
}

std::string ExecutionNode::toString() const { return fmt::format("ExecutionNode(id:{})", workerId); }

WorkerId ExecutionNode::getId() const { return workerId; }

PlacedDecomposedQueryPlans ExecutionNode::getAllQuerySubPlans() { return mapOfSharedQueryToDecomposedQueryPlans; }

bool ExecutionNode::equal(NodePtr const& rhs) const { return rhs->as<ExecutionNode>()->getId() == workerId; }

std::vector<std::string> ExecutionNode::toMultilineString() {
    std::vector<std::string> lines;
    lines.push_back(toString());

    for (const auto& mapOfQuerySubPlan : mapOfSharedQueryToDecomposedQueryPlans) {
        for (const auto& [querySubPlanId, querySubPlan] : mapOfQuerySubPlan.second) {
            lines.push_back(
                fmt::format("QuerySubPlan(SharedQueryId:{}, DecomposedQueryId:{}, DecomposedQueryVersion:{}, queryState:{})",
                            mapOfQuerySubPlan.first,
                            querySubPlan->getDecomposedQueryId(),
                            querySubPlan->getVersion(),
                            magic_enum::enum_name(querySubPlan->getState())));

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
