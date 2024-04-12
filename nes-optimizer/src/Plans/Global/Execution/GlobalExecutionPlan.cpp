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
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryConsoleDumpHandler.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <algorithm>
#include <nlohmann/json.hpp>

namespace NES::Optimizer {

GlobalExecutionPlanPtr Optimizer::GlobalExecutionPlan::create() { return std::make_shared<GlobalExecutionPlan>(); }

bool GlobalExecutionPlan::registerExecutionNode(const TopologyNodeWLock& lockedTopologyNode) {
    ExecutionNodeId executionNodeId = lockedTopologyNode->operator*()->getId();

    if (!idToExecutionNodeMap.contains(executionNodeId)) {

        NES_WARNING("No execution node found with the id {}. Creating a new one.", executionNodeId);

        //Otherwise, create a new execution node and register the decomposed query plan to it.
        auto newExecutionNode = ExecutionNode::create(executionNodeId);
        // Add child execution nodes
        for (const auto& childTopologyNode : lockedTopologyNode->operator*()->getChildren()) {
            auto childExecutionNodeId = childTopologyNode->as<TopologyNode>()->getId();
            if (idToExecutionNodeMap.contains(childExecutionNodeId)) {
                auto lockedChildExecutionNode = idToExecutionNodeMap.at(childExecutionNodeId).wlock();
                (*lockedChildExecutionNode)->addParent(newExecutionNode);
            }
        }
        //Add as root execution node
        if (lockedTopologyNode->operator*()->getParents().empty()) {
            addExecutionNodeAsRoot(executionNodeId);
        } else {
            // Add parent execution nodes
            for (const auto& parentTopologyNode : lockedTopologyNode->operator*()->getParents()) {
                auto parentExecutionNodeId = parentTopologyNode->as<TopologyNode>()->getId();
                if (idToExecutionNodeMap.contains(parentExecutionNodeId)) {
                    auto lockedParentExecutionNode = idToExecutionNodeMap.at(parentExecutionNodeId).wlock();
                    (*lockedParentExecutionNode)->addChild(newExecutionNode);
                }
            }
        }

        idToExecutionNodeMap[executionNodeId] = newExecutionNode;
        NES_DEBUG("Added execution node with id {} ", executionNodeId);
        return true;
    }
    NES_WARNING("Execution node {} already exists", executionNodeId);
    return false;
}

bool GlobalExecutionPlan::addDecomposedQueryPlan(const ExecutionNodeId& executionNodeId,
                                                 DecomposedQueryPlanPtr decomposedQueryPlan) {

    NES_DEBUG("Adding decomposed query plan {}-{} to the execution node with id  {}",
              decomposedQueryPlan->getSharedQueryId(),
              decomposedQueryPlan->getDecomposedQueryPlanId(),
              executionNodeId);
    SharedQueryId sharedQueryId = decomposedQueryPlan->getSharedQueryId();
    if (idToExecutionNodeMap.contains(executionNodeId)) {
        auto lockedExecutionNode = idToExecutionNodeMap[executionNodeId].wlock();
        if ((*lockedExecutionNode)->registerDecomposedQueryPlan(decomposedQueryPlan)) {
            if (sharedQueryIdToExecutionNodeIdMap->contains(sharedQueryId)) {
                auto wlock = sharedQueryIdToExecutionNodeIdMap.wlock();
                auto executionNodeIds = (*wlock)[sharedQueryId];
                std::set<ExecutionNodeId> newExecutionNodeIds(executionNodeIds.begin(), executionNodeIds.end());
                newExecutionNodeIds.emplace(executionNodeId);
                (*wlock)[sharedQueryId] = newExecutionNodeIds;
            } else {
                auto wlock = sharedQueryIdToExecutionNodeIdMap.wlock();
                (*wlock)[sharedQueryId] = {executionNodeId};
            }
            return true;
        }
    }
    return false;
}

bool GlobalExecutionPlan::updateDecomposedQueryPlans(const ExecutionNodeId& executionNodeId,
                                                     std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans) {

    NES_DEBUG("Adding decomposed query plans to the execution node with id  {}", executionNodeId);
    if (idToExecutionNodeMap.contains(executionNodeId)) {
        auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).wlock();
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            (*lockedExecutionNode)->registerDecomposedQueryPlan(decomposedQueryPlan);
        }
        return true;
    }
    return false;
}

bool GlobalExecutionPlan::updateDecomposedQueryPlanState(ExecutionNodeId executionNodeId,
                                                         SharedQueryId sharedQueryId,
                                                         DecomposedQueryPlanId decomposedQueryPlanId,
                                                         DecomposedQueryPlanVersion expectedVersion,
                                                         QueryState newDecomposedQueryPlanState) {

    if (idToExecutionNodeMap.contains(executionNodeId)) {
        NES_DEBUG("Returning execution node with id  {}", executionNodeId);
        auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).wlock();
        auto decomposedPlan = (*lockedExecutionNode)->getDecomposedQueryPlan(sharedQueryId, decomposedQueryPlanId);
        if (!decomposedPlan) {
            NES_ERROR("No decomposed query plan with the id {} found.", decomposedQueryPlanId);
            return false;
        }
        if (decomposedPlan->getVersion() != expectedVersion) {
            NES_ERROR("Current {} and the expected version {} are different.",
                      magic_enum::enum_name(decomposedPlan->getState()),
                      expectedVersion);
            return false;
        }
        decomposedPlan->setState(newDecomposedQueryPlanState);
        return true;
    }
    NES_ERROR("No execution node with id {} exists.", executionNodeId);
    return false;
}

std::set<SharedQueryId> GlobalExecutionPlan::getPlacedSharedQueryIds(ExecutionNodeId executionNodeId) const {

    if (idToExecutionNodeMap.contains(executionNodeId)) {
        NES_DEBUG("Returning execution node with id  {}", executionNodeId);
        auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).wlock();
        return (*lockedExecutionNode)->getPlacedSharedQueryPlanIds();
    }
    NES_ERROR("Unable to find execution node {}", executionNodeId);
    return {};
}

DecomposedQueryPlanPtr GlobalExecutionPlan::getCopyOfDecomposedQueryPlan(ExecutionNodeId executionNodeId,
                                                                         SharedQueryId sharedQueryId,
                                                                         DecomposedQueryPlanId decomposedQueryPlanId) {

    if (idToExecutionNodeMap.contains(executionNodeId)) {
        NES_DEBUG("Returning execution node with id  {}", executionNodeId);
        auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).wlock();
        const auto& decomposedQueryPlan = (*lockedExecutionNode)->getDecomposedQueryPlan(sharedQueryId, decomposedQueryPlanId);
        if (decomposedQueryPlan) {
            return decomposedQueryPlan->copy();
        }
        return nullptr;
    }
    NES_ERROR("Unable to find execution node {}", executionNodeId);
    return nullptr;
}

std::vector<DecomposedQueryPlanPtr> GlobalExecutionPlan::getCopyOfAllDecomposedQueryPlans(ExecutionNodeId executionNodeId,
                                                                                          SharedQueryId sharedQueryId) {

    if (idToExecutionNodeMap.contains(executionNodeId)) {
        NES_DEBUG("Returning execution node with id  {}", executionNodeId);
        auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).wlock();
        const auto& decomposedQueryPlans = (*lockedExecutionNode)->getAllDecomposedQueryPlans(sharedQueryId);
        std::vector<DecomposedQueryPlanPtr> copiedDecomposedQueryPlans;
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            copiedDecomposedQueryPlans.emplace_back(decomposedQueryPlan->copy());
        }
        return copiedDecomposedQueryPlans;
    }
    NES_ERROR("Unable to find execution node {}", executionNodeId);
    return {};
}

ExecutionNodeWLock GlobalExecutionPlan::getLockedExecutionNode(ExecutionNodeId executionNodeId) {
    NES_DEBUG("Lock the execution node with id  {}", executionNodeId);
    if (idToExecutionNodeMap.contains(executionNodeId)) {
        auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).tryWLock();
        //Try to acquire a write lock on the topology node
        if (lockedExecutionNode) {
            NES_ERROR("Locked execution node with id  {}", executionNodeId);
            return std::make_shared<folly::Synchronized<ExecutionNodePtr>::WLockedPtr>(std::move(lockedExecutionNode));
        }
        NES_ERROR("Unable to lock execution node {}", executionNodeId);
        return nullptr;
    }
    NES_WARNING("Execution node doesn't exists with the id {}", executionNodeId);
    return nullptr;
}

bool GlobalExecutionPlan::addExecutionNodeAsRoot(ExecutionNodeId executionNodeId) {
    NES_DEBUG("Added Execution node as root node");
    auto lockedRootExecutionNodeIds = rootExecutionNodeIds.wlock();
    auto found = std::find(lockedRootExecutionNodeIds->begin(), lockedRootExecutionNodeIds->end(), executionNodeId);
    if (found == lockedRootExecutionNodeIds->end()) {
        lockedRootExecutionNodeIds->emplace_back(executionNodeId);
    } else {
        NES_WARNING("Execution node already present in the root node list");
    }
    return true;
}

bool GlobalExecutionPlan::removeExecutionNode(ExecutionNodeId executionNodeId) {
    NES_DEBUG("Removing Execution node with id  {}", executionNodeId);
    //Lock execution node map and root execution node id
    if (idToExecutionNodeMap.contains(executionNodeId)) {
        NES_DEBUG("Removed execution node with id  {}", executionNodeId);
        auto lockedRootExecutionNodeIds = rootExecutionNodeIds.wlock();
        auto found = std::find(lockedRootExecutionNodeIds->begin(), lockedRootExecutionNodeIds->end(), executionNodeId);
        if (found != lockedRootExecutionNodeIds->end()) {
            lockedRootExecutionNodeIds->erase(found);
            //Release the lock
            lockedRootExecutionNodeIds.unlock();
        }
        auto lockedExecutionNodeToRemove = idToExecutionNodeMap.at(executionNodeId).wlock();
        const auto& parentExecutionNodes = (*lockedExecutionNodeToRemove)->getParents();
        for (const auto& parentExecutionNode : parentExecutionNodes) {
            parentExecutionNode->removeChild((*lockedExecutionNodeToRemove));
        }
        const auto& childrenExecutionNodes = (*lockedExecutionNodeToRemove)->getChildren();
        for (const auto& childExecutionNode : childrenExecutionNodes) {
            childExecutionNode->removeParent((*lockedExecutionNodeToRemove));
        }
        //Unlock the execution node before removal
        lockedExecutionNodeToRemove.unlock();
        //Erase the execution node
        return idToExecutionNodeMap.erase(executionNodeId) == 1;
    }
    NES_DEBUG("Failed to remove Execution node with id  {}", executionNodeId);
    return false;
}

bool GlobalExecutionPlan::removeAllDecomposedQueryPlans(SharedQueryId sharedQueryId) {
    NES_DEBUG("Removing all decomposed query plans for shared query {}", sharedQueryId);
    if (!sharedQueryIdToExecutionNodeIdMap->contains(sharedQueryId)) {
        NES_DEBUG("No query with id {} exists in the system", sharedQueryId);
        return false;
    }

    auto wlock = sharedQueryIdToExecutionNodeIdMap.wlock();
    auto executionNodeIds = (*wlock)[sharedQueryId];
    NES_DEBUG("Found {} Execution node for shared query with id {}", executionNodeIds.size(), sharedQueryId);
    for (const auto& executionNodeId : executionNodeIds) {
        auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).wlock();
        if (!(*lockedExecutionNode)->removeDecomposedQueryPlans(sharedQueryId)) {
            NES_ERROR("Unable to remove query sub plan with id {} from execution node with id {}",
                      sharedQueryId,
                      executionNodeId);
            return false;
        }

        //        if ((*lockedExecutionNode)->getAllQuerySubPlans().empty()) {
        //            //Release all locks before node removal
        //            lockedExecutionNode.unlock();
        //            removeExecutionNode(executionNodeId);
        //        }
    }
    sharedQueryIdToExecutionNodeIdMap->erase(sharedQueryId);
    NES_DEBUG("Removed all Execution nodes for the shared query with id {}", sharedQueryId);
    return true;
}

bool GlobalExecutionPlan::removeDecomposedQueryPlan(NES::ExecutionNodeId executionNodeId,
                                                    NES::SharedQueryId sharedQueryId,
                                                    NES::DecomposedQueryPlanId decomposedQueryPlanId,
                                                    NES::DecomposedQueryPlanVersion decomposedQueryPlanVersion) {

    NES_DEBUG("Removing decomposed query plan {} for shared query {}", decomposedQueryPlanVersion, sharedQueryId);

    if (!sharedQueryIdToExecutionNodeIdMap->contains(sharedQueryId)) {
        NES_WARNING("No query with id {} exists.", sharedQueryId);
        return false;
    }

    if (!idToExecutionNodeMap.contains(executionNodeId)) {
        NES_WARNING("No execution node with id {} exists.", sharedQueryId);
        return false;
    }

    auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).wlock();
    auto decomposedQueryPlan = (*lockedExecutionNode)->getDecomposedQueryPlan(sharedQueryId, decomposedQueryPlanId);
    if (decomposedQueryPlan->getVersion() != decomposedQueryPlanVersion) {
        NES_WARNING("The current version {} of the decomposed query plan do not match with the input version {}.",
                    decomposedQueryPlan->getVersion(),
                    decomposedQueryPlanVersion);
        return false;
    }

    if ((*lockedExecutionNode)->removeDecomposedQueryPlan(sharedQueryId, decomposedQueryPlanId)) {

        if (!(*lockedExecutionNode)->hasRegisteredDecomposedQueryPlans(sharedQueryId)) {

            auto wlock = sharedQueryIdToExecutionNodeIdMap.wlock();
            auto executionNodeIds = (*wlock)[sharedQueryId];
            executionNodeIds.erase(executionNodeId);
            if (executionNodeIds.empty()) {
                wlock->erase(sharedQueryId);
            } else {
                (*wlock)[sharedQueryId] = executionNodeIds;
            }
        }

        //        if ((*lockedExecutionNode)->getAllQuerySubPlans().empty()) {
        //            //Release all locks before node removal
        //            lockedExecutionNode.unlock();
        //            removeExecutionNode(executionNodeId);
        //        }
        NES_DEBUG("Removed decomposed query plan {} for shared query {}", decomposedQueryPlanVersion, sharedQueryId);
        return true;
    }
    NES_WARNING("Failed to removed decomposed query plan {} for shared query {}", decomposedQueryPlanVersion, sharedQueryId);
    return false;
}

std::vector<ExecutionNodeWLock> GlobalExecutionPlan::getLockedExecutionNodesHostingSharedQueryId(SharedQueryId sharedQueryId) {

    if (sharedQueryIdToExecutionNodeIdMap->contains(sharedQueryId)) {
        NES_DEBUG("Returning vector of Execution nodes for the shared query with id  {}", sharedQueryId);
        auto wlock = sharedQueryIdToExecutionNodeIdMap.wlock();
        auto executionNodeIds = (*wlock)[sharedQueryId];

        std::vector<ExecutionNodeWLock> lockedExecutionNodes;
        for (const auto& executionNodeId : executionNodeIds) {
            auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).wlock();
            lockedExecutionNodes.emplace_back(
                std::make_shared<folly::Synchronized<ExecutionNodePtr>::WLockedPtr>(std::move(lockedExecutionNode)));
        }
        return lockedExecutionNodes;
    }
    NES_WARNING("unable to find the Execution nodes for the shared query with id {}", sharedQueryId);
    return {};
}

//bool GlobalExecutionPlan::removeQuerySubPlanFromNode(ExecutionNodeId executionNodeId,
//                                                     SharedQueryId sharedQueryId,
//                                                     DecomposedQueryPlanId decomposedQueryPlanId) {
//
//    //return false if no node with the given id could be found
//    if (idToExecutionNodeMap->contains(executionNodeId)) {
//        return false;
//    }
//
//    auto executionNode = idToExecutionNodeMap[executionNodeId];
//
//    //return false if no query sub plan with the given id was found at the node
//    if (!executionNode->removeDecomposedQueryPlan(sharedQueryId, decomposedQueryPlanId)) {
//        return false;
//    }
//
//    /* Check if the node still hosts query sub plans belonging the shared query with the given id. If not, remove
//     * the node from the vector of nodes associated with this shared query*/
//    if (executionNode->getAllDecomposedQueryPlans(sharedQueryId).empty()) {
//        auto& mappedNodes = sharedQueryIdToExecutionNodeIdMap[sharedQueryId];
//        if (mappedNodes.size() == 1) {
//            /* if this was the only node associated with this shared query id, remove the entry for this shared query
//             * from the index */
//            sharedQueryIdToExecutionNodeIdMap->erase(sharedQueryId);
//        } else {
//            /* if other nodes are still hosting sub queries of this shared query, remove only this node, from the list
//             * of nodes which host sub query plans of this shared query */
//            mappedNodes.erase(std::find(mappedNodes.begin(), mappedNodes.end(), executionNode));
//        }
//    }
//
//    // if the node does not host any query sub plans anymore, remove it
//    if (executionNode->getAllQuerySubPlans().empty()) {
//        removeExecutionNode(executionNodeId);
//    }
//    return true;
//}

std::string GlobalExecutionPlan::getAsString() {
    NES_DEBUG("Get Execution plan as string");
    //Lock execution node map and root execution node id
    auto lockedRootExecutionNodeIds = rootExecutionNodeIds.wlock();
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler::create(ss);

    auto rootIds = (*lockedRootExecutionNodeIds);
    for (const auto& rootExecutionNodeId : rootIds) {
        auto rootExecutionNode = idToExecutionNodeMap.at(rootExecutionNodeId).wlock();
        dumpHandler->multilineDump((*rootExecutionNode));
    }
    return ss.str();
}

nlohmann::json GlobalExecutionPlan::getAsJson(SharedQueryId sharedQueryId) {
    NES_INFO("UtilityFunctions: getting execution plan as JSON");

    nlohmann::json executionPlanJson{};

    if (!sharedQueryIdToExecutionNodeIdMap->contains(sharedQueryId)) {
        NES_DEBUG("No shared query with id {} exists.", sharedQueryId);
        return executionPlanJson;
    }

    std::vector<nlohmann::json> nodes = {};
    auto wlock = sharedQueryIdToExecutionNodeIdMap.wlock();
    auto executionNodeIds = (*wlock)[sharedQueryId];
    for (const auto& executionNodeId : executionNodeIds) {
        nlohmann::json currentExecutionNodeJsonValue{};
        auto lockedExecutionNode = idToExecutionNodeMap.at(executionNodeId).wlock();
        currentExecutionNodeJsonValue["ExecutionNodeId"] = executionNodeId;
        auto allDecomposedQueryPlans = (*lockedExecutionNode)->getAllDecomposedQueryPlans(sharedQueryId);
        if (allDecomposedQueryPlans.empty()) {
            continue;
        }
        nlohmann::json sharedQueryPlanToDecomposedQueryPlans{};
        sharedQueryPlanToDecomposedQueryPlans["SharedQueryId"] = sharedQueryId;

        std::vector<nlohmann::json> scheduledSubQueries;
        // loop over all query sub plans inside the current executionNode
        for (const auto& decomposedQueryPlan : allDecomposedQueryPlans) {

            // prepare json object to hold information on current query sub plan
            nlohmann::json currentQuerySubPlan{};

            // id of current query sub plan
            currentQuerySubPlan["DecomposedQueryPlanId"] = decomposedQueryPlan->getDecomposedQueryPlanId();

            // add the string containing operator to the json object of current query sub plan
            currentQuerySubPlan["DecomposedQueryPlan"] = decomposedQueryPlan->toString();

            scheduledSubQueries.push_back(currentQuerySubPlan);
        }
        sharedQueryPlanToDecomposedQueryPlans["DecomposedQueryPlans"] = scheduledSubQueries;
        currentExecutionNodeJsonValue["ScheduledDecomposedQueries"] = sharedQueryPlanToDecomposedQueryPlans;
        nodes.push_back(currentExecutionNodeJsonValue);
    }

    // add `executionNodes` JSON array to the final JSON result
    executionPlanJson["ExecutionNodes"] = nodes;
    return executionPlanJson;
}

//void GlobalExecutionPlan::scheduleExecutionNode(const ExecutionNodePtr& executionNode) {
//    NES_DEBUG("Schedule execution node for deployment");
//    auto found = std::find(executionNodesToSchedule.begin(), executionNodesToSchedule.end(), executionNode);
//    if (found != executionNodesToSchedule.end()) {
//        NES_DEBUG("Execution node {} marked as to be scheduled", executionNode->getId());
//        executionNodesToSchedule.push_back(executionNode);
//    } else {
//        NES_WARNING("Execution node {} already scheduled", executionNode->getId());
//    }
//    mapExecutionNodeToSharedQueryId(executionNode);
//}

/*void GlobalExecutionPlan::mapExecutionNodeToSharedQueryId(const ExecutionNodePtr& executionNode) {
    NES_DEBUG("Mapping execution node {} to the query Id index.", executionNode->getId());
    auto querySubPlanMap = executionNode->getAllQuerySubPlans();
    for (const auto& [sharedQueryId, querySubPlans] : querySubPlanMap) {
        if (!sharedQueryIdToExecutionNodeIdMap->contains(sharedQueryId)) {
            NES_DEBUG("Query Id {} does not exists adding a new entry with execution node {}",
                      sharedQueryId,
                      executionNode->getId());
            sharedQueryIdToExecutionNodeIdMap[sharedQueryId] = {executionNode};
        } else {
            auto executionNodes = sharedQueryIdToExecutionNodeIdMap[sharedQueryId];
            auto found = std::find(executionNodes.begin(), executionNodes.end(), executionNode);
            if (found == executionNodes.end()) {
                NES_DEBUG("Adding execution node {} to the query Id {}", executionNode->getId(), sharedQueryId);
                executionNodes.push_back(executionNode);
                sharedQueryIdToExecutionNodeIdMap[sharedQueryId] = executionNodes;
            } else {
                NES_DEBUG("Skipping as execution node {} already mapped to the query Id {}",
                          executionNode->getId(),
                          sharedQueryId);
            }
        }
    }
}*/

//std::map<WorkerId, uint32_t> GlobalExecutionPlan::getMapOfWorkerIdToOccupiedResource(SharedQueryId sharedQueryId) {
//    NES_INFO("Get a map of occupied resources for the shared query {}", sharedQueryId);
//    std::map<WorkerId, uint32_t> mapOfWorkerIdToOccupiedResources;
//    auto executionNodes = sharedQueryIdToExecutionNodeIdMap[sharedQueryId];
//    NES_DEBUG("Found {} Execution node for the shared query with id {}", executionNodes.size(), sharedQueryId);
//    for (auto& executionNode : executionNodes) {
//        uint32_t occupiedResource = executionNode->getOccupiedResources(sharedQueryId);
//        mapOfWorkerIdToOccupiedResources[executionNode->getTopologyNode()->getId()] = occupiedResource;
//    }
//    NES_DEBUG("returning the map of occupied resources for the shared query  {}", sharedQueryId);
//    return mapOfWorkerIdToOccupiedResources;
//}

}// namespace NES::Optimizer
