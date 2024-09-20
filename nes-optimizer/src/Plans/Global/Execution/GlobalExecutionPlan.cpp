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
#include <Identifiers/NESStrongTypeJson.hpp>
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
    WorkerId workerId = lockedTopologyNode->operator*()->getId();

    if (!idToExecutionNodeMap.contains(workerId)) {

        NES_WARNING("No execution node found with the id {}. Creating a new one.", workerId);

        //Otherwise, create a new execution node and register the decomposed query plan to it.
        auto newExecutionNode = ExecutionNode::create(workerId);
        // Add child execution nodes
        for (const auto& childTopologyNode : lockedTopologyNode->operator*()->getChildren()) {
            auto childWorkerId = childTopologyNode->as<TopologyNode>()->getId();
            if (idToExecutionNodeMap.contains(childWorkerId)) {
                auto lockedChildExecutionNode = idToExecutionNodeMap.at(childWorkerId).wlock();
                (*lockedChildExecutionNode)->addParent(newExecutionNode);
            }
        }
        //Add as root execution node
        if (lockedTopologyNode->operator*()->getParents().empty()) {
            addExecutionNodeAsRoot(workerId);
        } else {
            // Add parent execution nodes
            for (const auto& parentTopologyNode : lockedTopologyNode->operator*()->getParents()) {
                auto parentWorkerId = parentTopologyNode->as<TopologyNode>()->getId();
                if (idToExecutionNodeMap.contains(parentWorkerId)) {
                    auto lockedParentExecutionNode = idToExecutionNodeMap.at(parentWorkerId).wlock();
                    (*lockedParentExecutionNode)->addChild(newExecutionNode);
                }
            }
        }

        idToExecutionNodeMap[workerId] = newExecutionNode;
        NES_DEBUG("Added execution node with id {} ", workerId);
        return true;
    }
    NES_WARNING("Execution node {} already exists", workerId);
    return false;
}

bool GlobalExecutionPlan::addDecomposedQueryPlan(const WorkerId& workerId, DecomposedQueryPlanPtr decomposedQueryPlan) {

    NES_DEBUG("Adding decomposed query plan {}-{} to the execution node with id  {}",
              decomposedQueryPlan->getSharedQueryId(),
              decomposedQueryPlan->getDecomposedQueryId(),
              workerId);
    SharedQueryId sharedQueryId = decomposedQueryPlan->getSharedQueryId();
    if (idToExecutionNodeMap.contains(workerId)) {
        auto lockedExecutionNode = idToExecutionNodeMap[workerId].wlock();
        if ((*lockedExecutionNode)->registerDecomposedQueryPlan(decomposedQueryPlan)) {
            if (sharedQueryIdToWorkerIdMap->contains(sharedQueryId)) {
                auto wlock = sharedQueryIdToWorkerIdMap.wlock();
                auto workerIds = (*wlock)[sharedQueryId];
                std::set<WorkerId> newWorkerIds(workerIds.begin(), workerIds.end());
                newWorkerIds.emplace(workerId);
                (*wlock)[sharedQueryId] = newWorkerIds;
            } else {
                auto wlock = sharedQueryIdToWorkerIdMap.wlock();
                (*wlock)[sharedQueryId] = {workerId};
            }
            return true;
        }
    }
    return false;
}

bool GlobalExecutionPlan::updateDecomposedQueryPlans(const WorkerId& workerId,
                                                     std::vector<DecomposedQueryPlanPtr> decomposedQueryPlans) {

    NES_DEBUG("Adding decomposed query plans to the execution node with id  {}", workerId);
    if (idToExecutionNodeMap.contains(workerId)) {
        auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).wlock();
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            (*lockedExecutionNode)->registerDecomposedQueryPlan(decomposedQueryPlan);
        }
        return true;
    }
    return false;
}

bool GlobalExecutionPlan::updateDecomposedQueryPlanState(WorkerId workerId,
                                                         SharedQueryId sharedQueryId,
                                                         DecomposedQueryId decomposedQueryId,
                                                         DecomposedQueryPlanVersion expectedVersion,
                                                         QueryState newDecomposedQueryPlanState) {

    if (idToExecutionNodeMap.contains(workerId)) {
        NES_DEBUG("Returning execution node with id  {}", workerId);
        auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).wlock();
        auto decomposedPlan = (*lockedExecutionNode)->getDecomposedQueryPlan(sharedQueryId, decomposedQueryId);
        if (!decomposedPlan) {
            NES_ERROR("No decomposed query plan with the id {} found.", decomposedQueryId);
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
    NES_ERROR("No execution node with id {} exists.", workerId);
    return false;
}

std::set<SharedQueryId> GlobalExecutionPlan::getPlacedSharedQueryIds(WorkerId workerId) const {

    if (idToExecutionNodeMap.contains(workerId)) {
        NES_DEBUG("Returning execution node with id  {}", workerId);
        auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).wlock();
        return (*lockedExecutionNode)->getPlacedSharedQueryPlanIds();
    }
    NES_ERROR("Unable to find execution node {}", workerId);
    return {};
}

DecomposedQueryPlanPtr GlobalExecutionPlan::getCopyOfDecomposedQueryPlan(WorkerId workerId,
                                                                         SharedQueryId sharedQueryId,
                                                                         DecomposedQueryId decomposedQueryId) {

    if (idToExecutionNodeMap.contains(workerId)) {
        NES_DEBUG("Returning execution node with id  {}", workerId);
        auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).wlock();
        const auto& decomposedQueryPlan = (*lockedExecutionNode)->getDecomposedQueryPlan(sharedQueryId, decomposedQueryId);
        if (decomposedQueryPlan) {
            return decomposedQueryPlan->copy();
        }
        return nullptr;
    }
    NES_ERROR("Unable to find execution node {}", workerId);
    return nullptr;
}

std::vector<DecomposedQueryPlanPtr> GlobalExecutionPlan::getCopyOfAllDecomposedQueryPlans(WorkerId workerId,
                                                                                          SharedQueryId sharedQueryId) {

    if (idToExecutionNodeMap.contains(workerId)) {
        NES_DEBUG("Returning execution node with id  {}", workerId);
        auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).wlock();
        const auto& decomposedQueryPlans = (*lockedExecutionNode)->getAllDecomposedQueryPlans(sharedQueryId);
        std::vector<DecomposedQueryPlanPtr> copiedDecomposedQueryPlans;
        for (const auto& decomposedQueryPlan : decomposedQueryPlans) {
            copiedDecomposedQueryPlans.emplace_back(decomposedQueryPlan->copy());
        }
        return copiedDecomposedQueryPlans;
    }
    NES_ERROR("Unable to find execution node {}", workerId);
    return {};
}

ExecutionNodeWLock GlobalExecutionPlan::getLockedExecutionNode(WorkerId workerId) {
    NES_DEBUG("Lock the execution node with id  {}", workerId);
    if (idToExecutionNodeMap.contains(workerId)) {
        auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).tryWLock();
        //Try to acquire a write lock on the topology node
        if (lockedExecutionNode) {
            NES_DEBUG("Locked execution node with id  {}", workerId);
            return std::make_shared<folly::Synchronized<ExecutionNodePtr>::WLockedPtr>(std::move(lockedExecutionNode));
        }
        NES_ERROR("Unable to lock execution node {}", workerId);
        return nullptr;
    }
    NES_WARNING("Execution node doesn't exists with the id {}", workerId);
    return nullptr;
}

bool GlobalExecutionPlan::addExecutionNodeAsRoot(WorkerId WorkerId) {
    NES_DEBUG("Added Execution node as root node");
    auto lockedRootWorkerIds = rootWorkerIds.wlock();
    auto found = std::find(lockedRootWorkerIds->begin(), lockedRootWorkerIds->end(), WorkerId);
    if (found == lockedRootWorkerIds->end()) {
        lockedRootWorkerIds->emplace_back(WorkerId);
    } else {
        NES_WARNING("Execution node already present in the root node list");
    }
    return true;
}

bool GlobalExecutionPlan::removeExecutionNode(WorkerId workerId) {
    NES_DEBUG("Removing Execution node with id  {}", workerId);
    //Lock execution node map and root execution node id
    if (idToExecutionNodeMap.contains(workerId)) {
        NES_DEBUG("Removed execution node with id  {}", workerId);
        auto lockedRootWorkerIds = rootWorkerIds.wlock();
        auto found = std::find(lockedRootWorkerIds->begin(), lockedRootWorkerIds->end(), workerId);
        if (found != lockedRootWorkerIds->end()) {
            lockedRootWorkerIds->erase(found);
            //Release the lock
            lockedRootWorkerIds.unlock();
        }
        auto lockedExecutionNodeToRemove = idToExecutionNodeMap.at(workerId).wlock();
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
        return idToExecutionNodeMap.erase(workerId) == 1;
    }
    NES_DEBUG("Failed to remove Execution node with id  {}", workerId);
    return false;
}

bool GlobalExecutionPlan::removeAllDecomposedQueryPlans(SharedQueryId sharedQueryId) {
    NES_DEBUG("Removing all decomposed query plans for shared query {}", sharedQueryId);
    if (!sharedQueryIdToWorkerIdMap->contains(sharedQueryId)) {
        NES_DEBUG("No query with id {} exists in the system", sharedQueryId);
        return false;
    }

    auto wlock = sharedQueryIdToWorkerIdMap.wlock();
    auto workerIds = (*wlock)[sharedQueryId];
    NES_DEBUG("Found {} Execution node for shared query with id {}", workerIds.size(), sharedQueryId);
    for (const auto& workerId : workerIds) {
        auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).wlock();
        if (!(*lockedExecutionNode)->removeDecomposedQueryPlans(sharedQueryId)) {
            NES_ERROR("Unable to remove query sub plan with id {} from execution node with id {}", sharedQueryId, workerId);
            return false;
        }

        if ((*lockedExecutionNode)->getAllQuerySubPlans().empty()) {
            //Release all locks before node removal
            lockedExecutionNode.unlock();
            removeExecutionNode(workerId);
        }
    }
    sharedQueryIdToWorkerIdMap->erase(sharedQueryId);
    NES_DEBUG("Removed all Execution nodes for the shared query with id {}", sharedQueryId);
    return true;
}

bool GlobalExecutionPlan::removeDecomposedQueryPlan(NES::WorkerId workerId,
                                                    NES::SharedQueryId sharedQueryId,
                                                    NES::DecomposedQueryId decomposedQueryId,
                                                    NES::DecomposedQueryPlanVersion decomposedQueryPlanVersion) {

    NES_DEBUG("Removing decomposed query plan {} for shared query {}", decomposedQueryPlanVersion, sharedQueryId);

    if (!sharedQueryIdToWorkerIdMap->contains(sharedQueryId)) {
        NES_WARNING("No query with id {} exists.", sharedQueryId);
        return false;
    }

    if (!idToExecutionNodeMap.contains(workerId)) {
        NES_WARNING("No execution node with id {} exists.", sharedQueryId);
        return false;
    }

    auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).wlock();
    auto decomposedQueryPlan = (*lockedExecutionNode)->getDecomposedQueryPlan(sharedQueryId, decomposedQueryId);
    if (decomposedQueryPlan->getVersion() != decomposedQueryPlanVersion) {
        NES_WARNING("The current version {} of the decomposed query plan do not match with the input version {}.",
                    decomposedQueryPlan->getVersion(),
                    decomposedQueryPlanVersion);
        return false;
    }

    if ((*lockedExecutionNode)->removeDecomposedQueryPlan(sharedQueryId, decomposedQueryId)) {

        if (!(*lockedExecutionNode)->hasRegisteredDecomposedQueryPlans(sharedQueryId)) {

            auto wlock = sharedQueryIdToWorkerIdMap.wlock();
            auto workerIds = (*wlock)[sharedQueryId];
            workerIds.erase(workerId);
            if (workerIds.empty()) {
                wlock->erase(sharedQueryId);
            } else {
                (*wlock)[sharedQueryId] = workerIds;
            }
        }

        if ((*lockedExecutionNode)->getAllQuerySubPlans().empty()) {
            //Release all locks before node removal
            lockedExecutionNode.unlock();
            removeExecutionNode(workerId);
        }
        NES_DEBUG("Removed decomposed query plan {} for shared query {}", decomposedQueryPlanVersion, sharedQueryId);
        return true;
    }
    NES_WARNING("Failed to removed decomposed query plan {} for shared query {}", decomposedQueryPlanVersion, sharedQueryId);
    return false;
}

std::vector<ExecutionNodeWLock> GlobalExecutionPlan::getLockedExecutionNodesHostingSharedQueryId(SharedQueryId sharedQueryId) {

    if (sharedQueryIdToWorkerIdMap->contains(sharedQueryId)) {
        NES_DEBUG("Returning vector of Execution nodes for the shared query with id  {}", sharedQueryId);
        auto wlock = sharedQueryIdToWorkerIdMap.wlock();
        auto workerIds = (*wlock)[sharedQueryId];

        std::vector<ExecutionNodeWLock> lockedExecutionNodes;
        for (const auto& workerId : workerIds) {
            auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).wlock();
            lockedExecutionNodes.emplace_back(
                std::make_shared<folly::Synchronized<ExecutionNodePtr>::WLockedPtr>(std::move(lockedExecutionNode)));
        }
        return lockedExecutionNodes;
    }
    NES_WARNING("unable to find the Execution nodes for the shared query with id {}", sharedQueryId);
    return {};
}

std::string GlobalExecutionPlan::getAsString() {
    NES_DEBUG("Get Execution plan as string");
    //Lock execution node map and root execution node id
    auto lockedRootWorkerIds = rootWorkerIds.wlock();
    std::stringstream ss;
    auto dumpHandler = QueryConsoleDumpHandler::create(ss);

    auto rootIds = (*lockedRootWorkerIds);
    for (const auto& rootWorkerId : rootIds) {
        auto rootExecutionNode = idToExecutionNodeMap.at(rootWorkerId).wlock();
        dumpHandler->multilineDump((*rootExecutionNode));
    }
    return ss.str();
}

nlohmann::json GlobalExecutionPlan::getAsJson(SharedQueryId sharedQueryId) {
    NES_INFO("UtilityFunctions: getting execution plan as JSON");

    nlohmann::json executionPlanJson{};

    if (!sharedQueryIdToWorkerIdMap->contains(sharedQueryId)) {
        NES_DEBUG("No shared query with id {} exists.", sharedQueryId);
        return executionPlanJson;
    }

    std::vector<nlohmann::json> nodes = {};
    auto wlock = sharedQueryIdToWorkerIdMap.wlock();
    auto workerIds = (*wlock)[sharedQueryId];
    for (const auto& workerId : workerIds) {
        nlohmann::json currentExecutionNodeJsonValue{};
        auto lockedExecutionNode = idToExecutionNodeMap.at(workerId).wlock();
        currentExecutionNodeJsonValue["WorkerId"] = workerId;
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
            currentQuerySubPlan["DecomposedQueryId"] = decomposedQueryPlan->getDecomposedQueryId();

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
}// namespace NES::Optimizer
