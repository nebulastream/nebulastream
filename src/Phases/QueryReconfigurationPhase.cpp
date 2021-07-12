/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Exceptions/QueryDeploymentException.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Operators/LogicalOperators/Sinks/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Sinks/SinkLogicalOperatorNode.hpp>
#include <Operators/LogicalOperators/Sources/NetworkSourceDescriptor.hpp>
#include <Operators/LogicalOperators/Sources/SourceLogicalOperatorNode.hpp>
#include <Phases/QueryReconfigurationPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlan.hpp>
#include <Plans/Global/Query/SharedQueryPlanChangeLog.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Plans/Query/QueryReconfigurationPlan.hpp>
#include <Plans/Utils/PlanIdGenerator.hpp>
#include <Plans/Utils/QueryPlanIterator.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <WorkerRPCService.grpc.pb.h>
#include <utility>
namespace NES {

QueryReconfigurationPhase::QueryReconfigurationPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                                     WorkerRPCClientPtr workerRpcClient)
    : workerRPCClient(std::move(workerRpcClient)), globalExecutionPlan(std::move(globalExecutionPlan)) {
    NES_DEBUG("QueryReconfigurationPhase()");
}

QueryReconfigurationPhasePtr QueryReconfigurationPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                               WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryReconfigurationPhase>(
        QueryReconfigurationPhase(std::move(globalExecutionPlan), std::move(workerRpcClient)));
}

bool QueryReconfigurationPhase::execute(const SharedQueryPlanPtr& sharedPlan) {
    auto queryPlan = sharedPlan->getQueryPlan();
    auto queryId = queryPlan->getQueryId();

    NES_DEBUG("QueryReconfigurationPhase: deploy the query");

    SharedQueryPlanChangeLogPtr changeLog = sharedPlan->getChangeLog();

    mapOperatorToSubPlan(queryId);

    mapNetworkSourcesToSinks();

    std::set<QueryPlanPtr> subPlansContainingMergePoints;

    for (const auto& removal : changeLog->getRemoval()) {
        OperatorId childIdOfOperatorRemoved = removal.first->getId();
        if (!queryPlan->hasOperatorWithId(childIdOfOperatorRemoved)) {
            continue;
        }
        subPlansContainingMergePoints.insert(operatorToSubPlan[childIdOfOperatorRemoved]);
    }

    for (const auto& addition : changeLog->getAddition()) {
        OperatorId childIdOfOperatorAdded = addition.first->getId();
        subPlansContainingMergePoints.insert(operatorToSubPlan[childIdOfOperatorAdded]);
    }

    std::set<QueryPlanPtr> querySubPlansToStop =
        plansAboveMergePoint(changeLog->getRemovedSinks(), subPlansContainingMergePoints);

    std::set<QueryPlanPtr> querySubPlansToStart = plansAboveMergePoint(changeLog->getAddedSinks(), subPlansContainingMergePoints);

    // removal of chain from merge point to pointer removed pending (placement after deletion)

    for (const auto& subPlanContainingMergePoint : subPlansContainingMergePoints) {
        std::vector<QueryPlanPtr> subPlanChains{subPlanContainingMergePoint};
        while (!subPlanChains.empty()) {
            auto planToProcess = subPlanChains.back();
            subPlanChains.pop_back();
            shadowSubPlans.insert(planToProcess);
            auto sources = planToProcess->getSourceOperators();
            for (const auto& source : sources) {
                const auto& found = networkSourcesToSinks.find(source);
                if (found != networkSourcesToSinks.end()) {
                    for (const auto& sinkSendingDataToSource : found->second) {
                        subPlanChains.emplace_back(operatorToSubPlan[sinkSendingDataToSource->getId()]);
                    }
                }
            }
        }
    }

    // reassign network operator Ids for plans below plan containing operator with modification
    for (const auto& shadowSubPlan : shadowSubPlans) {
        if (subPlansContainingMergePoints.contains(shadowSubPlan)) {
            continue;
        }
        for (const auto& sink : shadowSubPlan->getSinkOperators()) {
            const SinkDescriptorPtr& sinkDesc = sink->getSinkDescriptor();
            if (sinkDesc->instanceOf<Network::NetworkSinkDescriptor>()) {
                auto oldNetSinkDesc = sinkDesc->as<Network::NetworkSinkDescriptor>();
                auto oldNesPartition = oldNetSinkDesc->getNesPartition();
                auto oldNetSrcOperatorId = oldNesPartition.getOperatorId();
                auto netSrcSubPlan = operatorToSubPlan[oldNetSrcOperatorId];
                auto newNetSrcOperatorId = UtilityFunctions::getNextOperatorId();
                const SourceLogicalOperatorNodePtr srcNetOperator =
                    netSrcSubPlan->getOperatorWithId(oldNetSrcOperatorId)->as<SourceLogicalOperatorNode>();
                auto oldSrcDescriptor = srcNetOperator->getSourceDescriptor();
                srcNetOperator->setId(newNetSrcOperatorId);
                const Network::NesPartition& newPartition = Network::NesPartition(oldNesPartition.getQueryId(),
                                                                                  newNetSrcOperatorId,
                                                                                  oldNesPartition.getPartitionId(),
                                                                                  oldNesPartition.getSubpartitionId());
                srcNetOperator->as<SourceLogicalOperatorNode>()->setSourceDescriptor(
                    Network::NetworkSourceDescriptor::create(oldSrcDescriptor->getSchema(), newPartition));
                auto newSinkDesc = Network::NetworkSinkDescriptor::create(oldNetSinkDesc->getNodeLocation(),
                                                                          newPartition,
                                                                          oldNetSinkDesc->getWaitTime(),
                                                                          oldNetSinkDesc->getRetryTimes());
                sink->setSinkDescriptor(newSinkDesc);
                sink->setId(UtilityFunctions::getNextOperatorId());
                operatorToSubPlan[newNetSrcOperatorId] = netSrcSubPlan;
                operatorToSubPlan[sink->getId()] = shadowSubPlan;
                shadowSubPlans.insert(netSrcSubPlan);
            }
        }
    }

    for (const auto& source : queryPlan->getSourceOperators()) {
        populateReconfigurationPlan(queryId, source->getId(), DATA_SOURCE);
    }

    for (const auto& newSinkOperatorId : changeLog->getAddedSinks()) {
        auto sinkSubPlan = operatorToSubPlan[newSinkOperatorId];
        if (subPlansContainingMergePoints.contains(sinkSubPlan)) {
            populateReconfigurationPlan(queryId, newSinkOperatorId, DATA_SINK);
        }
    }

    for (const auto& subPlan : reconfigurationShadowPlans) {
        querySubPlansToStart.erase(subPlan);
        querySubPlansToStop.erase(subPlan);
    }

    for (const auto& shadowSubPlan : shadowSubPlans) {
        shadowSubPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
    }

    // Remove operator in query subplan
    removeDeletedOperators(queryPlan, changeLog, querySubPlansToStop);

    NES_DEBUG("QueryReconfigurationPhase: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());

    querySubPlansToStart.insert(shadowSubPlans.begin(), shadowSubPlans.end());

    deployQuery(queryId, querySubPlansToStart);
    registerForReconfiguration(queryId);
    triggerReconfigurationOfType(queryId, DATA_SINK);
    startQuery(queryId, querySubPlansToStart);
    triggerReconfigurationOfType(queryId, DATA_SOURCE);
    stopQuerySubPlan(queryId, querySubPlansToStop);
    unregisterQuerySubPlan(queryId, querySubPlansToStop);

    if (!querySubPlansToStop.empty()) {
        NES_DEBUG("QueryReconfigurationPhase: Update Global Execution Plan After removing sub plans to stop: \n"
                  << globalExecutionPlan->getAsString());
    }

    operatorToSubPlan.clear();
    subPlanToExecutionNode.clear();
    shadowSubPlans.clear();
    reconfigurationShadowPlans.clear();
    executionNodeToReconfigurationPlans.clear();
    networkSourcesToSinks.clear();

    return true;
}

void QueryReconfigurationPhase::removeDeletedOperators(QueryPlanPtr& queryPlan,
                                                       const SharedQueryPlanChangeLogPtr& changeLog,
                                                       const std::set<QueryPlanPtr>& querySubPlansToStop) {
    for (const auto& removal : changeLog->getRemoval()) {
        OperatorId childIdOfOperatorRemoved = removal.first->getId();
        if (!queryPlan->hasOperatorWithId(childIdOfOperatorRemoved)) {
            continue;
        }
        auto subPlanContainingModifiedOperator = operatorToSubPlan[childIdOfOperatorRemoved];
        for (auto removedOperator : removal.second) {
            auto subPlanContainingRemovedOperator = operatorToSubPlan[removedOperator];
            if (subPlanContainingModifiedOperator->getQuerySubPlanId() == subPlanContainingRemovedOperator->getQuerySubPlanId()) {
                auto removedOperatorInSubPlan = subPlanContainingModifiedOperator->getOperatorWithId(removedOperator);
                if (removedOperatorInSubPlan == nullptr) {
                    continue;
                }
                for (const auto& rootNode : removedOperatorInSubPlan->getAllRootNodes()) {
                    if (rootNode->getChildren().size() <= 1) {
                        subPlanContainingRemovedOperator->removeAsRootOperator(rootNode->as<LogicalOperatorNode>());
                    }
                }
                removedOperatorInSubPlan->removeChildren();
                for (const auto& operatorToRemove : removedOperatorInSubPlan->getAndFlattenAllAncestors()) {
                    operatorToRemove->removeAllParent();
                }
            } else {
                auto operatorBeingModified = subPlanContainingModifiedOperator->getOperatorWithId(childIdOfOperatorRemoved);
                for (const auto& root : subPlanContainingModifiedOperator->getSinkOperators()) {
                    if (root->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
                        auto sinkDesc = root->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>();
                        auto subPlanSinkWritesTo = operatorToSubPlan[sinkDesc->getNesPartition().getOperatorId()];
                        if (querySubPlansToStop.contains(subPlanSinkWritesTo)) {
                            root->removeChild(operatorBeingModified);
                            if (root->getChildren().empty()) {
                                subPlanContainingModifiedOperator->removeAsRootOperator(root);
                            }
                        }
                    }
                }
            }
        }
    }
}

std::set<QueryPlanPtr>
QueryReconfigurationPhase::plansAboveMergePoint(const std::vector<uint64_t>& sinkOperatorIds,
                                                const std::set<QueryPlanPtr>& subPlansContainingMergePoints) {
    std::set<QueryPlanPtr> plansAboveMergePoint;
    for (auto sinkOperatorIdRemoved : sinkOperatorIds) {
        std::vector<QueryPlanPtr> subPlanChains{operatorToSubPlan[sinkOperatorIdRemoved]};
        while (!subPlanChains.empty()) {
            auto subPlanToProcess = subPlanChains.back();
            subPlanChains.pop_back();
            if (!subPlansContainingMergePoints.contains(subPlanToProcess)) {
                plansAboveMergePoint.insert(subPlanToProcess);
                auto sources = subPlanToProcess->getSourceOperators();
                for (const auto& source : sources) {
                    const auto& found = networkSourcesToSinks.find(source);
                    if (found != networkSourcesToSinks.end()) {
                        for (const auto& sinkSendingDataToSource : found->second) {
                            subPlanChains.emplace_back(operatorToSubPlan[sinkSendingDataToSource->getId()]);
                        }
                    }
                }
            }
        }
    }
    return plansAboveMergePoint;
}

void QueryReconfigurationPhase::mapNetworkSourcesToSinks() {
    for (const auto& entry : subPlanToExecutionNode) {
        auto querySubPlan = entry.first;
        for (const auto& sink : querySubPlan->getSinkOperators()) {
            if (sink->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
                auto desc = sink->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>();
                auto netSrcOperatorId = desc->getNesPartition().getOperatorId();
                auto netSrcSubPlan = operatorToSubPlan[netSrcOperatorId];
                auto netSrcOperator = netSrcSubPlan->getOperatorWithId(netSrcOperatorId)->as<SourceLogicalOperatorNode>();
                auto found = networkSourcesToSinks.find(netSrcOperator);
                if (found == networkSourcesToSinks.end()) {
                    networkSourcesToSinks[netSrcOperator] = {sink};
                } else {
                    (*found).second.insert(sink);
                }
            }
        }
    }
}
void QueryReconfigurationPhase::mapOperatorToSubPlan(QueryId queryId) {
    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);

    for (const auto& executionNode : executionNodes) {
        for (const auto& subPlan : executionNode->getQuerySubPlans(queryId)) {
            auto nodes = QueryPlanIterator(subPlan).snapshot();
            for (const auto& node : nodes) {
                operatorToSubPlan[node->as<OperatorNode>()->getId()] = subPlan;
            }
            subPlanToExecutionNode[subPlan] = executionNode;
        }
    }
}

void QueryReconfigurationPhase::populateReconfigurationPlan(QueryId queryId,
                                                            const OperatorId operatorId,
                                                            QueryReconfigurationTypes reconfigurationType) {
    auto subPlan = operatorToSubPlan[operatorId];
    if (reconfigurationShadowPlans.contains(subPlan)) {
        return;
    }
    reconfigurationShadowPlans.insert(subPlan);
    shadowSubPlans.erase(subPlan);
    auto srcExecutionNode = subPlanToExecutionNode[subPlan];
    auto newSubPlanId = PlanIdGenerator::getNextQuerySubPlanId();
    auto reconfigurationPlan =
        QueryReconfigurationPlan::create(queryId, reconfigurationType, subPlan->getQuerySubPlanId(), newSubPlanId);
    auto found = executionNodeToReconfigurationPlans.find(srcExecutionNode);
    if (found == executionNodeToReconfigurationPlans.end()) {
        executionNodeToReconfigurationPlans[srcExecutionNode] = {reconfigurationPlan};
    } else {
        (*found).second.push_back(reconfigurationPlan);
    }
    subPlan->setQuerySubPlanId(newSubPlanId);
}

bool QueryReconfigurationPhase::registerForReconfiguration(QueryId queryId) {
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const auto& shadowPlan : reconfigurationShadowPlans) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        std::string rpcAddress = getRpcAddress(subPlanToExecutionNode[shadowPlan]);
        workerRPCClient->registerQueryForReconfigurationAsync(rpcAddress, shadowPlan, queueForExecutionNode);
        NES_DEBUG("QueryReconfigurationPhase:registerForReconfiguration: Registering querySubPlan"
                  << shadowPlan->getQuerySubPlanId() << " to node: " << rpcAddress << " ");
        completionQueues[queueForExecutionNode] = 1;
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, RegisterForReconfiguration);

    NES_DEBUG("QueryReconfigurationPhase::registerForReconfiguration: Finished deploying execution plan for query with Id "
              << queryId << " success=" << result);
    return result;
}

bool QueryReconfigurationPhase::triggerReconfigurationOfType(QueryId queryId, QueryReconfigurationTypes reconfigurationType) {
    std::map<CompletionQueuePtr, uint64_t> reconfigurationCompletionQueues;
    for (const auto& reconfiguration : executionNodeToReconfigurationPlans) {
        auto reconfigurationPlans = reconfiguration.second;
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();
        std::string rpcAddress = getRpcAddress(reconfiguration.first);
        auto plansReconfigured = 0;
        for (const auto& reconfigurationPlan : reconfigurationPlans) {
            if (reconfigurationPlan->getReconfigurationType() == reconfigurationType) {
                plansReconfigured++;
                workerRPCClient->triggerReconfigurationAsync(rpcAddress, reconfigurationPlan, queueForExecutionNode);
                NES_DEBUG("QueryReconfigurationPhase:triggerReconfiguration: ReconfigurationId: "
                          << reconfigurationPlan->getId() << " to node: " << rpcAddress << " successful");
            }
        }
        if (plansReconfigured > 0) {
            reconfigurationCompletionQueues[queueForExecutionNode] = plansReconfigured;
        }
    }

    bool result = workerRPCClient->checkAsyncResult(reconfigurationCompletionQueues, TriggerReconfiguration);

    NES_DEBUG("QueryReconfigurationPhase::triggerReconfigurationOfType: Finished triggering reconfiguration of type: "
              << reconfigurationType << " for for query with Id " << queryId << " success=" << result);
    return result;
}

bool QueryReconfigurationPhase::deployQuery(QueryId queryId, const std::set<QueryPlanPtr>& querySubPlans) {
    if (querySubPlans.empty()) {
        return true;
    }
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const auto& querySubPlan : querySubPlans) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        std::string rpcAddress = getRpcAddress(subPlanToExecutionNode[querySubPlan]);
        workerRPCClient->registerQueryAsync(rpcAddress, querySubPlan, queueForExecutionNode);
        NES_DEBUG("QueryReconfigurationPhase::deployQuery: Registering  subQueryPlan with Id "
                  << querySubPlan->getQuerySubPlanId() << " in node: " << rpcAddress);
        completionQueues[queueForExecutionNode] = 1;
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, Register);

    NES_DEBUG("QueryReconfigurationPhase::deployQuery: Finished deploying execution plan for query with Id "
              << queryId << " success=" << result);
    return result;
}

std::string QueryReconfigurationPhase::getRpcAddress(const ExecutionNodePtr& executionNode) {
    const auto& nesNode = executionNode->getTopologyNode();
    auto ipAddress = nesNode->getIpAddress();
    auto grpcPort = nesNode->getGrpcPort();
    std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
    return rpcAddress;
}

bool QueryReconfigurationPhase::stopQuerySubPlan(QueryId queryId, const std::set<QueryPlanPtr>& querySubPlans) {
    if (querySubPlans.empty()) {
        return true;
    }
    NES_DEBUG("QueryReconfigurationPhase::stopQuerySubPlan: queryId=" << queryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const auto& subPlan : querySubPlans) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();
        auto executionNode = subPlanToExecutionNode[subPlan];
        std::string rpcAddress = getRpcAddress(executionNode);
        workerRPCClient->stopQuerySubPlanAsync(rpcAddress, subPlan->getQuerySubPlanId(), queueForExecutionNode);
        NES_DEBUG("QueryReconfigurationPhase::stopQuerySubPlan: subQueryId=" << subPlan->getQuerySubPlanId());
        completionQueues[queueForExecutionNode] = 1;
        executionNode->removeQuerySubPlan(subPlan);
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, StopSubPlan);
    if (!result) {
        NES_ERROR("QueryReconfigurationPhase::stopQuerySubPlan: Failed for Query: " << queryId);
    }
    return result;
}

bool QueryReconfigurationPhase::unregisterQuerySubPlan(QueryId queryId, const std::set<QueryPlanPtr>& querySubPlans) {
    if (querySubPlans.empty()) {
        return true;
    }
    NES_DEBUG("QueryReconfigurationPhase::stopQuerySubPlan: queryId=" << queryId);
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const auto& subPlan : querySubPlans) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();
        auto executionNode = subPlanToExecutionNode[subPlan];
        std::string rpcAddress = getRpcAddress(executionNode);
        workerRPCClient->unregisterQuerySubPlanAsync(rpcAddress, subPlan->getQuerySubPlanId(), queueForExecutionNode);
        completionQueues[queueForExecutionNode] = 1;
        executionNode->removeQuerySubPlan(subPlan);
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, UnregisterSubPlan);
    if (!result) {
        NES_ERROR("QueryReconfigurationPhase::unRegisterQuerySubPlan: Failed for Query: " << queryId);
    }
    return result;
}

bool QueryReconfigurationPhase::startQuery(QueryId queryId, const std::set<QueryPlanPtr>& querySubPlans) {
    if (querySubPlans.empty()) {
        return true;
    }
    NES_DEBUG("QueryReconfigurationPhase::startQuery queryId=" << queryId);
    //TODO: check if one queue can be used among multiple connections
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    std::set<ExecutionNodePtr> executionNodes;

    for (const auto& subPlan : querySubPlans) {
        executionNodes.insert(subPlanToExecutionNode[subPlan]);
    }
    for (const auto& executionNode : executionNodes) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();
        std::string rpcAddress = getRpcAddress(executionNode);
        bool success = workerRPCClient->startQueryAsyn(rpcAddress, queryId, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryReconfigurationPhase::startQuery: " << queryId << " to " << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryReconfigurationPhase::startQuery: " << queryId << " to " << rpcAddress << "  failed");
            return false;
        }
        completionQueues[queueForExecutionNode] = 1;
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, Start);
    NES_DEBUG("QueryReconfigurationPhase::startQuery: Finished starting execution plan for query with Id "
              << queryId << " success=" << result);
    return result;
}

}// namespace NES