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

    std::set<SinkLogicalOperatorNodePtr> newlyAddedQuerySinks;
    std::set<QueryPlanPtr> subPlansContainingMergePoints;

    // Identify newly added sinks

    for (const auto& addition : changeLog->getAddition()) {
        subPlansContainingMergePoints.insert(operatorToSubPlan[addition.first->getId()]);
        for (auto newOperatorId : addition.second) {
            auto newOperator = queryPlan->getOperatorWithId(newOperatorId);
            for (const auto& rootOp : newOperator->getAllRootNodes()) {
                if (rootOp->instanceOf<SinkLogicalOperatorNode>()) {
                    newlyAddedQuerySinks.insert(rootOp->as<SinkLogicalOperatorNode>());
                }
            }
        }
    }

    std::set<QueryPlanPtr> querySubPlansToRemove;
    std::set<SinkLogicalOperatorNodePtr> sinksBeingRemoved;

    for (const auto& removal : changeLog->getRemoval()) {
        for (auto operatorToRemove : removal.second) {
            std::vector<SinkLogicalOperatorNodePtr> sinkParentsChain;
            bool mergePointEncountered = false;
            auto subPlanContainingMergeOperator = operatorToSubPlan[removal.first->getId()];
            auto subPlanWithOperatorBeingRemoved = operatorToSubPlan[operatorToRemove];
            // if operator being removed and child are in the same sub plan, then modify sub plan
            if (subPlanContainingMergeOperator == subPlanWithOperatorBeingRemoved) {
                auto operatorBeingRemoved = subPlanWithOperatorBeingRemoved->getOperatorWithId(operatorToRemove);
                auto sinksOfOperatorBeingRemoved = operatorBeingRemoved->getNodesByType<SinkLogicalOperatorNode>();
                operatorBeingRemoved->removeChildren();
                operatorBeingRemoved->removeAllParent();
                for (const auto& sinkOfOperatorBeingRemoved : sinksOfOperatorBeingRemoved) {
                    subPlanWithOperatorBeingRemoved->removeAsRootOperator(sinkOfOperatorBeingRemoved);
                    sinkParentsChain.insert(sinkParentsChain.end(),
                                            sinksOfOperatorBeingRemoved.begin(),
                                            sinksOfOperatorBeingRemoved.end());
                    mergePointEncountered = true;
                }
            } else {
                querySubPlansToRemove.insert(subPlanWithOperatorBeingRemoved);
            }
            // remove all subplans feeding from a plan recursively
            while (!sinkParentsChain.empty()) {
                auto sink = sinkParentsChain.back();
                sinkParentsChain.pop_back();
                if (sink->getSinkDescriptor()->instanceOf<Network::NetworkSinkDescriptor>()) {
                    auto sinkDesc = sink->getSinkDescriptor()->as<Network::NetworkSinkDescriptor>();
                    auto netSrcOperatorId = sinkDesc->getNesPartition().getOperatorId();
                    auto sourceSinks = operatorToSubPlan[netSrcOperatorId]->getSinkOperators();
                    sinkParentsChain.insert(sinkParentsChain.end(), sourceSinks.begin(), sourceSinks.end());
                } else {
                    sinksBeingRemoved.insert(sink);
                }
            }
            // unlinking point and removed operator are in different nodes, joined by network sources and sinks, have to remove those
            if (!mergePointEncountered) {
                std::vector<SourceLogicalOperatorNodePtr> sourcesChildrenChain;
                auto planSources = subPlanWithOperatorBeingRemoved->getSourceOperators();
                sourcesChildrenChain.insert(sourcesChildrenChain.end(), planSources.begin(), planSources.end());
                while (!sourcesChildrenChain.empty()) {
                    auto source = sourcesChildrenChain.back();
                    sourcesChildrenChain.pop_back();
                    for (const auto& netSink : networkSourcesToSinks[source]) {
                        auto subPlan = operatorToSubPlan[netSink->getId()];
                        // found sink used by operator to write data to operator being removed
                        if (subPlanContainingMergeOperator == subPlan) {
                            const auto& operatorBeingRemoved = netSink;
                            auto sinksOfOperatorBeingRemoved = operatorBeingRemoved->getNodesByType<SinkLogicalOperatorNode>();
                            operatorBeingRemoved->removeChildren();
                            operatorBeingRemoved->removeAllParent();
                            subPlan->removeAsRootOperator(netSink);
                        } else {
                            querySubPlansToRemove.insert(subPlan);
                        }
                    }
                }
            }
        }
    }

    std::set<QueryPlanPtr> newQuerySubPlans;

    for (const auto& sink : newlyAddedQuerySinks) {
        auto subPlanContainingSink = operatorToSubPlan[sink->getId()];
        auto querySinkPlanSources = subPlanContainingSink->getSourceOperators();
        std::vector<SourceLogicalOperatorNodePtr> sources{querySinkPlanSources.begin(), querySinkPlanSources.end()};
        newQuerySubPlans.insert(subPlanContainingSink);
        while (!sources.empty()) {
            auto source = sources.back();
            sources.pop_back();
            if (networkSourcesToSinks.contains(source)) {
                auto sinksWritingToSource = networkSourcesToSinks[source];
                for (const auto& sinkWritingToSource : sinksWritingToSource) {
                    auto sinkPlan = operatorToSubPlan[sinkWritingToSource->getId()];
                    if (subPlansContainingMergePoints.contains(sinkPlan)) {
                        continue;
                    }
                    newQuerySubPlans.insert(sinkPlan);
                    auto sinkSources = sinkPlan->getSourceOperators();
                    sources.insert(sources.end(), sinkSources.begin(), sinkSources.end());
                }
            }
        }
    }

    for (const auto& addition : changeLog->getAddition()) {
        std::vector<NodePtr> operatorChildChain{queryPlan->getOperatorWithId(addition.first->getId())};
        while (!operatorChildChain.empty()) {
            auto op = operatorChildChain.back()->as<OperatorNode>();
            operatorChildChain.pop_back();
            shadowSubPlans.insert(operatorToSubPlan[op->getId()]);
            auto opChildren = op->getChildren();
            operatorChildChain.insert(operatorChildChain.end(), opChildren.begin(), opChildren.end());
        }
    }

    for (const auto& removal : changeLog->getRemoval()) {
        std::vector<NodePtr> operatorChildChain{queryPlan->getOperatorWithId(removal.first->getId())};
        while (!operatorChildChain.empty()) {
            auto op = operatorChildChain.back()->as<OperatorNode>();
            operatorChildChain.pop_back();
            shadowSubPlans.insert(operatorToSubPlan[op->getId()]);
            auto opChildren = op->getChildren();
            operatorChildChain.insert(operatorChildChain.end(), opChildren.begin(), opChildren.end());
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
        populateReconfigurationPlan(queryId, source, DATA_SOURCE);
    }

    for (const auto& sink : newlyAddedQuerySinks) {
        auto sinkSubPlan = operatorToSubPlan[sink->getId()];
        if (subPlansContainingMergePoints.contains(sinkSubPlan)) {
            populateReconfigurationPlan(queryId, sink, DATA_SINK);
        }
    }

    for (const auto& subPlan : reconfigurationShadowPlans) {
        newQuerySubPlans.erase(subPlan);
        querySubPlansToRemove.erase(subPlan);
    }

    for (const auto& shadowSubPlan : shadowSubPlans) {
        shadowSubPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
    }

    NES_DEBUG("QueryReconfigurationPhase: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());

    std::set<QueryPlanPtr> querySubPlansToStart{newQuerySubPlans};
    querySubPlansToStart.insert(shadowSubPlans.begin(), shadowSubPlans.end());

    deployQuery(queryId, querySubPlansToStart);
    registerForReconfiguration(queryId);
    triggerReconfigurationOfType(queryId, DATA_SINK);
    startQuery(queryId, querySubPlansToStart);
    triggerReconfigurationOfType(queryId, DATA_SOURCE);
    stopQuerySubPlan(queryId, querySubPlansToRemove);
    unregisterQuerySubPlan(queryId, querySubPlansToRemove);

    if (!querySubPlansToRemove.empty()) {
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
                                                            const OperatorNodePtr& operatorNode,
                                                            QueryReconfigurationTypes reconfigurationType) {
    auto subPlan = operatorToSubPlan[operatorNode->getId()];
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
        bool success = workerRPCClient->registerQueryForReconfigurationAsync(rpcAddress, shadowPlan, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryReconfigurationPhase:registerForReconfiguration: " << shadowPlan->getQuerySubPlanId() << " to "
                                                                               << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryReconfigurationPhase:registerForReconfiguration: " << shadowPlan->getQuerySubPlanId() << " to "
                                                                               << rpcAddress << "  failed");
            return false;
        }
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
                bool success =
                    workerRPCClient->triggerReconfigurationAsync(rpcAddress, reconfigurationPlan, queueForExecutionNode);
                if (success) {
                    NES_DEBUG("QueryReconfigurationPhase:triggerReconfiguration: " << reconfigurationPlan->getId() << " to "
                                                                                   << rpcAddress << " successful");
                } else {
                    NES_ERROR("QueryReconfigurationPhase:triggerReconfiguration: " << reconfigurationPlan->getId() << " to "
                                                                                   << rpcAddress << "  failed");
                    return false;
                }
            }
        }
        if (plansReconfigured > 0) {
            reconfigurationCompletionQueues[queueForExecutionNode] = plansReconfigured;
        }
    }

    bool result = workerRPCClient->checkAsyncResult(reconfigurationCompletionQueues, TriggerReconfiguration);

    NES_DEBUG(
        "QueryReconfigurationPhase::triggerReconfigurationOfType: Finished triggering reconfiguration for for query with Id "
        << queryId << " success=" << result);
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
        bool success = workerRPCClient->registerQueryAsync(rpcAddress, querySubPlan, queueForExecutionNode);
        if (success) {
            NES_DEBUG("QueryReconfigurationPhase:registerForReconfiguration: " << querySubPlan->getQuerySubPlanId() << " to "
                                                                               << rpcAddress << " successful");
        } else {
            NES_ERROR("QueryReconfigurationPhase:registerForReconfiguration: " << querySubPlan->getQuerySubPlanId() << " to "
                                                                               << rpcAddress << "  failed");
            return false;
        }
        completionQueues[queueForExecutionNode] = 1;
    }
    bool result = workerRPCClient->checkAsyncResult(completionQueues, Register);

    NES_DEBUG("QueryReconfigurationPhase::registerForReconfiguration: Finished deploying execution plan for query with Id "
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