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

    std::set<QueryPlanPtr> querySubPlansToStart;
    std::set<QueryPlanPtr> sourceReconfiguration;

    SharedQueryPlanChangeLogPtr changeLog = sharedPlan->getChangeLog();

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

    for (const auto& addition : changeLog->getAddition()) {
        auto sourceSubPlan = operatorToSubPlan[addition.first->getId()];
        for (auto newOperator : addition.second) {
            std::vector<NodePtr> operatorChain{queryPlan->getOperatorWithId(newOperator)};
            while (!operatorChain.empty()) {
                auto op = operatorChain.back()->as<OperatorNode>();
                operatorChain.pop_back();
                std::shared_ptr<QueryPlan>& subPlanContainingOperator = operatorToSubPlan[op->getId()];
                if (subPlanContainingOperator->getQuerySubPlanId() != sourceSubPlan->getQuerySubPlanId()) {
                    querySubPlansToStart.insert(subPlanContainingOperator);
                }
                auto opParents = queryPlan->getOperatorWithId(op->getId())->getParents();
                operatorChain.insert(operatorChain.end(), opParents.begin(), opParents.end());
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

    // reassign network operator Ids
    for (const auto& shadowSubPlan : shadowSubPlans) {
        auto handlingSubPlanWithModification = false;
        for (const auto& addition : changeLog->getAddition()) {
            if (operatorToSubPlan[addition.first->getId()] == shadowSubPlan) {
                handlingSubPlanWithModification = true;
                break;
            }
        }
        if (!handlingSubPlanWithModification) {
            for (const auto& sink : shadowSubPlan->getSinkOperators()) {
                const SinkDescriptorPtr& sinkDesc = sink->getSinkDescriptor();
                if (sinkDesc->instanceOf<Network::NetworkSinkDescriptor>()) {
                    auto oldNetSinkDesc = sinkDesc->as<Network::NetworkSinkDescriptor>();
                    auto oldNesPartition = oldNetSinkDesc->getNesPartition();
                    auto oldNetSrcOperatorId = oldNesPartition.getOperatorId();
                    auto netSrcSubPlan = operatorToSubPlan[oldNetSrcOperatorId];
                    auto newNetSrcOperatorId = UtilityFunctions::getNextOperatorId();
                    netSrcSubPlan->getOperatorWithId(oldNetSrcOperatorId)->setId(newNetSrcOperatorId);
                    auto newSinkDesc =
                        Network::NetworkSinkDescriptor::create(oldNetSinkDesc->getNodeLocation(),
                                                               Network::NesPartition(oldNesPartition.getQueryId(),
                                                                                     newNetSrcOperatorId,
                                                                                     oldNesPartition.getPartitionId(),
                                                                                     oldNesPartition.getSubpartitionId()),
                                                               oldNetSinkDesc->getWaitTime(),
                                                               oldNetSinkDesc->getRetryTimes());
                    sink->setSinkDescriptor(newSinkDesc);
                    operatorToSubPlan[newNetSrcOperatorId] = netSrcSubPlan;
                    shadowSubPlans.insert(netSrcSubPlan);
                }
            }
        }
    }

    for (const auto& source : queryPlan->getSourceOperators()) {
        populateReconfigurationPlan(queryId, source, DATA_SOURCE);
    }

    for (const auto& sink : queryPlan->getSinkOperators()) {
        auto sinkSubPlan = operatorToSubPlan[sink->getId()];
        if (shadowSubPlans.contains(sinkSubPlan)) {
            populateReconfigurationPlan(queryId, sink, DATA_SINK);
        }
    }

    for (const auto& shadowSubPlan : shadowSubPlans) {
        shadowSubPlan->setQuerySubPlanId(PlanIdGenerator::getNextQuerySubPlanId());
    }

    NES_DEBUG("QueryReconfigurationPhase: Update Global Execution Plan : \n" << globalExecutionPlan->getAsString());

    deployQuery(queryId);
    registerForReconfiguration(queryId);
    triggerReconfigurationOfType(queryId, DATA_SINK);
    startQuery(queryId);
    triggerReconfigurationOfType(queryId, DATA_SOURCE);

    operatorToSubPlan.clear();
    subPlanToExecutionNode.clear();
    shadowSubPlans.clear();
    reconfigurationShadowPlans.clear();
    executionNodeToReconfigurationPlans.clear();

    return true;
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
    std::map<CompletionQueuePtr, uint64_t> sinkReconfigurationCompletionQueues;
    for (const auto& reconfiguration : executionNodeToReconfigurationPlans) {
        auto reconfigurationPlans = reconfiguration.second;
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();
        std::string rpcAddress = getRpcAddress(reconfiguration.first);
        auto plansReconfigured = 0;
        for (const auto& reconfigurationPlan : reconfigurationPlans) {
            if (reconfigurationPlan->getReconfigurationType() == reconfigurationType) {
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
            sinkReconfigurationCompletionQueues[queueForExecutionNode] = plansReconfigured;
        }
    }

    bool result = workerRPCClient->checkAsyncResult(sinkReconfigurationCompletionQueues, RegisterForReconfiguration);

    NES_DEBUG("QueryReconfigurationPhase::registerForReconfiguration: Finished deploying execution plan for query with Id "
              << queryId << " success=" << result);
    return result;
}

bool QueryReconfigurationPhase::deployQuery(QueryId queryId) {
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    for (const auto& shadowPlan : shadowSubPlans) {
        CompletionQueuePtr queueForExecutionNode = std::make_shared<CompletionQueue>();

        std::string rpcAddress = getRpcAddress(subPlanToExecutionNode[shadowPlan]);
        bool success = workerRPCClient->registerQueryAsync(rpcAddress, shadowPlan, queueForExecutionNode);
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

bool QueryReconfigurationPhase::startQuery(QueryId queryId) {
    NES_DEBUG("QueryReconfigurationPhase::startQuery queryId=" << queryId);
    //TODO: check if one queue can be used among multiple connections
    std::map<CompletionQueuePtr, uint64_t> completionQueues;
    std::set<ExecutionNodePtr> executionNodes;

    for (const auto& shadowPlan : shadowSubPlans) {
        executionNodes.insert(subPlanToExecutionNode[shadowPlan]);
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