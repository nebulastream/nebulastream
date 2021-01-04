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
#include <Phases/QueryReconfigurationPhase.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>

namespace NES {

QueryReconfigurationPhase::QueryReconfigurationPhase(GlobalExecutionPlanPtr globalExecutionPlan,
                                                     WorkerRPCClientPtr workerRpcClient)
    : globalExecutionPlan(globalExecutionPlan), workerRPCClient(workerRpcClient) {
    NES_DEBUG("QueryReconfigurationPhase()");
}

QueryReconfigurationPhasePtr QueryReconfigurationPhase::create(GlobalExecutionPlanPtr globalExecutionPlan,
                                                               WorkerRPCClientPtr workerRpcClient) {
    return std::make_shared<QueryReconfigurationPhase>(QueryReconfigurationPhase(globalExecutionPlan, workerRpcClient));
}

QueryReconfigurationPhase::~QueryReconfigurationPhase() { NES_DEBUG("~QueryReconfigurationPhase()"); }

bool QueryReconfigurationPhase::execute(QueryId queryId) {

    NES_DEBUG("QueryReconfigurationPhase: reconfigure the query");

    std::vector<ExecutionNodePtr> executionNodes = globalExecutionPlan->getExecutionNodesByQueryId(queryId);
    if (executionNodes.empty()) {
        NES_ERROR("QueryReconfigurationPhase: Unable to find ExecutionNodes to be reconfigured for query " << queryId);
        throw QueryDeploymentException("QueryReconfigurationPhase: Unable to find ExecutionNodes to be reconfigure the query "
                                       + std::to_string(queryId));
    }

    bool successReconfiguration = reconfigureSinks(queryId, executionNodes);
    if (successReconfiguration) {
        NES_DEBUG("QueryReconfigurationPhase: reconfiguration for query " + std::to_string(queryId) + " successful");
    } else {
        NES_ERROR("QueryReconfigurationPhase: Failed to reconfigure query " << queryId);
        throw QueryDeploymentException("QueryReconfigurationPhase: Failed to reconfigure query " + std::to_string(queryId));
    }
    return successReconfiguration;
}

bool QueryReconfigurationPhase::resetGlobalExecutionPlan(QueryId queryId) {
    NES_DEBUG("QueryReconfigurationPhase: reset global execution plan " + std::to_string(queryId));
    bool resetSuccessful = globalExecutionPlan->removeQuerySubPlans(queryId);
    return resetSuccessful;
}

bool QueryReconfigurationPhase::reconfigureSinks(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes) {
    NES_DEBUG("QueryReconfigurationPhase::reconfigure sinks queryId=" << queryId);

    for (ExecutionNodePtr executionNode : executionNodes) {
        NES_DEBUG("QueryReconfigurationPhase::reconfigureQuery serialize id=" << executionNode->getId());
        std::vector<QueryPlanPtr> querySubPlans = executionNode->getQuerySubPlans(queryId);
        if (querySubPlans.empty()) {
            NES_WARNING("QueryReconfigurationPhase : unable to find query sub plan with id " << queryId);
            return false;
        }

        const auto& nesNode = executionNode->getTopologyNode();
        auto ipAddress = nesNode->getIpAddress();
        auto grpcPort = nesNode->getGrpcPort();
        std::string rpcAddress = ipAddress + ":" + std::to_string(grpcPort);
        NES_DEBUG("QueryReconfigurationPhase:reconfigureQuery: " << queryId << " to " << rpcAddress);

        for (auto& querySubPlan : querySubPlans) {
            bool success = workerRPCClient->reconfigureQuery(rpcAddress, querySubPlan);
            if (success) {
                NES_DEBUG("QueryReconfigurationPhase:reconfigureQuery: " << queryId << " to " << rpcAddress << " successful");
            } else {
                NES_ERROR("QueryReconfigurationPhase:reconfigureQuery: " << queryId << " to " << rpcAddress << "  failed");
                return false;
            }
        }
    }
    NES_INFO("QueryReconfigurationPhase: Finished reconfiguring sinks for query with Id " << queryId);
    return true;
}

}// namespace NES
