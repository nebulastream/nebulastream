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

#ifndef NES_INCLUDE_PHASES_QUERY_UNDEPLOYMENT_PHASE_HPP_
#define NES_INCLUDE_PHASES_QUERY_UNDEPLOYMENT_PHASE_HPP_

#include <Plans/Query/QueryId.hpp>
#include <iostream>
#include <memory>
#include <vector>

namespace NES {

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class QueryUndeploymentPhase;
using QueryUndeploymentPhasePtr = std::shared_ptr<QueryUndeploymentPhase>;

/**
 * @brief This class is responsible for undeploying and stopping a running query
 */
class QueryUndeploymentPhase {

  public:
    /**
     * @brief Returns a smart pointer to the QueryUndeploymentPhase
     * @param globalExecutionPlan : global execution plan
     * @param workerRpcClient : rpc client to communicate with workers
     * @return shared pointer to the instance of QueryUndeploymentPhase
     */
    static QueryUndeploymentPhasePtr
    create(TopologyPtr topology, GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);

    /**
     * @brief method for stopping and undeploying the query with the given id
     * @param queryId : id of the query
     * @return true if successful
     */
    bool execute(QueryId queryId);

  private:
    explicit QueryUndeploymentPhase(TopologyPtr topology,
                                    GlobalExecutionPlanPtr globalExecutionPlan,
                                    WorkerRPCClientPtr workerRpcClient);
    /**
     * @brief method remove query from nodes
     * @param queryId
     * @return bool indicating success
     */
    bool undeployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes);

    /**
     * @brief method to stop a query
     * @param queryId
     * @return bool indicating success
     */
    bool stopQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes);

    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;
    WorkerRPCClientPtr workerRPCClient;
};
}// namespace NES

#endif  // NES_INCLUDE_PHASES_QUERY_UNDEPLOYMENT_PHASE_HPP_
