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

#ifndef NES_INCLUDE_PHASES_QUERYDEPLOYMENTPHASE_HPP_
#define NES_INCLUDE_PHASES_QUERYDEPLOYMENTPHASE_HPP_

#include <Plans/Query/QueryId.hpp>
#include <iostream>
#include <memory>
#include <vector>

namespace NES {

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class ExecutionNode;
using ExecutionNodePtr = std::shared_ptr<ExecutionNode>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class QueryDeploymentPhase;
using QueryDeploymentPhasePtr = std::shared_ptr<QueryDeploymentPhase>;

/**
 * @brief The query deployment phase is responsible for deploying the query plan for a query to respective worker nodes.
 */
class QueryDeploymentPhase {
  public:
    /**
     * @brief Returns a smart pointer to the QueryDeploymentPhase
     * @param globalExecutionPlan : global execution plan
     * @param workerRpcClient : rpc client to communicate with workers
     * @return shared pointer to the instance of QueryDeploymentPhase
     */
    static QueryDeploymentPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);

    /**
     * @brief method for deploying and starting the query
     * @param queryId : the query Id of the query to be deployed and started
     * @return true if successful else false
     */
    bool execute(QueryId queryId);

  private:
    explicit QueryDeploymentPhase(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);
    /**
     * @brief method send query to nodes
     * @param queryId
     * @return bool indicating success
     */
    bool deployQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes);

    /**
     * @brief method to start a already deployed query
     * @param queryId
     * @return bool indicating success
     */
    bool startQuery(QueryId queryId, const std::vector<ExecutionNodePtr>& executionNodes);

    WorkerRPCClientPtr workerRPCClient;
    GlobalExecutionPlanPtr globalExecutionPlan;
};
}// namespace NES
#endif  // NES_INCLUDE_PHASES_QUERYDEPLOYMENTPHASE_HPP_
