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

#ifndef NES_COORDINATOR_INCLUDE_PHASES_QUERYUNDEPLOYMENTPHASE_HPP_
#define NES_COORDINATOR_INCLUDE_PHASES_QUERYUNDEPLOYMENTPHASE_HPP_

#include <Identifiers.hpp>
#include <Util/SharedQueryPlanStatus.hpp>
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
     * @return shared pointer to the instance of QueryUndeploymentPhase
     */
    static QueryUndeploymentPhasePtr create(const TopologyPtr& topology, const GlobalExecutionPlanPtr& globalExecutionPlan);

    /**
     * @brief method for stopping and undeploying the shared query with the given id
     * @param sharedQueryId : the id of the shared query plan
     * @throws ExecutionNodeNotFoundException: Unable to find ExecutionNodes where the query {queryId} is deployed
     * @throws QueryUndeploymentException: Failed to remove query subplans for the query {queryId}
     */
    void execute(SharedQueryId sharedQueryId, SharedQueryPlanStatus sharedQueryPlanStatus);

  private:
    explicit QueryUndeploymentPhase(const TopologyPtr& topology, const GlobalExecutionPlanPtr& globalExecutionPlan);
    /**
     * @brief method remove query from nodes
     * @param sharedQueryId : the id of the shared query plan
     * @param executionNodes execution nodes where query is deployed
     * @throws RPCQueryUndeploymentException: message, failedRpcExecutionNodeIds, RpcClientModes::Unregister
     */
    void undeployQuery(SharedQueryId sharedQueryId, const std::vector<ExecutionNodePtr>& executionNodes);

    /**
     * @brief method to stop a query
     * @param sharedQueryId : the id of the shared query plan
     * @param executionNodes execution nodes where query is deployed
     * @param sharedQueryPlanStatus shared query plan status
     * @throws RPCQueryUndeploymentException: message, failedRpcExecutionNodeIds, RpcClientModes::Stop
     */
    void stopQuery(SharedQueryId sharedQueryId,
                   const std::vector<ExecutionNodePtr>& executionNodes,
                   SharedQueryPlanStatus sharedQueryPlanStatus);

    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;
    WorkerRPCClientPtr workerRPCClient;
};
}// namespace NES

#endif// NES_COORDINATOR_INCLUDE_PHASES_QUERYUNDEPLOYMENTPHASE_HPP_
