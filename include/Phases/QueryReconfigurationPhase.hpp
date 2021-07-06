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

#ifndef NES_QUERYRECONFIGURATIONPHASE_HPP
#define NES_QUERYRECONFIGURATIONPHASE_HPP

#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QueryReconfigurationPlan.hpp>
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

class QueryReconfigurationPhase;
using QueryReconfigurationPhasePtr = std::shared_ptr<QueryReconfigurationPhase>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

/**
 * @brief The query reconfiguration phase is responsible for deploying the query plan for a reconfigured worker.
 */
class QueryReconfigurationPhase {
  public:
    /**
     * @brief Returns a smart pointer to the QueryReconfigurationPhase
     * @param globalExecutionPlan : global execution plan
     * @param workerRpcClient : rpc client to communicate with workers
     * @return shared pointer to the instance of QueryReconfigurationPhase
     */
    static QueryReconfigurationPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);

    /**
     * @brief method for deploying and starting the query
     * @param queryPlan : the query plan to be deployed
     * @return true if successful else false
     */
    bool execute(const SharedQueryPlanPtr& sharedPlan);

  private:
    explicit QueryReconfigurationPhase(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);
    /**
     * @brief method send query subplan to nodes for reconfiguration
     * @param queryId
     * @return bool indicating success
     */
    bool registerForReconfiguration(QueryId queryId);

    /**
     * @brief method to trigger reconfiguration
     * @param queryId
     * @return bool indicating success
     */
    bool triggerReconfiguration(QueryId queryId);

    /**
     * @brief method send query to nodes
     * @param queryId
     * @return bool indicating success
     */
    bool deployQuery(QueryId queryId);

    /**
     * @brief method to start a already deployed query
     * @param queryId
     * @return bool indicating success
     */
    bool startQuery(QueryId queryId);

    WorkerRPCClientPtr workerRPCClient;
    GlobalExecutionPlanPtr globalExecutionPlan;
    std::map<OperatorId, QueryPlanPtr> operatorToSubPlan;
    std::map<QueryPlanPtr, ExecutionNodePtr> subPlanToExecutionNode;
    std::set<QueryPlanPtr> shadowSubPlans;
    std::set<QueryPlanPtr> reconfigurationShadowPlans;
    std::map<ExecutionNodePtr, std::vector<QueryReconfigurationPlanPtr>> executionNodeToReconfigurationPlans;
    void populateReconfigurationPlan(QueryId queryId,
                                     const OperatorNodePtr& operatorNode,
                                     QueryReconfigurationTypes reconfigurationType);
    std::string getRpcAddress(const ExecutionNodePtr& executionNode);
    bool triggerReconfigurationOfType(QueryId queryId, QueryReconfigurationTypes reconfigurationType);
};
}// namespace NES
#endif//NES_QUERYRECONFIGURATIONPHASE_HPP
