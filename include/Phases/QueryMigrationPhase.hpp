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
//
// Created by balint on 17.04.21.
//

#ifndef NES_QUERYMIGRATIONPHASE_HPP
#define NES_QUERYMIGRATIONPHASE_HPP
#include <Plans/Query/QueryId.hpp>
#include <Services/StrategyType.hpp>
#include <Topology/TopologyNodeId.hpp>
#include <memory>



namespace NES{

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryMigrationPhase;
typedef std::shared_ptr<QueryMigrationPhase> QueryMigrationPhasePtr;

class QueryMigrationPhase{

  public:
    /**
     * @brief Returns a smart pointer to the QueryMigrationPhase
     * @param globalExecutionPlan : global execution plan
     * @param workerRpcClient : rpc client to communicate with workers
     * @return shared pointer to the instance of QueryMigrationPhase
     */
    static QueryMigrationPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRPCClient);

    /**
     * @brief method for executing a query migration.
     * @param queryId : the query Id of the query to be migrated
     * @param topologyNodeId: node to be taken down for maintenance
     * @details First tries to find suitable node to migrate queries to. Then all child nodes of node marked for maintenance that are part of query
     * are made to buffer data. Subqueries for query on node marked for maintenance are migrated to new node. Then network sink for all children are
     * reconfigured. Lastly we replay buffers to new node.
     *
     * @return true if successful else false
     */
    bool execute(QueryId queryId, TopologyNodeId topologyNodeId, StrategyType strategyType);
    ~QueryMigrationPhase();

  private:
    explicit QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan, WorkerRPCClientPtr workerRpcClient);
    /**
     * @brief method send query to nodes
     * @param queryId
     * @return bool indicating success
     */

    WorkerRPCClientPtr  workerRPCClient;
    GlobalExecutionPlanPtr globalExecutionPlan;
};

}//namespace NES

#endif//NES_QUERYMIGRATIONPHASE_HPP
