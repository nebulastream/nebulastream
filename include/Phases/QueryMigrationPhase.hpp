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

#include <Network/NodeLocation.hpp>
#include <Optimizer/Phases/QueryPlacementPhase.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/StrategyType.hpp>
#include <Topology/TopologyNodeId.hpp>
#include <memory>
#include <vector>

namespace NES{

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

class QueryMigrationPhase;
typedef std::shared_ptr<QueryMigrationPhase> QueryMigrationPhasePtr;

class QueryPlacementPhase;
typedef std::shared_ptr<QueryPlacementPhase> QueryPlacementPhasePtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class TopologyNode;
typedef std::shared_ptr<TopologyNode> TopologyNodePtr;

class MigrateQueryRequest;
typedef std::shared_ptr<MigrateQueryRequest> MigrateQueryRequestPtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class QueryMigrationPhase{

  public:
    /**
     * @brief Returns a smart pointer to the QueryMigrationPhase
     * @param globalExecutionPlan : global execution plan
     * @param workerRpcClient : rpc client to communicate with workers
     * @return shared pointer to the instance of QueryMigrationPhase
     */
    static QueryMigrationPhasePtr create(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, WorkerRPCClientPtr workerRPCClient, Optimizer::QueryPlacementPhasePtr queryPlacementPhase);

    /**
     * @brief method for executing a query migration.
     * @param migrateQueryRequest, contains QueryId, TopologyNode and withBuffer boolean
     * @return true if successful else false
     */
    bool execute(MigrateQueryRequestPtr migrateQueryRequest);

    /**
 * finds all child execution nodes of the execution node corresponding to the topology node and query
 * @param queryId
 * @param topologyNodeId
 * @return vector of top nodes or empty if none
 */
    std::vector<TopologyNodePtr> findChildExecutionNodesAsTopologyNodes (QueryId queryId, TopologyNodeId topologyNodeId);

    /**
    * finds all parent execution nodes of the execution node corresponding to the topology node and query
    * @param queryId
    * @param topologyNodeId
    * @return vector of top nodes or empty if none
    */
    std::vector<TopologyNodePtr> findParentExecutionNodesAsTopologyNodes(QueryId queryId, TopologyNodeId topologyNodeId);

  private:

    bool executeMigrationWithBuffer(std::vector<QueryPlanPtr>& queryPlans);

    bool executeMigrationWithoutBuffer(const std::vector<QueryPlanPtr>& queryPlans);

    explicit QueryMigrationPhase(GlobalExecutionPlanPtr globalExecutionPlan, TopologyPtr topology, WorkerRPCClientPtr workerRpcClient, Optimizer::QueryPlacementPhasePtr queryPlacementPhase);

/**
    * @brief method send query to nodes
    * @param queryId
    * @return bool indicating success
    */
    bool deployQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes);

    TopologyNodePtr findNewNodeLocationOfNetworkSource(QueryId queryId, OperatorId sourceOperatorIds,std::vector<ExecutionNodePtr>& potentialLocations);

    /**
     * method to find Path between all child and parent execution nodes of the node marked for maintenance for a specific query Id
     * @param queryId
     * @param topologyNodeId
     * @return path
     */
    std::vector<TopologyNodePtr> findPath(QueryId queryId, TopologyNodeId topologyNodeId);

    /**
     * @brief method to start a already deployed query
     * @param queryId
     * @return bool indicating success
     */
    bool startQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes);

    WorkerRPCClientPtr  workerRPCClient;
    TopologyPtr topology;
    GlobalExecutionPlanPtr globalExecutionPlan;
    Optimizer::QueryPlacementPhasePtr queryPlacementPhase;

};

}//namespace NES

#endif//NES_QUERYMIGRATIONPHASE_HPP
