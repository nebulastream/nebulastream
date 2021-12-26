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
// Created by balint on 25.12.21.
//

#ifndef NES_MAINTENANCESERVICE_HPP
#define NES_MAINTENANCESERVICE_HPP
#include <Plans/Query/QueryId.hpp>
#include <Services/StrategyType.hpp>
#include <Topology/TopologyNodeId.hpp>
#include <memory>
#include <optional>
#include <vector>
#include <string>

namespace NES {

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;
class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;
class NESRequestQueue;
typedef std::shared_ptr<NESRequestQueue> NESRequestQueuePtr;
class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;
class QueryPlan;
typedef std::shared_ptr<QueryPlan> queryPlanPtr;
class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;
class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;
class TopologyNode;
typedef std::shared_ptr<TopologyNode> TopologyNodePtr;


/**
 * @brief this class is responsible for handling maintenance requests. Three different strategies are implemented to handle query redeployment due to nodes being taken offline
 */
class MaintenanceService {
  public:

    struct QueryMigrationMessage{
        QueryId queryId;
        bool success;
        std::string info;
        bool operator == (const QueryMigrationMessage& lhs){
            if(this->queryId == lhs.queryId && this->success == lhs.success && this->info == lhs.info){
                return true;
            }
            else
                return false;
        };
    };

    MaintenanceService(TopologyPtr topology, QueryCatalogPtr queryCatalog, NESRequestQueuePtr queryRequestQueue,
                       GlobalExecutionPlanPtr globalExecutionPlan);
    ~MaintenanceService();
    /**
     * submit a request to take a node offline for maintenance
     * @returns Vector of QueryIds to restart
     * @param nodeId
     * @param strategy
     */
   std::vector<MaintenanceService::QueryMigrationMessage> submitMaintenanceRequest(TopologyNodeId nodeId, StrategyType strategy);

    /**
     * retunrs either an exisiting execution node or a new execution node
     */
    ExecutionNodePtr getExecutionNode(uint64_t candidateTopologyNode);

    /**
    * @brief method send query to nodes
    * @param queryId
    * @return bool indicating success
    */
    bool deployQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes);


    /**
     * @brief method to start a already deployed query
     * @param queryId
     * @return bool indicating success
     */
    bool startQuery(QueryId queryId, std::vector<ExecutionNodePtr> executionNodes);


  private:
    /**
     * This method represents the first strategy. Here, all queries with subqueries on nodes that are to be taken offline are redeployed in their entirety
     * @param nodeId
     * @return true if success or false if failure
     */
    std::vector<MaintenanceService::QueryMigrationMessage> executeQueryMigrationWithRestart(std::vector<QueryId> QueryIds);

    /**
     * this method represents the second strategy. Here, data is buffered on the upstream nodes until the subquery on the node to be taken offline is redeployed
     * @param nodeId
     * @return
     */
    std::vector<MaintenanceService::QueryMigrationMessage> executeQueryMigrationWithBuffer(std::vector<QueryId> queryIds,TopologyNodeId nodeId);

    /**
     * This method represents the third strategy. Here, subqueries of nodes are redeployed before marking the node for maintenance. This ensures uninterrupted data proccessing.
     * @param nodeId
     * @return
     */
    std::vector<MaintenanceService::QueryMigrationMessage> executeQueryMigrationWithoutBuffer(std::vector<QueryId> queryIds,TopologyNodeId nodeId);

    /**
     * sets maintenanceFlag of node to true
     * @param nodeId
     * @return
     */
    bool markNodeForMaintenance(uint64_t nodeId);

    TopologyPtr topology;
    QueryCatalogPtr queryCatalog;
    NESRequestQueuePtr queryRequestQueue;
    GlobalExecutionPlanPtr globalExecutionPlan;
    WorkerRPCClientPtr workerRPCClient;
};


typedef std::shared_ptr<MaintenanceService> MaintenanceServicePtr;
} //namepsace NES
#endif//NES_MAINTENANCESERVICE_HPP
