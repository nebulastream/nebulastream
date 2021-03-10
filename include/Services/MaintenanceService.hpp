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
// Created by balint on 28.02.21.
//

#ifndef NES_MAINTENANCESERVICE_HPP
#define NES_MAINTENANCESERVICE_HPP
#include <memory>
#include <vector>

namespace NES {

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;
class QueryCatalog;
typedef std::shared_ptr<QueryCatalog> QueryCatalogPtr;
class QueryRequestQueue;
typedef std::shared_ptr<QueryRequestQueue> QueryRequestQueuePtr;
class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;

/**
 * @brief this class is responsible for handling maintenance requests. Three different strategies are implemented to handle query redeployment due to nodes being taken offline
 */
class MaintenanceService {
  public:
    explicit MaintenanceService(TopologyPtr topology, QueryCatalogPtr queryCatalog, QueryRequestQueuePtr queryRequestQueue, GlobalExecutionPlanPtr globalExecutionPlan);

    ~MaintenanceService();
    /**
     * submit a request to take a node offline for maintenance
     * @param nodeId
     * @param strategy
     */
    std::vector<uint64_t> submitMaintenanceRequest(uint64_t nodeId, uint8_t strategy);

  private:
    /**
     * This method represents the first strategy. Here, all queries with subqueries on nodes that are to be taken offline are redeployed in their entirety
     * @param nodeId
     * @return true if success or false if failure
     */
    std::vector<uint64_t> firstStrat(uint64_t nodeId);

    /**
     * this method represents the second strategy. Here, data is buffered on the upstream nodes until the subquery on the node to be taken offline is redeployed
     * @param nodeId
     * @return
     */
    std::vector<uint64_t> secondStrat(uint64_t nodeId);

    /**
     * This method represents the third strategy. Here, subqueries of nodes are redeployed before marking the node for maintenance. This ensures uninterrupted data proccessing.
     * @param nodeId
     * @return
     */
    std::vector<uint64_t> thirdStrat(uint64_t nodeId);

    /**
     * sets maintenanceFlag of node to true
     * @param nodeId
     * @return
     */
    bool markNodeForMaintenance(uint64_t nodeId);

    TopologyPtr topology;
    QueryCatalogPtr queryCatalog;
    QueryRequestQueuePtr queryRequestQueue;
    GlobalExecutionPlanPtr globalExecutionPlan;

};

typedef std::shared_ptr<MaintenanceService> MaintenanceServicePtr;
} //namepsace NES
#endif//NES_MAINTENANCESERVICE_HPP
