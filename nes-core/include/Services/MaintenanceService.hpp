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
#include <Topology/TopologyNodeId.hpp>
#include <memory>
#include <optional>
#include <vector>
#include <string>
#include <Phases/MigrationTypes.hpp>

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
class ExecutionNode;
typedef std::shared_ptr<ExecutionNode> ExecutionNodePtr;
class TopologyNode;
typedef std::shared_ptr<TopologyNode> TopologyNodePtr;


/**
 * @brief this class is responsible for handling maintenance requests.
 */
class MaintenanceService {
  public:

    MaintenanceService(TopologyPtr topology, QueryCatalogPtr queryCatalog, NESRequestQueuePtr queryRequestQueue,
                       GlobalExecutionPlanPtr globalExecutionPlan);
    ~MaintenanceService();
    /**
     * submit a request to take a node offline for maintenance
     * @returns a pair indicating if a maintenance request has been successfully submitted and info
     * @param nodeId
     * @param MigrationType
     */
  std::pair<bool, std::string> submitMaintenanceRequest(TopologyNodeId nodeId,  MigrationType type);

  private:

    TopologyPtr topology;
    QueryCatalogPtr queryCatalog;
    NESRequestQueuePtr queryRequestQueue;
    GlobalExecutionPlanPtr globalExecutionPlan;
};


typedef std::shared_ptr<MaintenanceService> MaintenanceServicePtr;
} //namepsace NES
#endif//NES_MAINTENANCESERVICE_HPP
