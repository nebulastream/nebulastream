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

#ifndef NES_CORE_INCLUDE_SERVICES_MAINTENANCESERVICE_HPP_
#define NES_CORE_INCLUDE_SERVICES_MAINTENANCESERVICE_HPP_

#include <Common/Identifiers.hpp>
#include <Phases/MigrationType.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace NES {
class QueryCatalogService;
using QueryCatalogServicePtr = std::shared_ptr<QueryCatalogService>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class RequestQueue;
using RequestQueuePtr = std::shared_ptr<RequestQueue>;

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;
}// namespace NES

namespace NES::Experimental {

class MaintenanceService;
using MaintenanceServicePtr = std::shared_ptr<MaintenanceService>;

/**
 * @brief this class is responsible for handling maintenance requests.
 */
class MaintenanceService {
  public:
    MaintenanceService(TopologyPtr topology, RequestQueuePtr queryRequestQueue);

    ~MaintenanceService();

    /**
     * submit a request to take a node offline for maintenance
     * @returns a pair indicating if a maintenance request has been successfully submitted and info
     * @param nodeId
     * @param MigrationType
     */
    std::pair<bool, std::string> submitMaintenanceRequest(TopologyNodeId nodeId, MigrationType::Value type);

  private:
    TopologyPtr topology;
    RequestQueuePtr queryRequestQueue;
};

}// namespace NES::Experimental
#endif// NES_CORE_INCLUDE_SERVICES_MAINTENANCESERVICE_HPP_
