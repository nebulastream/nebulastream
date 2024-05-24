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

#ifndef NES_COORDINATOR_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
#define NES_COORDINATOR_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <memory>

namespace NES {

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class RequestHandlerService;
using RequestHandlerServicePtr = std::shared_ptr<RequestHandlerService>;

namespace Catalogs::Query {
class QueryCatalog;
using QueryCatalogPtr = std::shared_ptr<QueryCatalog>;
}// namespace Catalogs::Query

/**
 * @brief: This class is responsible for handling requests related to fetching information regarding monitoring data.
 */
class MonitoringService {
  public:
    MonitoringService(TopologyPtr topology,
                      RequestHandlerServicePtr requestHandlerService,
                      Catalogs::Query::QueryCatalogPtr queryCatalog);

    MonitoringService(TopologyPtr topology,
                      RequestHandlerServicePtr requestHandlerService,
                      Catalogs::Query::QueryCatalogPtr queryCatalog,
                      bool enableMonitoring);

    /**
     * @brief Registers a monitoring plan at all nodes. A MonitoringPlan indicates which metrics have to be sampled at a node.
     * @param monitoringPlan
     * @return json to indicate if it was successfully
     */
    nlohmann::json registerMonitoringPlanToAllNodes(Monitoring::MonitoringPlanPtr monitoringPlan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @return a json with all metrics indicated by the registered MonitoringPlan.
     */
    nlohmann::json requestMonitoringDataAsJson(WorkerId nodeId);

    /**
     * @brief Requests from all remote worker nodes for monitoring data.
     * @return a json with all metrics indicated by the registered MonitoringPlan.
     */
    nlohmann::json requestMonitoringDataFromAllNodesAsJson();

    /**
     * @brief Requests from all remote worker nodes for monitoring data.
     * @return a json with all metrics indicated by the registered MonitoringPlan.
    */
    nlohmann::json requestNewestMonitoringDataFromMetricStoreAsJson();

    /**
     * @brief Starts the monitoring streams for monitoring data.
     * @return true if initiated
    */
    nlohmann::json startMonitoringStreams();

    /**
     * @brief Starts the monitoring streams for monitoring data.
     * @return true if initiated
    */
    nlohmann::json stopMonitoringStreams();

    /**
     * @brief Gets the monitoring streams for monitoring data.
     * @return a json with all query IDs and the status indicated by the registered MonitoringPlan.
    */
    nlohmann::json getMonitoringStreams();

    /**
     * @brief Getter for MonitoringManager
     * @return The MonitoringManager
     */
    [[nodiscard]] const Monitoring::MonitoringManagerPtr getMonitoringManager() const;

    /**
     * @brief Returns bool if monitoring is enabled or not.
     * @return Monitoring flag
     */
    bool isMonitoringEnabled() const;

  private:
    Monitoring::MonitoringManagerPtr monitoringManager;
    TopologyPtr topology;
    bool enableMonitoring;
};

using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

}// namespace NES

#endif// NES_COORDINATOR_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
