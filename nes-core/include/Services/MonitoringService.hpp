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

#ifndef NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
#define NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_

#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <memory>

namespace NES {

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;


/**
 * @brief: This class is responsible for handling requests related to fetching information regarding monitoring data.
 */
class MonitoringService {
  public:
    MonitoringService(WorkerRPCClientPtr workerClient, TopologyPtr topology);
    MonitoringService(WorkerRPCClientPtr workerClient, TopologyPtr topology, bool enableMonitoring);

    /**
     * @brief Submitting a monitoring plan to all nodes which indicates which metrics have to be sampled.
     * @param monitoringPlan
     * @return json to indicate if it was successfully
     */
    web::json::value registerMonitoringPlanToAllNodes(MonitoringPlanPtr monitoringPlan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param id of the node
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestMonitoringDataAsJson(uint64_t nodeId);

    /**
     * @brief Requests from all remote worker nodes for monitoring data.
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestMonitoringDataFromAllNodesAsJson();

    /**
     * @brief Requests from all remote worker nodes for monitoring data.
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestNewestMonitoringDataFromMetricStoreAsJson();

    /**
     * Getter for MonitoringManager
     * @return The MonitoringManager
     */
    [[nodiscard]] const MonitoringManagerPtr getMonitoringManager() const;

    /**
     * @brief Returns bool if monitoring is enabled or not.
     * @return Monitoring flag
     */
    bool isEnableMonitoring() const;

  private:
    MonitoringManagerPtr monitoringManager;
    TopologyPtr topology;
    bool enableMonitoring;
};

using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

}// namespace NES

#endif  // NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
