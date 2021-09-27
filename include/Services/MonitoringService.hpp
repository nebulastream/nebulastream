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

#ifndef NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
#define NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_

#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <cpprest/json.h>
#include <memory>

namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class MonitoringPlan;
using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;

class MonitoringManager;
using MonitoringManagerPtr = std::shared_ptr<MonitoringManager>;

/**
 * @brief: This class is responsible for handling requests related to fetching information regarding monitoring data.
 */
class MonitoringService {
  public:
    explicit MonitoringService(MonitoringManagerPtr manager);
    ~MonitoringService();

    web::json::value registerMonitoringPlanToAllNodes(MonitoringPlanPtr monitoringPlan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param id of the node
     * @param the monitoring plan
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestMonitoringDataAsJson(uint64_t nodeId);

    /**
     * @brief Requests from all remote worker nodes for monitoring data.
     * @param id of the node
     * @param the monitoring plan
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestMonitoringDataFromAllNodesAsJson();

    /**
     * @brief Requests from a remote worker node its monitoring data via prometheus node exporter.
     * @param nodeId the NES ID of the node
     * @param port of the prometheus node exporter port from the node
     * @param the monitoring plan
     * @return the metrics as plain string
     */
    utf8string requestMonitoringDataViaPrometheusAsString(int64_t nodeId, int16_t port);

    /**
     * @brief Requests from a remote worker node its monitoring data via prometheus node exporter. Warning: It assumes the default port 9100, otherwise
     * use requestMonitoringDataViaPrometheusAsString(..)
     * @param the monitoring plan
     * @return the metrics as json
     */
    web::json::value requestMonitoringDataFromAllNodesViaPrometheusAsJson();

  private:
    MonitoringManagerPtr monitoringManager;
};

using MonitoringServicePtr = std::shared_ptr<MonitoringService>;

}// namespace NES

#endif//NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
