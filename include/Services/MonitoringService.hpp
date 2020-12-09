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

#include <cpprest/json.h>
#include <memory>
#include <NodeEngine/NodeEngineForwaredRefs.hpp>

namespace NES {

class Schema;
typedef std::shared_ptr<Schema> SchemaPtr;

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

class MonitoringPlan;
typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;


class MonitoringService {
  public:
    explicit MonitoringService(WorkerRPCClientPtr workerClient, TopologyPtr topology, NodeEngine::BufferManagerPtr bufferManager);

    ~MonitoringService();

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param ipAddress
     * @param grpcPort
     * @param the monitoring plan
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    std::tuple<SchemaPtr, TupleBuffer> requestMonitoringData(const std::string& ipAddress, int64_t grpcPort,
                                                             MonitoringPlanPtr plan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param ipAddress
     * @param grpcPort
     * @param the monitoring plan
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer as json
     */
    web::json::value requestMonitoringDataAsJson(const std::string& ipAddress, int64_t grpcPort, MonitoringPlanPtr plan);

    /**
     * @brief Requests from a remote worker node its monitoring data.
     * @param id of the node
     * @param the monitoring plan
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestMonitoringDataAsJson(int64_t nodeId, MonitoringPlanPtr plan);

    /**
     * @brief Requests from all remote worker nodes for monitoring data.
     * @param id of the node
     * @param the monitoring plan
     * @param the buffer where the data will be written into
     * @return a tuple with the schema and tuplebuffer
     */
    web::json::value requestMonitoringDataForAllNodesAsJson(MonitoringPlanPtr plan);

    /**
     * @brief Requests from a remote worker node its monitoring data via prometheus node exporter.
     * @param nodeId the NES ID of the node
     * @param port of the prometheus node exporter port from the node
     * @param the monitoring plan
     * @return the metrics as plain string
     */
    utf8string requestMonitoringDataViaPrometheusAsString(int64_t nodeId, int16_t port, MonitoringPlanPtr plan);

    /**
     * @brief Requests from a remote worker node its monitoring data via prometheus node exporter. Warning: It assumes the default port 9100, otherwise
     * use requestMonitoringDataViaPrometheusAsString(..)
     * @param the monitoring plan
     * @return the metrics as json
     */
    web::json::value requestMonitoringDataFromAllNodesViaPrometheusAsJson(MonitoringPlanPtr plan);

  private:
    WorkerRPCClientPtr workerClient;
    TopologyPtr topology;
    NodeEngine::BufferManagerPtr bufferManager;
};

typedef std::shared_ptr<MonitoringService> MonitoringServicePtr;

}// namespace NES

#endif//NES_INCLUDE_SERVICES_MONITORINGSERVICE_HPP_
