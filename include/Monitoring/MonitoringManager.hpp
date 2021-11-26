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

#ifndef NES_INCLUDE_MONITORING_MONITORING_MANAGER_HPP_
#define NES_INCLUDE_MONITORING_MONITORING_MANAGER_HPP_

#include <Runtime/NodeEngineForwaredRefs.hpp>
#include <memory>
#include <unordered_map>
#include <vector>

namespace NES {

class MetricStore;
using MetricStorePtr = std::shared_ptr<MetricStore>;

class MonitoringPlan;
using MonitoringPlanPtr = std::shared_ptr<MonitoringPlan>;

class WorkerRPCClient;
using WorkerRPCClientPtr = std::shared_ptr<WorkerRPCClient>;

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

class GroupedMetricValues;
using GroupedMetricValuesPtr = std::shared_ptr<GroupedMetricValues>;

/**
 * @brief The MonitoringManager is responsible for managing all global metrics of all nodes in the topology.
 */
class MonitoringManager {
  public:
    MonitoringManager(WorkerRPCClientPtr workerClient, TopologyPtr topology);
    MonitoringManager(WorkerRPCClientPtr workerClient, TopologyPtr topology, bool enableMonitoring);
    ~MonitoringManager();

    /**
     * @brief Register a monitoring plan for given nodes.
     * @param nodeId
     * @param monitoringPlan
     * @return True, if successful, else false
     */
    bool registerRemoteMonitoringPlans(const std::vector<uint64_t>& nodeIds, const MonitoringPlanPtr& monitoringPlan);

    /**
     * @brief Get the monitoring data for a given node.
     * Note: Multiple nodes are not possible, as every node can have a different monitoring plan and
     * TupleBuffer is not supporting different nested schemas.
     * @param nodeId
     * @param tupleBuffer
     * @return the grouped metric values
     */
    GroupedMetricValues requestMonitoringData(uint64_t nodeId, Runtime::BufferManagerPtr bufferManager);

    /**
     * @brief Requests monitoring data from metric store.
     * @param nodeId
     * @return the grouped metric values
     */
    GroupedMetricValuesPtr requestNewestMonitoringDataFromMetricStore(uint64_t nodeId);

    /**
     * @brief Receive arbitrary monitoring data from a given node.
     * @param nodeId
     * @param GroupedMetricValuesPtr the grouped metric values
     */
    void receiveMonitoringData(uint64_t nodeId, GroupedMetricValuesPtr metricValues);

    /**
     * @brief Remove node from monitoring store.
     * @param nodeId
     */
    void removeMonitoringNode(uint64_t nodeId);

    /**
     * @brief Get the monitoring plan for a given node ID. If the node exists in the topology but has not a registered
     * plan, MonitoringPlan::Default will be returned. If the node does not exist an NES exception is thrown.
     * @param nodeId
     * @return The monitoring plan
     */
    MonitoringPlanPtr getMonitoringPlan(uint64_t nodeId);

  private:
    std::unordered_map<uint64_t, MonitoringPlanPtr> monitoringPlanMap;
    MetricStorePtr metricStore;
    WorkerRPCClientPtr workerClient;
    TopologyPtr topology;
    bool enableMonitoring;
};

using MonitoringManagerPtr = std::shared_ptr<MonitoringManager>;

}// namespace NES

#endif// NES_INCLUDE_MONITORING_MONITORING_MANAGER_HPP_
