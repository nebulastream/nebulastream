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

#ifndef NES_INCLUDE_MONITORING_MONITORINGMANAGER_HPP_
#define NES_INCLUDE_MONITORING_MONITORINGMANAGER_HPP_

#include <NodeEngine/NodeEngineForwaredRefs.hpp>
#include <memory>
#include <unordered_map>
#include <vector>

namespace NES {

class MonitoringPlan;
class TupleBuffer;
typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;

class WorkerRPCClient;
typedef std::shared_ptr<WorkerRPCClient> WorkerRPCClientPtr;

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;

/**
 * @brief The MonitoringManager is responsible for managing all global metrics of all nodes in the topology.
 */
class MonitoringManager {
  public:
    MonitoringManager(WorkerRPCClientPtr workerClient, TopologyPtr topology);
    ~MonitoringManager();

    /**
     * @brief Register a monitoring plan for given nodes.
     * @param nodeId
     * @param monitoringPlan
     * @return True, if successful, else false
     */
    bool registerRemoteMonitoringPlans(std::vector<uint64_t> nodeIds, MonitoringPlanPtr monitoringPlan);

    /**
     * @brief Get the monitoring data for a given node.
     * Note: Multiple nodes are not possible, as every node can have a different monitoring plan and
     * TupleBuffer is not supporting different nested schemas.
     * @param nodeId
     * @param tupleBuffer
     * @return
     */
    bool requestMonitoringData(uint64_t nodeId, NodeEngine::TupleBuffer& tupleBuffer);

    /**
     * @brief Get the monitoring plan for a given node ID. If the node exists in the topology but has not a registered
     * plan, MonitoringPlan::Default will be returned. If the node does not exist an NES exception is thrown.
     * @param nodeId
     * @return The monitoring plan
     */
    MonitoringPlanPtr getMonitoringPlan(uint64_t nodeId);

  private:
    std::unordered_map<uint64_t, MonitoringPlanPtr> monitoringPlanMap;
    WorkerRPCClientPtr workerClient;
    TopologyPtr topology;
};

typedef std::shared_ptr<MonitoringManager> MonitoringManagerPtr;

}// namespace NES

#endif//NES_INCLUDE_MONITORING_MONITORINGMANAGER_HPP_
