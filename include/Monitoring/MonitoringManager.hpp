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

namespace NES {

class MonitoringPlan;
class TupleBuffer;
typedef std::shared_ptr<MonitoringPlan> MonitoringPlanPtr;

/**
 * @brief The MonitoringManager is responsible for managing all global metrics of all nodes in the topology.
 */
class MonitoringManager {
  public:
    MonitoringManager();

    bool registerMonitoringPlan(uint64_t nodeId, MonitoringPlanPtr monitoringPlan);
    bool requestMonitoringData(uint64_t nodeId, NodeEngine::TupleBuffer& tupleBuffer);

  private:
    std::unordered_map<uint64_t, MonitoringPlanPtr> monitoringPlanMap;
};

}

#endif//NES_INCLUDE_MONITORING_MONITORINGMANAGER_HPP_
