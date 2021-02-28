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
namespace NES {

class Topology;
typedef std::shared_ptr<Topology> TopologyPtr;
class QueryService;
typedef std::shared_ptr<QueryService> QueryServicePtr;
class GlobalExecutionPlan;
typedef std::shared_ptr<GlobalExecutionPlan> GlobalExecutionPlanPtr;


class MaintenanceService {
  public:
    explicit MaintenanceService(TopologyPtr topology, QueryServicePtr queryService, GlobalExecutionPlanPtr globalExecutionPlan);

    void doStuff(std::string maintType, uint64_t nodeId);

  private:
    TopologyPtr topology;
    QueryServicePtr queryService;
    GlobalExecutionPlanPtr globalExecutionPlan;

};

typedef std::shared_ptr<MaintenanceService> MaintenanceServicePtr;
} //namepsace NES
#endif//NES_MAINTENANCESERVICE_HPP
