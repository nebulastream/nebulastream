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

#include "Services/MaintenanceService.hpp"
#include <Util/Logger.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>

namespace NES {
MaintenanceService::MaintenanceService(TopologyPtr topology, QueryServicePtr queryService,
                                       GlobalExecutionPlanPtr globalExecutionPlan): topology{topology}, queryService{queryService}, globalExecutionPlan{globalExecutionPlan}{
    NES_DEBUG("MaintenanceService: Initializing");
};
MaintenanceService::~MaintenanceService() {
    NES_DEBUG("Destroying MaintenanceService");
}
std::vector<uint64_t> MaintenanceService::submitMaintenanceRequest(uint64_t nodeId, uint8_t strategy){
    NES_DEBUG("In MaintenanceService and doing Stuff!");
    if(strategy == 1){
        return firstStrat(nodeId);
    }
    else if(strategy == 2){
        return secondStrat(nodeId);
    }
    else if (strategy == 3){
        return thirdStrat(nodeId);
    }
    else{
        NES_DEBUG("Not a valid strategy");
        throw std::runtime_error("MaintenanceService: Strategy doesnt exist");
    }
}

bool MaintenanceService::markNodeForMaintenance(uint64_t nodeId) {
    TopologyNodePtr node = topology->findNodeWithId(nodeId);
    bool maintenanceFlag = node->getMaintenanceFlag();
    if(maintenanceFlag){
        NES_DEBUG("MaintenanceService: Node with id " << nodeId << " already marked for maintenance");
        return true;
    }
    NES_DEBUG("Setting maintenance flag for node with id: " + std::to_string(nodeId));
    node->setMaintenanceFlag(true);
    return true;
}

std::vector<uint64_t> MaintenanceService::firstStrat(uint64_t nodeId) {
    NES_DEBUG("Applying first strategy for maintenance on node" << std::to_string(nodeId));
    std::vector<uint64_t> parentQueryIds;
    TopologyNodePtr topologyNode = topology->findNodeWithId(nodeId);
    if(!topologyNode){
        throw std::runtime_error("MaintenanceService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
    ExecutionNodePtr executionNode = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
    if(!executionNode){
        NES_DEBUG("No queries deployed on node with id " << std::to_string(nodeId));
        markNodeForMaintenance(nodeId);
        return parentQueryIds;
    }
    auto map = executionNode->getAllQuerySubPlans();

    for(auto it: map){
        for(auto it2: it.second){
          uint64_t id = it2->getQueryId();
           if(std::find(parentQueryIds.begin(), parentQueryIds.end(),id) == parentQueryIds.end()){
               parentQueryIds.push_back(id);
           }
        }
    }
    auto  ID = parentQueryIds.front();
    NES_DEBUG("ID of first query :" << std::to_string(ID));

    return parentQueryIds;
}
std::vector<uint64_t> MaintenanceService::secondStrat(uint64_t nodeId) {
    NES_DEBUG("Applying second strategy for maintenance on node" << std::to_string(nodeId));
    std::vector<uint64_t> placeholder;
    TopologyNodePtr topologyNode = topology->findNodeWithId(nodeId);
    if(!topologyNode){
        throw std::runtime_error("MaintenanceService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
    return placeholder;
}

std::vector<uint64_t> MaintenanceService::thirdStrat(uint64_t nodeId) {
    std::vector<uint64_t> placeholder;
    NES_DEBUG("Applying third strategy for maintenance on node" << std::to_string(nodeId));
    TopologyNodePtr topologyNode = topology->findNodeWithId(nodeId);
    if(!topologyNode){
        throw std::runtime_error("MaintenanceService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
    return placeholder;
}

}//namespace NES