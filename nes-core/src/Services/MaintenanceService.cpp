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
// Created by balint on 25.12.21.
//

#include <Services/MaintenanceService.hpp>
#include <Catalogs/QueryCatalog.hpp>
#include <GRPC/WorkerRPCClient.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <Services/NESRequestProcessorService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger.hpp>
#include <WorkQueues//NESRequestQueue.hpp>
#include <WorkQueues/RequestTypes/MigrateQueryRequest.hpp>
#include <WorkQueues/RequestTypes/NESRequest.hpp>
#include <WorkQueues/RequestTypes/RestartQueryRequest.hpp>

namespace NES {
MaintenanceService::MaintenanceService(TopologyPtr topology, QueryCatalogPtr queryCatalog, NESRequestQueuePtr queryRequestQueue,
                                       GlobalExecutionPlanPtr globalExecutionPlan):
                                       topology{topology}, queryCatalog{queryCatalog},
                                       queryRequestQueue{queryRequestQueue}, globalExecutionPlan{globalExecutionPlan}
{
    NES_DEBUG("MaintenanceService: Initializing");
};
MaintenanceService::~MaintenanceService() {
    NES_DEBUG("Destroying MaintenanceService");
}
std::pair<bool, std::string> MaintenanceService::submitMaintenanceRequest(TopologyNodeId nodeId, MigrationType type){
    std::pair<bool, std::string> result;
    //check if topology node exists
    if(!topology->findNodeWithId(nodeId)){
        NES_DEBUG("MaintenanceService: TopologyNode with ID: " << nodeId << " does not exist");
        result.first = false;
        result.second = "No Topology Node with ID " + std::to_string(nodeId);
        return result;
    }
    //check if topology has corresponding execution node
    if(!globalExecutionPlan->checkIfExecutionNodeExists(nodeId)){
        NES_DEBUG("MaintenanceService: No ExecutionNode for TopologyNodeID: " << nodeId << ". Node can be taken down for maintenance immediately");
        topology->findNodeWithId(nodeId)->setMaintenanceFlag(true);
        result.first = true;
        result.second = "No ExecutionNode for TopologyNode with ID: " + std::to_string(nodeId) +". Node can be taken down for maintenance immediately";
        return result;
    }
    //check if valid Migration Type
    if(std::find(migrationTypes.begin(), migrationTypes.end(), type ) == migrationTypes.end()){
        NES_DEBUG("MaintenanceService: MigrationType: " << type << " is not a valid type. Type must be 1 (Restart), 2 (Migration with Buffering) or 3 (Migration without Buffering)");
        result.first = false;
        result.second = "MigrationType: " + std::to_string(type) + " not a valid type. Type must be either 1 (Restart), 2 (Migration with Buffering) or 3 (Migration without Buffering)";
        return result;
    }
    //get keys of map
    auto map = globalExecutionPlan->getExecutionNodeByNodeId(nodeId)->getAllQuerySubPlans();
    std::vector<uint64_t> queryIds;
    for(auto it: map){
        queryIds.push_back(it.first);
    }
    //for each query on node, create migration request
    for(auto queryId : queryIds){
        //Migrations of Type RESTART are handled separately from other Migration Types and thus get their own Query Request Type
        if(type == RESTART){
            queryCatalog->markQueryAs(queryId,QueryStatus::Restart);
            queryRequestQueue->add(RestartQueryRequest::create(queryId));
        }
        else {
            queryCatalog->markQueryAs(queryId,QueryStatus::Migrating);
            queryRequestQueue->add(MigrateQueryRequest::create(queryId,nodeId,type));
        }
    }
    result.first = true;
    result.second = "Successfully submitted Query Migration Requests for all queries on Topology Node with ID: " + std::to_string(nodeId);
    return result;
}
}//namespace NES