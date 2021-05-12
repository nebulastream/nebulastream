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
std::vector<MaintenanceService::QueryMigrationMessage> MaintenanceService::submitMaintenanceRequest(TopologyNodeId nodeId, uint8_t strategy){
    std::vector<MaintenanceService::QueryMigrationMessage> results = {};
    if(strategy >3 ){
        NES_DEBUG("Not a valid strategy");
        MaintenanceService::QueryMigrationMessage  message {0,false,"Not a valid strategy"};
        results.push_back(message);
        return results;
    }
    NES_DEBUG("Applying first strategy for maintenance on node" << std::to_string(nodeId));
    std::vector<uint64_t> queryIds;
    TopologyNodePtr topologyNode = topology->findNodeWithId(nodeId);
    if(!topologyNode){
        NES_INFO("MaintenanceService: Node with ID " + std::to_string(nodeId) + " does not exit.");
        MaintenanceService::QueryMigrationMessage  message {0,false,"Topology with supplied ID doesnt exist"};
        results.push_back(message);
        return results;
    }
    markNodeForMaintenance(nodeId);
    ExecutionNodePtr executionNode = globalExecutionPlan->getExecutionNodeByNodeId(nodeId);
    if(!executionNode){
        NES_DEBUG("No queries deployed on node with id " << std::to_string(nodeId) << " can be immediatly taken down");
        MaintenanceService::QueryMigrationMessage  message {0,true,"No queries deployed on Topology Node. Node can be taken down immediatly"};
        results.push_back(message);
        return results;
    }
    //collect all Parent Querys on Exec. Node
    auto map = executionNode->getAllQuerySubPlans();
    for(auto it: map){
        for(auto it2: it.second){
            uint64_t id = it2->getQueryId();
            if(std::find(queryIds.begin(), queryIds.end(),id) == queryIds.end()){
                queryIds.push_back(id);
            }
        }
    }
    if(strategy == 1){
        return executeQueryMigrationWithRestart(queryIds);
    }
    if(strategy == 2){
        return executeQueryMigrationWithBuffer(queryIds,nodeId);
    }
    if(strategy == 3){
        return executeQueryMigrationWithoutBuffer(queryIds,nodeId);

    }
    results.push_back(QueryMigrationMessage { 0, false, "Shouldnt reach here"});
    return results;
}


std::vector<MaintenanceService::QueryMigrationMessage> MaintenanceService::executeQueryMigrationWithRestart(std::vector<QueryId> queryIds) {
    NES_DEBUG("Applying Migration with restart for all queries on given topology node");
    std::vector<MaintenanceService::QueryMigrationMessage> results = {};
    for(auto id : queryIds){
        queryCatalog->markQueryAs(id,QueryStatus::Restart);
        queryRequestQueue->add(RestartQueryRequest::create(id));
        results.push_back( MaintenanceService::QueryMigrationMessage{id,true,"Successful migration using strat 1"});
    }
    return results;
}

std::vector<MaintenanceService::QueryMigrationMessage> MaintenanceService::executeQueryMigrationWithBuffer(std::vector<QueryId> queryIds, TopologyNodeId nodeId) {
    NES_DEBUG("Applying Migration with buffer for all queries on given topology node");
    std::vector<MaintenanceService::QueryMigrationMessage> results = {};
    for(auto id : queryIds){
        queryCatalog->markQueryAs(id,QueryStatus::Migrating);
        queryRequestQueue->add(MigrateQueryRequest::create(id,nodeId,true));
        results.push_back( MaintenanceService::QueryMigrationMessage{id,true,"Successful migration using strat 2"});
    }
    return results;
}

std::vector<MaintenanceService::QueryMigrationMessage> MaintenanceService::executeQueryMigrationWithoutBuffer(std::vector<QueryId> queryIds, TopologyNodeId nodeId) {
    NES_DEBUG("Applying Migration without buffer for all queries on given topology node");
    for(auto id : queryIds){
        NES_DEBUG(id);
    }
    std::vector<MaintenanceService::QueryMigrationMessage> results = {};
    NES_DEBUG("Applying third strategy for maintenance on node" << std::to_string(nodeId));
    TopologyNodePtr topologyNode = topology->findNodeWithId(nodeId);
    if(!topologyNode){
        throw std::runtime_error("MaintenanceService: Node with ID " + std::to_string(nodeId) + " does not exit.");
    }
    return results;
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
}//namespace NES