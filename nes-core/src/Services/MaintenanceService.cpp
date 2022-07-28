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

#include <GRPC/WorkerRPCClient.hpp>
#include <Plans/Global/Execution/ExecutionNode.hpp>
#include <Plans/Global/Execution/GlobalExecutionPlan.hpp>
#include <Services/MaintenanceService.hpp>
#include <Services/QueryCatalogService.hpp>
#include <Services/RequestProcessorService.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/QueryStatus.hpp>
#include <WorkQueues/RequestQueue.hpp>
#include <WorkQueues/RequestTypes/MaintenanceRequest.hpp>
#include <WorkQueues/RequestTypes/RestartQueryRequest.hpp>

namespace NES::Experimental {

MaintenanceService::MaintenanceService(TopologyPtr topology, RequestQueuePtr queryRequestQueue)
    : topology{topology}, queryRequestQueue{queryRequestQueue} {
    NES_DEBUG("MaintenanceService: Initializing");
};

MaintenanceService::~MaintenanceService() { NES_DEBUG("Destroying MaintenanceService"); }

std::pair<bool, std::string> MaintenanceService::submitMaintenanceRequest(TopologyNodeId nodeId, MigrationType::Value type) {
    std::pair<bool, std::string> result;
    //check if topology node exists
    if (!topology->findNodeWithId(nodeId)) {
        NES_DEBUG("MaintenanceService: TopologyNode with ID: " << nodeId << " does not exist");
        result.first = false;
        result.second = "No Topology Node with ID " + std::to_string(nodeId);
        return result;
    }

    //check if valid Migration Type
    if (!MigrationType::isValidMigrationType(type)) {
        NES_DEBUG(
            "MaintenanceService: MigrationType: "
            << type
            << " is not a valid type. Type must be 1 (Restart), 2 (Migration with Buffering) or 3 (Migration without Buffering)");
        result.first = false;
        result.second = "MigrationType: " + std::to_string(type)
            + " not a valid type. Type must be either 1 (Restart), 2 (Migration with Buffering) or 3 (Migration without "
              "Buffering)";
        return result;
    }

    //create MaintenanceRequest
    //Migrations of Type RESTART are handled separately from other Migration Types and thus get their own Query Request Type
    if (type == MigrationType::Value::RESTART) {
        result.first = false;
        result.second = "RESTART currently not supported. Will be added in future";
        //functionality will be added in #2873
    } else {
        queryRequestQueue->add(MaintenanceRequest::create(nodeId, type));
        result.first = true;
        result.second = "Successfully submitted Query Migration Requests for Topology Node with ID: " + std::to_string(nodeId);
    }

    return result;
}
}//namespace NES::Experimental