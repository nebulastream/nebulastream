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

#include <WorkQueues/RequestTypes/MaintenanceRequest.hpp>
#include <string>
#include <Util/magicenum/magic_enum.hpp>

namespace NES::Experimental {

MaintenanceRequestPtr MaintenanceRequest::create(TopologyNodeId nodeId, MigrationType migrationType) {
    return std::make_shared<MaintenanceRequest>(MaintenanceRequest(nodeId, migrationType));
}

MaintenanceRequest::MaintenanceRequest(TopologyNodeId nodeId, MigrationType migrationType)
    : nodeId(nodeId), migrationType(migrationType){};

MigrationType MaintenanceRequest::getMigrationType() { return migrationType; }

std::string MaintenanceRequest::toString() {
    return "MaintenanceRequest { Topology Node: " + std::to_string(nodeId) + ", withBuffer: " +
            std::string(magic_enum::enum_name(migrationType)) + "}";
}
TopologyNodeId MaintenanceRequest::getTopologyNode() { return nodeId; }
}// namespace NES::Experimental