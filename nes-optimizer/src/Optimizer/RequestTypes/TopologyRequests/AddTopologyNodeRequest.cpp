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

#include <Util/Mobility/GeoLocation.hpp>
#include <Optimizer/RequestTypes/TopologyRequests/AddTopologyNodeRequest.hpp>
#include <utility>

namespace NES::Experimental {

AddTopologyNodeRequest::AddTopologyNodeRequest(TopologyNodeId topologyNodeId,
                                               std::string address,
                                               uint64_t grpcPort,
                                               uint64_t dataPort,
                                               uint16_t numberOfSlots,
                                               NES::Spatial::DataTypes::Experimental::GeoLocation& geoLocation,
                                               const std::map<std::string, std::any>& workerProperties)
    : topologyNodeId(topologyNodeId), address(std::move(address)), grpcPort(grpcPort), dataPort(dataPort),
      numberOfSlots(numberOfSlots), geoLocation(geoLocation), workerProperties(workerProperties) {}

AddTopologyNodeRequestPtr AddTopologyNodeRequest::create(TopologyNodeId topologyNodeId,
                                                         const std::string& address,
                                                         uint64_t grpcPort,
                                                         uint64_t dataPort,
                                                         uint16_t numberOfSlots,
                                                         NES::Spatial::DataTypes::Experimental::GeoLocation& geoLocation,
                                                         const std::map<std::string, std::any>& workerProperties) {
    return std::make_shared<AddTopologyNodeRequest>(
        AddTopologyNodeRequest(topologyNodeId, address, grpcPort, dataPort, numberOfSlots, geoLocation, workerProperties));
}

TopologyNodeId AddTopologyNodeRequest::getTopologyNodeId() const { return topologyNodeId; }

const std::string& AddTopologyNodeRequest::getAddress() const { return address; }

int64_t AddTopologyNodeRequest::getGrpcPort() const { return grpcPort; }

int64_t AddTopologyNodeRequest::getDataPort() const { return dataPort; }

uint16_t AddTopologyNodeRequest::getNumberOfSlots() const { return numberOfSlots; }

Spatial::DataTypes::Experimental::GeoLocation& AddTopologyNodeRequest::getGeoLocation() const { return geoLocation; }

const std::map<std::string, std::any>& AddTopologyNodeRequest::getWorkerProperties() const { return workerProperties; }

std::string AddTopologyNodeRequest::toString() {
    return "AddTopologyNodeRequest { TopologyNodeId: " + std::to_string(topologyNodeId) + ", Address: " + address
        + ", GrpcPort: " + std::to_string(grpcPort) + ", DataPort: " + std::to_string(dataPort)
        + ", Number Of Slots: " + std::to_string(numberOfSlots) + ", GeoLocation: " + geoLocation.toString() + "}";
}

RequestType AddTopologyNodeRequest::getRequestType() { return RequestType::AddTopologyNode; }

}// namespace NES::Experimental
