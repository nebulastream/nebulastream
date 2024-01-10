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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_REQUESTTYPES_TOPOLOGYREQUESTS_ADDTOPOLOGYNODEREQUEST_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_REQUESTTYPES_TOPOLOGYREQUESTS_ADDTOPOLOGYNODEREQUEST_HPP_

#include  <Optimizer/RequestTypes/Request.hpp>
#include <any>
#include <map>

namespace NES {

namespace Spatial::DataTypes::Experimental {
class GeoLocation;
}

namespace Experimental {

class AddTopologyNodeRequest;
using AddTopologyNodeRequestPtr = std::shared_ptr<AddTopologyNodeRequest>;

/**
 * @brief This request is used for adding a topology node to the topology
 */
class AddTopologyNodeRequest : public Request {
  public:
    /**
     * @brief Create instance of add topology node request
     * @param topologyNodeId : id of the topology node
     * @param address : the rpc address of the worker node
     * @param grpcPort : its grpc port
     * @param dataPort : its data port
     * @param numberOfSlots : number of available slots
     * @param geoLocation : the geographical location
     * @param workerProperties : the worker properties
     * @return shared pointer of AddTopologyNodeRequest
     */
    static AddTopologyNodeRequestPtr create(WorkerId topologyNodeId,
                                            const std::string& address,
                                            uint64_t grpcPort,
                                            uint64_t dataPort,
                                            uint16_t numberOfSlots,
                                            NES::Spatial::DataTypes::Experimental::GeoLocation& geoLocation,
                                            const std::map<std::string, std::any>& workerProperties);

    WorkerId getWorkerId() const;

    const std::string& getAddress() const;

    int64_t getGrpcPort() const;

    int64_t getDataPort() const;

    uint16_t getNumberOfSlots() const;

    Spatial::DataTypes::Experimental::GeoLocation& getGeoLocation() const;

    const std::map<std::string, std::any>& getWorkerProperties() const;

    std::string toString() override;

    RequestType getRequestType() override;

  private:
    explicit AddTopologyNodeRequest(WorkerId topologyNodeId,
                                    std::string address,
                                    uint64_t grpcPort,
                                    uint64_t dataPort,
                                    uint16_t numberOfSlots,
                                    NES::Spatial::DataTypes::Experimental::GeoLocation& geoLocation,
                                    const std::map<std::string, std::any>& workerProperties);

    WorkerId topologyNodeId;
    std::string address;
    uint64_t grpcPort;
    uint64_t dataPort;
    uint16_t numberOfSlots;
    NES::Spatial::DataTypes::Experimental::GeoLocation& geoLocation;
    std::map<std::string, std::any> workerProperties;
};
}// namespace Experimental
}// namespace NES
#endif // NES_OPTIMIZER_INCLUDE_OPTIMIZER_REQUESTTYPES_TOPOLOGYREQUESTS_ADDTOPOLOGYNODEREQUEST_HPP_
