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
#ifndef NES_CORE_INCLUDE_SERVICES_LOCATIONSERVICE_HPP_
#define NES_CORE_INCLUDE_SERVICES_LOCATIONSERVICE_HPP_

#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <nlohmann/json_fwd.hpp>

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;
}// namespace NES

namespace NES::Spatial::Index::Experimental {
class LocationIndex;
using LocationIndexPtr = std::shared_ptr<LocationIndex>;

class Location;
using LocationPtr = std::shared_ptr<Location>;

class LocationService {
  public:
    explicit LocationService(TopologyPtr topology);

    /**
     * Get a json containing the id and the location of any node. In case the node is neither a field nor a mobile node,
     * the "location" attribute will be null
     * @param nodeId : the id of the requested node
     * @return a json in the format:
        {
            "id": <node id>,
            "location": [
                <latitude>,
                <longitude>
            ]
        }
     */
    nlohmann::json requestNodeLocationDataAsJson(uint64_t nodeId);

    /**
     * @brief get a list of all mobile nodes in the system and their current positions
     * @return a json list in the format:
     * [
            {
                "id": <node id>,
                "location": [
                    <latitude>,
                    <longitude>
                ]
            }
        ]
     */
    nlohmann::json requestLocationDataFromAllMobileNodesAsJson();

    /**
     * @brief get information about a mobile workers predicted trajectory the last update position of the devices local
     * node index and the scheduled reconnects of the device
     * @param nodeId : the id of the mobile device
     * @return a json in the format:
     * {
        "indexUpdatePosition": [
            <latitude>
            <longitude>
        ],
        "pathEnd": [
            <latitude>
            <longitude>
        ],
        "pathStart": [
            <latitude>
            <longitude>
        ],
        "reconnectPoints": [
            {
                "id": <parent id>,
                "reconnectPoint": [
                    <latitude>
                    <longitude>
                ],
                "time": <timestamp>
            },
            ...
        ]
        }
     */
    nlohmann::json requestReconnectScheduleAsJson(uint64_t nodeId);

    /**
     * @brief update the information saved at the coordinator side about a mobile devices predicted next reconnect
     * @param mobileWorkerId : The id of the mobile worker whose predicted reconnect has changed
     * @param reconnectNodeId : The id of the expected new parent after the next reconnect
     * @param location : The expected location at which the device will reconnect
     * @param time : The expected time at which the device will reconnect
     * @return true if the information was processed correctly
     */
    bool updatePredictedReconnect(uint64_t mobileWorkerId, Mobility::Experimental::ReconnectPrediction);

  private:
    /**
     * @brief convert a Location to a json representing the same coordinates
     * @param location : The location object to convert
     * @return a json array in the format:
     * [
     *   <latitude>,
     *   <longitude>,
     * ]
     */
    static nlohmann::json convertLocationToJson(Location location);

    /**
     * Use a node id and a Location to construct a Json representation containing these values.
     * @param id : the nodes id
     * @param loc : the nodes location. if this is a nullptr then the "location" attribute of the returned json will be null.
     * @return a json in the format:
        {
            "id": <node id>,
            "location": [
                <latitude>,
                <longitude>
            ]
        }
     */
    static nlohmann::json convertNodeLocationInfoToJson(uint64_t id, Location loc);

    LocationIndexPtr locationIndex;
    TopologyPtr topology;
};
}// namespace NES::Spatial::Index::Experimental

#endif// NES_CORE_INCLUDE_SERVICES_LOCATIONSERVICE_HPP_
