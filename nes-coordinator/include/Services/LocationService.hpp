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
#ifndef NES_COORDINATOR_INCLUDE_SERVICES_LOCATIONSERVICE_HPP_
#define NES_COORDINATOR_INCLUDE_SERVICES_LOCATIONSERVICE_HPP_

#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <nlohmann/json_fwd.hpp>

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Spatial::DataTypes::Experimental {
class GeoLocation;
}// namespace Spatial::DataTypes::Experimental

namespace Spatial::Index::Experimental {
class LocationIndex;
using LocationIndexPtr = std::shared_ptr<LocationIndex>;
}// namespace Spatial::Index::Experimental

namespace Spatial::Mobility::Experimental {
struct ReconnectPoint;
}

class LocationService {
  public:
    explicit LocationService(TopologyPtr topology, NES::Spatial::Index::Experimental::LocationIndexPtr locationIndex);

    /**
     * Get a json containing the id and the location of any node. In case the node is neither a field nor a mobile node,
     * the "location" attribute will be null
     * @param nodeId : the id of the requested node
     * @return a json in the format:
        {
            "id": <node id>,
            "location":
                  {
                      "latitude": <latitude>,
                      "longitude": <longitude>
                  }
        }
     */
    nlohmann::json requestNodeLocationDataAsJson(uint64_t nodeId);

    /**
     * @brief get a list of all mobile nodes in the system with known locations and their current positions as well as their parent nodes. Mobile nodes without known locations will not appear in the list
     * @return a json list in the format:
     * {
     *      "edges":
     *          [
     *              {
     *                  "source": <node id>,
     *                  "target": <node id>
     *              }
     *          ],
     *      "nodes":
     *          [
     *              {
     *                  "id": <node id>,
     *                  "location":
     *                      {
     *                          "latitude": <latitude>,
     *                          "longitude": <longitude>
     *                      }
     *              }
     *          ]
     *  }
     */
    nlohmann::json requestLocationAndParentDataFromAllMobileNodes();

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
    bool updatePredictedReconnect(const std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint>& addPredictions,
                                  const std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint>& removePredictions);

  private:
    /**
     * @brief convert a Location to a json representing the same coordinates
     * @param geoLocation : The location object to convert
     * @return a json array in the format:
     * [
     *   <latitude>,
     *   <longitude>,
     * ]
     */
    static nlohmann::json convertLocationToJson(NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation);

    /**
     * Use a node id and a Location to construct a Json representation containing these values.
     * @param id : the nodes id
     * @param geoLocation : the nodes location. if this is a nullptr then the "location" attribute of the returned json will be null.
     * @return a json in the format:
        {
            "id": <node id>,
            "location": [
                <latitude>,
                <longitude>
            ]
        }
     */
    static nlohmann::json convertNodeLocationInfoToJson(uint64_t id,
                                                        NES::Spatial::DataTypes::Experimental::GeoLocation geoLocation);

    NES::Spatial::Index::Experimental::LocationIndexPtr locationIndex;
    TopologyPtr topology;
};
}// namespace NES

#endif  // NES_COORDINATOR_INCLUDE_SERVICES_LOCATIONSERVICE_HPP_
