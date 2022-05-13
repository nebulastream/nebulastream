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
#ifndef NES_INCLUDE_SERVICES_LOCATIONSERVICE_HPP
#define NES_INCLUDE_SERVICES_LOCATIONSERVICE_HPP

#include <cpprest/json.h>
#include <memory>

namespace web::json {
class value;
}// namespace web::json

namespace NES {
class Topology;
using TopologyPtr = std::shared_ptr<Topology>;
}

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
    web::json::value requestNodeLocationDataAsJson(uint64_t nodeId);

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
    web::json::value requestLocationDataFromAllMobileNodesAsJson();

    /**
     * Use a node id and a LocationPtr to construct a Json representation containing these values.
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
    static web::json::value convertNodeLocationInfoToJson(uint64_t id, LocationPtr loc);

  private:
    LocationIndexPtr locationIndex;
    TopologyPtr topology;
};
}

#endif//NES_INCLUDE_SERVICES_LOCATIONSERVICE_HPP
