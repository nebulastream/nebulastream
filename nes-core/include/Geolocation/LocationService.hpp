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
#ifndef NES_GEOLOCATION_LOCATIONSERVICE_HPP
#define NES_GEOLOCATION_LOCATIONSERVICE_HPP

#include <Geolocation/LocationProvider.hpp>
#include <memory>
#include <vector>
#include <Util/Experimental/LocationProviderType.hpp>

namespace NES {
class CoordinatorRPCClient;
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;
}

namespace NES::Experimental::Mobility {
class Location;
using LocationPtr = std::shared_ptr<Location>;
using LocationProviderPtr = std::shared_ptr<LocationProvider>;

class LocationService {
  public:

    explicit LocationService( bool isMobile, Location fieldNodeLoc);

    /**
     * Experimental
     * @brief construct a mobile worker location source. This function is experimental.
     * @param type defines the the subclass of locationsource to be used
     * @param config the config parameters for the location source
     * @return
     */
    bool createLocationProvider(LocationProviderType type, std::string config);

    /**
     * Experimental
     * Set the rpcClient which this Location Service can use to communicate with the coordinator
     * @param rpcClientPtr
     */
    void setCoordinatorRPCClient(CoordinatorRPCClientPtr rpcClientPtr);
    /**
     * Experimental
     * @brief checks if this Worker runs on a non-mobile device with a known location (Field Node)
     */
    bool isFieldNode();

    /**
     * Experimental
     * @brief check if this worker runs on a mobile device
     */
    [[nodiscard]] bool isMobileNode() const;

    /**
     * Experimental
     * @brief returns an optional containing a Location object if the node has a fixed location or
     * containing a nullopt_t if the node does not have a location
     * @return optional containing the Location
     */
    Location getLocation();

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around a geographical point
     * @param coord: Location representing the center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, Location>> getNodeIdsInRange(Location coord, double radius);

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around the location of this node
     * @param radius = radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, Location>> getNodeIdsInRange(double radius);

    /**
     * @brief method to set the Nodes Location. it does not update the topology and is meant for initialization
     * @param geoLoc: The new fixed Location to be set
     * @return success of operation
     */
    bool setFixedLocationCoordinates(const Location& geoLoc);

  private:
    CoordinatorRPCClientPtr coordinatorRpcClient;
    LocationPtr fixedLocationCoordinates;
    bool isMobile;
    LocationProviderPtr locationProvider;
};

}//namespace NES::Experimental::Mobility
#endif//NES_GEOLOCATION_LOCATIONSERVICE_HPP
