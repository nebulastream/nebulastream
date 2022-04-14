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
#ifndef NES_GEOLOCATION_WORKERGEOSPATIALINFO_HPP
#define NES_GEOLOCATION_WORKERGEOSPATIALINFO_HPP

#include <memory>
#include <Geolocation/LocationSource.hpp>
#include <vector>

namespace NES {
class CoordinatorRPCClient;
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;
}

namespace NES::Experimental::Mobility {
class GeographicalLocation;
using GeographicalLocationPtr = std::shared_ptr<GeographicalLocation>;
using LocationSourcePtr = std::shared_ptr<LocationSource>;

class WorkerGeospatialInfo {
  public:

    explicit WorkerGeospatialInfo( bool isMobile, GeographicalLocation fieldNodeLoc);

    /**
     * Experimental
     * @brief construct a mobile worker location source. This function is experimental.
     * @param type defines the the subclass of locationsource to be used
     * @param config the config parameters for the location source
     * @return
     */
    bool createLocationSource(NES::Experimental::Mobility::LocationSource::Type type, std::string config);

    void setRPCClient(CoordinatorRPCClientPtr rpcClientPtr);
    /**
     * Experimental
     * @brief checks if this Worker runs on a non-mobile device with a known location (Field Node)
     */
    bool isFieldNode();

    /**
     * Experimental
     * @brief check if this worker runs on a mobile device
     */
    bool isMobileNode() const;

    /**
     * Experimental
     * @brief returns an optional containing a GeographicalLocation object if the node has a fixed location or
     * containing a nullopt_t if the node does not have a location
     * @return optional containing the GeographicalLocation
     */
    NES::Experimental::Mobility::GeographicalLocation getGeoLoc();

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around a geographical point
     * @param coord: GeographicalLocation representing the center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, NES::Experimental::Mobility::GeographicalLocation>> getNodeIdsInRange(NES::Experimental::Mobility::GeographicalLocation coord, double radius);

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around the location of this node
     * @param radius = radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, NES::Experimental::Mobility::GeographicalLocation>> getNodeIdsInRange(double radius);

    /**
     * @brief method to set the Nodes Location. it does not update the topology and is meant for initialization
     * @param geoLoc: The new fixed GeographicalLocation to be set
     * @return success of operation
     */
    bool setFixedLocationCoordinates(const NES::Experimental::Mobility::GeographicalLocation& geoLoc);

  private:
    CoordinatorRPCClientPtr coordinatorRpcClient;
    NES::Experimental::Mobility::GeographicalLocationPtr fixedLocationCoordinates;
    bool isMobile;
    NES::Experimental::Mobility::LocationSourcePtr locationSource;
};

}//namespace NES::Experimental::Mobility
#endif//NES_GEOLOCATION_WORKERGEOSPATIALINFO_HPP
