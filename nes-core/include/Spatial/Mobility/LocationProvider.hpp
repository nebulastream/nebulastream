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
#ifndef NES_CORE_INCLUDE_SPATIAL_MOBILITY_LOCATIONPROVIDER_HPP_
#define NES_CORE_INCLUDE_SPATIAL_MOBILITY_LOCATIONPROVIDER_HPP_

#include <Spatial/Index/Location.hpp>
#include <Util/Experimental/LocationProviderType.hpp>
#include <Util/Experimental/NodeType.hpp>
#include <Util/TimeMeasurement.hpp>
#include <memory>
#include <vector>
#ifdef S2DEF
#include <s2/s1chord_angle.h>
#include <s2/s2point.h>
#include <s2/s2point_index.h>
#endif

class S2Polyline;
using S2PolylinePtr = std::shared_ptr<S2Polyline>;

namespace NES {
class CoordinatorRPCClient;
using CoordinatorRPCClientPtr = std::shared_ptr<CoordinatorRPCClient>;

class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

namespace Spatial::Index::Experimental {
class Location;
using LocationPtr = std::shared_ptr<Location>;
class Waypoint;
using WaypointPtr = std::shared_ptr<Waypoint>;
using NodeIdsMapPtr = std::shared_ptr<std::unordered_map<uint64_t, Index::Experimental::Location>>;
}// namespace Spatial::Index::Experimental

namespace Configurations {

class WorkerConfiguration;
using WorkerConfigurationPtr = std::shared_ptr<WorkerConfiguration>;

namespace Spatial::Mobility::Experimental {
class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;
}// namespace Spatial::Mobility::Experimental
}// namespace Configurations
}// namespace NES

namespace NES::Spatial::Mobility::Experimental {
class LocationProvider;
using LocationProviderPtr = std::shared_ptr<LocationProvider>;

class TrajectoryPredictor;
using TrajectoryPredictorPtr = std::shared_ptr<TrajectoryPredictor>;

/**
 * @brief this class is the worker-side interface to access all location related information. It is allows querying for the fixed
 * position of a field node or the current position of a mobile node as well as making calls to the coordinator in order to
 * download field node spatial data to a mobile worker. To allow querying for the position of a mobile device, the class needs to
 * be subclassed to integrate with the device specific interfaces
 */
class LocationProvider {
  public:
    /**
     * Constructor
     * @param spatialType the type of worker: NO_LOCATION, FIXED_LOCATION (fixed location), MOBILE_NODE or INVALID
     * @param fieldNodeLoc the fixed location if this worker is a field node. Will be ignored if the spatial type is not FIXED_LOCATION
     */
    explicit LocationProvider(Index::Experimental::NodeType spatialType, Index::Experimental::Location fieldNodeLoc);

    /**
     * @brief default destructor
     */
    virtual ~LocationProvider() = default;

    /**
     * Experimental
     * @brief check if this worker runs on a mobile device, has a fixed location, of if there is no location data available
     */
    [[nodiscard]] Index::Experimental::NodeType getNodeType() const;

    /**
     * Experimental
     * @brief get the workers location.
     * @return Location object containig the current location if the worker runs on a mobile device, the fixed location if
     * the worker is a field node or an invalid location if there is no known location
     */
    Index::Experimental::WaypointPtr getWaypoint();

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around a geographical point
     * @param coord: Location representing the center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */

    Index::Experimental::NodeIdsMapPtr getNodeIdsInRange(Index::Experimental::LocationPtr location, double radius);

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around the location of this node
     * @param radius = radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::shared_ptr<std::unordered_map<uint64_t, Index::Experimental::Location>> getNodeIdsInRange(double radius);

    /**
     * @brief method to set the Nodes Location. it does not update the topology and is meant for initialization
     * @param geoLoc: The new fixed Location to be set
     * @return success of operation
     */
    bool setFixedLocationCoordinates(const Index::Experimental::Location& geoLoc);

    /**
     * @brief pass a pointer to this worker coordinator rpc client, so the location provider can query information from the coordinator
     * @param coordinatorClient : a smart pointer to the coordinator rpc client object
     */
    void setCoordinatorRPCCLient(CoordinatorRPCClientPtr coordinatorClient);

    /**
     * Experimental
     * @brief construct a mobile workers location provider. The supplied worker mobility configuration will be used to determine
     * which subclass of LocationProvider should be used. This function is experimental.
     * @param workerConfig : this workers WorkerConfiguration
     * @return a smart pointer to an object of the LocationProvider class or one of its subclasses
     */
    static LocationProviderPtr create(Configurations::WorkerConfigurationPtr workerConfig);

    /**
     * @brief get the last known location of the device
     * @return a pair containing a goegraphical location and the time when this location was recorded
     */
    virtual Index::Experimental::WaypointPtr getCurrentWaypoint();

  private:
    CoordinatorRPCClientPtr coordinatorRpcClient;
    Index::Experimental::LocationPtr fixedLocationCoordinates;
    Index::Experimental::NodeType nodeType;

    TrajectoryPredictorPtr trajectoryPredictor;
};
}//namespace NES::Spatial::Mobility::Experimental
#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_LOCATIONPROVIDER_HPP_
