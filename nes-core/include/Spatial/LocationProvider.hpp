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

#include <Util/Experimental/LocationProviderType.hpp>
#include <memory>
#include <vector>
#include <Util/TimeMeasurement.hpp>
#include <Util/Experimental/WorkerSpatialType.hpp>
#include <Common/Location.hpp>
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

}// namespace NES::Spatial::Index::Experimental

namespace Configurations {

class WorkerConfiguration;
using WorkerConfigurationPtr = std::shared_ptr<WorkerConfiguration>;

namespace Spatial::Mobility::Experimental {
class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;
}
}
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
     * @param spatialType the type of worker: NO_LOCATION, FIELD_NODE (fixed location), MOBILE_NODE or INVALID
     * @param fieldNodeLoc the fixed location if this worker is a field node. Will be ignored if the spatial type is not FIELD_NODE
     */
    explicit LocationProvider(Index::Experimental::WorkerSpatialType spatialType, Index::Experimental::Location fieldNodeLoc);

    /**
     * @brief default destructor
     */
    virtual ~LocationProvider() = default;

    /**
     * Experimental
     * @brief check if this worker runs on a mobile device, has a fixed location, of if there is no location data available
     */
    [[nodiscard]] Index::Experimental::WorkerSpatialType getSpatialType() const;

    /**
     * Experimental
     * @brief get the workers location.
     * @return Location object containig the current location if the worker runs on a mobile device, the fixed location if
     * the worker is a field node or an invalid location if there is no known location
     */
    Index::Experimental::Location getLocation();

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around a geographical point
     * @param coord: Location representing the center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, Index::Experimental::Location>> getNodeIdsInRange(Index::Experimental::Location coord,
                                                                                      double radius);

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around the location of this node
     * @param radius = radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    std::vector<std::pair<uint64_t, Index::Experimental::Location>> getNodeIdsInRange(double radius);


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
     * @param mobilityConfig : this worker Mobility configuration
     * @return a smart pointer to an object of the LocationProvider class or one of its subclasses
     */
    static LocationProviderPtr create(Configurations::WorkerConfigurationPtr workerConfig,
                                   NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfig);

    /**
     * @brief get the last known location of the device
     * @return a pair containing a goegraphical location and the time when this location was recorded
     */
    virtual std::pair<Index::Experimental::Location, Timestamp> getCurrentLocation();

  private:
    CoordinatorRPCClientPtr coordinatorRpcClient;
    Index::Experimental::Location fixedLocationCoordinates;
    Index::Experimental::WorkerSpatialType spatialType;

    TrajectoryPredictorPtr trajectoryPredictor;
};
}//namespace NES::Spatial::Mobility::Experimental
#endif//NES_GEOLOCATION_LOCATIONSERVICE_HPP
