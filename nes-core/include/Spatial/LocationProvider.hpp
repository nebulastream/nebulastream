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

//TODO: do we need thais here?
class TrajectoryPredictor;
using TrajectoryPredictorPtr = std::shared_ptr<TrajectoryPredictor>;

class ReconnectSchedule;
using ReconnectSchedulePtr = std::shared_ptr<const ReconnectSchedule>;

class LocationProvider {
  public:
    explicit LocationProvider(Index::Experimental::WorkerSpatialType spatialType, Index::Experimental::Location fieldNodeLoc);

    /*
    LocationProvider(bool spatialType, Index::Experimental::Location fieldNodeLoc,
                        uint64_t parentid,
                        NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr configuration);
                        */

    /**
     * @brief default destructor
     */
    virtual ~LocationProvider() = default;

    /**
     * Experimental
     * @brief checks if this Worker runs on a non-mobile device with a known location (Field Node)
     */
    //bool isFieldNode();

    /**
     * Experimental
     * @brief check if this worker runs on a mobile device
     */
    [[nodiscard]] Index::Experimental::WorkerSpatialType getSpatialType() const;

    /**
     * Experimental
     * @brief returns an optional containing a Location object if the node has a fixed location or
     * containing a nullopt_t if the node does not have a location
     * @return optional containing the Location
     */
    Index::Experimental::LocationPtr getLocation();

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


    void setCoordinatorRPCCLient(CoordinatorRPCClientPtr coordinatorClient);


    //todo: update parameters in comments
    /**
     * Experimental
     * @brief construct a mobile worker location provider. This function is experimental.
     * @param type defines the the subclass of locationProvider to be used
     * @param config the config parameters for the location provider
     * @return
     */
    static LocationProviderPtr create(Configurations::WorkerConfigurationPtr workerConfig,
                                   NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr mobilityConfig);

    /**
     * @brief get the last known location of the device
     * @return a pair containing a goegraphical location and the time when this location was recorded
     */
    virtual std::pair<Index::Experimental::LocationPtr, Timestamp> getCurrentLocation();
  private:
    CoordinatorRPCClientPtr coordinatorRpcClient;
    Index::Experimental::LocationPtr fixedLocationCoordinates;
    Index::Experimental::WorkerSpatialType spatialType;

    TrajectoryPredictorPtr trajectoryPredictor;
};

}//namespace NES::Spatial::Mobility::Experimental
#endif//NES_GEOLOCATION_LOCATIONSERVICE_HPP
