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
#ifndef NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTCONFIGURATOR_HPP_
#define NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTCONFIGURATOR_HPP_

#include <Spatial/DataTypes/GeoLocation.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Util/TimeMeasurement.hpp>
#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

#ifdef S2DEF
#include <s2/s1angle.h>
#include <s2/s2point.h>
#endif

namespace NES {
class NesWorker;
using NesWorkerPtr = std::shared_ptr<NesWorker>;

class CoordinatorRPCClient;
using CoordinatorRPCCLientPtr = std::shared_ptr<CoordinatorRPCClient>;

namespace Runtime {
class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;
}

namespace Configurations::Spatial::Mobility::Experimental {
class WorkerMobilityConfiguration;
using WorkerMobilityConfigurationPtr = std::shared_ptr<WorkerMobilityConfiguration>;
}// namespace Configurations::Spatial::Mobility::Experimental

namespace Spatial {
namespace Mobility::Experimental {
class ReconnectSchedulePredictor;
using ReconnectSchedulePredictorPtr = std::shared_ptr<ReconnectSchedulePredictor>;

class ReconnectSchedule;
using ReconnectSchedulePtr = std::unique_ptr<ReconnectSchedule>;

struct ReconnectPoint;

/**
* @brief This class runs in an independent thread at worker side and is responsible for mobility aspect of a worker.
 * It has the following three functions:
 * 1. Updates coordinator about the current location (*whenever location is changed)
 * 2. Updates coordinator about the next predicted re-connection point based on the current location.
 * 3. Performs reconnection to a new base worker and informs coordinator about change to the parent worker.
 * 4. Initiates mechanisms to prevent query interruption (Un-/buffering, reconfigure sink operators)
 * This class is not thread safe!
*/
class WorkerMobilityHandler {
  public:
    /**
     * Constructor
     * @param worker The worker to which this instance belongs
     * @param coordinatorRpcClient This workers rpc client for communicating with the coordinator
     * @param mobilityConfiguration the configuration containing settings related to the operation of the mobile device
     */
    explicit WorkerMobilityHandler(
        uint64_t parentId,
        LocationProviderPtr locationProvider,
        CoordinatorRPCCLientPtr coordinatorRpcClient,
        Runtime::NodeEnginePtr nodeEngine,
        const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration);

    /**
     * @brief check if the device has moved further than the defined threshold from the last position that was communicated to the coordinator
     * and if so, send the new location and the time it was recorded to the coordinator and safe it as the last transmitted position
     */
    void sendLocationUpdate(const DataTypes::Experimental::Waypoint &currentWaypoint);

    /**
     * @brief keep the coordinator updated about this devices position by periodically calling sendLocationUpdate()
     */
    void startLocationUpdatingAndReconnectScheduling();

    /**
     * tell the thread which executes startLocationUpdatingAndReconnectScheduling() to exit the update loop and stop execution
     * @return true if the thread was running, false if no such thread was running
     */
    bool stopPeriodicUpdating();

    /**
     * @brief inform the WorkerMobilityHandler about the latest scheduled reconnect. If the supplied reconnect data differs
     * from the previous prediction, it will be sent to the coordinator and also saved as a member of this object
     * @param scheduledReconnect : an optional containing a tuple made up of the id of the expected new parent, the expected
     * Location where the reconnect will happen, and the expected time of the reconnect. Or nullopt in case no prediction
     * exists.
     * @return true if the the supplied prediction differed from the previous prediction. false if the value did not change
     * and therefore no update was sent to the coordinator
     */
    bool sendScheduledReconnect(const std::optional<Mobility::Experimental::ReconnectPrediction>& scheduledReconnect);

    /**
     * @brief change the mobile workers position in the topology by giving it a new parent
     * @param oldParent : the mobile workers old parent
     * @param newParent : the mobile workers new parent
     * @return true if the parents were successfully exchanged
     */
    bool reconnect(uint64_t oldParent, uint64_t newParent);

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around a geographical point
     * @param coord: Location representing the center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    DataTypes::Experimental::NodeIdToGeoLocationMap getNodeIdsInRange(const DataTypes::Experimental::GeoLocation& location,
                                                                      double radius);

    /**
     * Experimental
     * @brief Method to get all field nodes within a certain range around the location of this node
     * @param radius = radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    DataTypes::Experimental::NodeIdToGeoLocationMap getNodeIdsInRange(double radius);

    /**
     * @brief download the the field node locations within the configured distance around the devices position. If the list of the
     * downloaded positions is non empty, delete the old spatial index and replace it with the new data.
     * @param currentLocation : the device position
     * @return true if the received list of node positions was not empty
     */
    bool downloadFieldNodes(const DataTypes::Experimental::GeoLocation& currentLocation);

    /**
     * @brief: Perform a reconnect to change this workers parent in the topology to the closest node in the local node index and
     * update devicePositionTuplesAtLastReconnect, ParentId and currentParentLocation.
     * @param ownLocation: This workers current location
     */
    bool reconnectToClosestNode(const DataTypes::Experimental::GeoLocation& ownLocation, S1Angle maxDistance);

    /**
     * @brief check if the device has moved closer than the threshold to the edge of the area covered by the current local
     * spatial index. If so download new node data around the current location
     * @param currentLocation : the current location of the mobile device
     * @return true if the index was updated, false if the device is still further than the threshold away from the edge.
     */
    void updateDownloadedNodeIndex(const DataTypes::Experimental::GeoLocation& currentLocation);

    /**
     * @brief determines if the device should reconnect to a new parent and returns the parent id if the need for a reconnect
     * exists
     * @param currentOwnLocation The devices own location
     * @param currentParentLocation
     * @return
     */
    std::optional<uint64_t> getReconnectParent(const DataTypes::Experimental::GeoLocation& currentOwnLocation,
                                               const DataTypes::Experimental::GeoLocation& currentParentLocation);

    /**
     * @brief checks if the supplied position is less then the defined threshold away from the fringe of the area covered by the
     * nodes which are currently in the index.
     * @param currentLocation
     * @return true if the device is close to the fringe and the index should be updated
     */
    bool checkNodeIndexThreshold(const DataTypes::Experimental::GeoLocation& currentLocation);

    /**
     * @brief checks if the position supplied as an argument is further than the configured threshold from the last position
     * that was transmitted to the coordinator.
     * @param currentLocation
     * @return true if the distance is larger than the threshold
     */
    bool checkPositionThreshold(const DataTypes::Experimental::GeoLocation& currentLocation);

    /**
     * @brief sets the position at which the device was located when it last fetched an index of field nodes from the
     * coordinator. This position marks the center of the area covered by the downloaded field nodes.
     * @param currentLocation The device position at the time when the download was made
     */
    void setPositionOfLastIndexUpdate(const DataTypes::Experimental::GeoLocation& currentLocation);

  private:
    std::atomic<bool> sendUpdates{};
    std::recursive_mutex reconnectConfigMutex;
    CoordinatorRPCCLientPtr coordinatorRpcClient;
    std::optional<ReconnectPrediction> lastTransmittedReconnectPrediction;
#ifdef S2DEF
    S2Point lastTransmittedLocation;
    S1Angle locationUpdateThreshold;
#endif
    uint64_t locationUpdateInterval;
    //todo: instead of both of these have one interval and 2 rates?
    uint64_t pathPredictionUpdateInterval;
    std::shared_ptr<std::thread> sendLocationUpdateThread;

    double nodeInfoDownloadRadius;
    LocationProviderPtr locationProvider;
    std::unordered_map<uint64_t, S2Point> fieldNodeMap;
    std::optional<S2Point> positionOfLastNodeIndexUpdate;
    S2PointIndex<uint64_t> fieldNodeIndex;
    ReconnectSchedulePredictorPtr reconnectSchedulePredictor;
    ReconnectSchedulePtr reconnectSchedule;

    std::optional<S2Point> currentParentLocation;
    uint64_t parentId;
    S1Angle coveredRadiusWithoutThreshold;

    Runtime::NodeEnginePtr nodeEngine;

};
using WorkerMobilityHandlerPtr = std::shared_ptr<WorkerMobilityHandler>;
}// namespace Mobility::Experimental
}// namespace Spatial
}// namespace NES

#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTCONFIGURATOR_HPP_
