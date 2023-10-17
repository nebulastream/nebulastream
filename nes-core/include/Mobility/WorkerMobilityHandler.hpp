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
#ifndef NES_CORE_INCLUDE_SPATIAL_MOBILITY_WORKERMOBILITYHANDLER_HPP_
#define NES_CORE_INCLUDE_SPATIAL_MOBILITY_WORKERMOBILITYHANDLER_HPP_

#include<Util/Mobility/GeoLocation.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
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
class CoordinatorRPCClient;
using CoordinatorRPCCLientPtr = std::shared_ptr<CoordinatorRPCClient>;

namespace Runtime {
class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;
}// namespace Runtime

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
     * @param locationProvider the location provider from which the workers current locations can be obtained
     * @param coordinatorRpcClient This workers rpc client for communicating with the coordinator
     * @param nodeEngine this workers node engine which is needed to initiate buffering before every reconnect
     * @param mobilityConfiguration the configuration containing settings related to the operation of the mobile device
     */
    explicit WorkerMobilityHandler(
        const LocationProviderPtr& locationProvider,
        CoordinatorRPCCLientPtr coordinatorRpcClient,
        Runtime::NodeEnginePtr nodeEngine,
        const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration);

    /**
     * @brief starts reconnect scheduling by creatin a new thread in which the
     * run() function will run
     * @param currentParentWorkerIds a list of the workers current parents
     */
    void start(std::vector<uint64_t> currentParentWorkerIds);

    /**
     * tell the thread which executes start() to exit the update loop and stop execution
     * @return true if the thread was running, false if no such thread was running
     */
    bool stop();

  private:
    /**
     * @brief check if the device has moved further than the defined threshold from the last position that was communicated to the coordinator
     * and if so, send the new location and the time it was recorded to the coordinator and safe it as the last transmitted position
     * @param lastTransmittedLocation the last location that was transmitted to the coordinator
     * @param currentWaypoint the waypoint containing the devices current location
     */
    void sendCurrentWaypoint(const DataTypes::Experimental::Waypoint& currentWaypoint);

    /**
     * @brief inform the WorkerMobilityHandler about the latest scheduled reconnect. If the supplied reconnect data differs
     * from the previous prediction, it will be sent to the coordinator and also saved as a member of this object
     * @param scheduledReconnects : an optional containing the current reconnect plan made up of the ids of the expected
     * new parents, the expected Locations where the reconnects will happen, and the expected time of the reconnect.
     * Or nullopt in case no prediction exists.
     * @param removedReconnects : the previous reconnect plan which is considered obsolete and should therefore be
     * removed on the coordinator side or nullopt if there are not old predcitions that need to be removed
     * @return true if the the data was succesfully sent
     */
    bool
    sendNextPredictedReconnect(const std::optional<NES::Spatial::Mobility::Experimental::ReconnectSchedule>& scheduledReconnects,
                               const std::optional<NES::Spatial::Mobility::Experimental::ReconnectSchedule>& removedReconnects);

    /**
     * @brief Buffer outgoing data, perform reconnect and unbuffer data once reconnect succeeded
     * @param oldParent : the mobile workers old parent
     * @param newParent : the mobile workers new parent
     * @param currentParentWorkerIds : a list of the ids of this workers current parents
     * @param currentParentLocations : a list of this workers parents locations if they are known
     * @param neighbourWorkerIdToLocationMap : a map on the ids of other workers nodes in the vicinity of this worker,
     *  which contains each workers location.
     * @return true if the reconnect was successful
     */
    bool triggerReconnectionRoutine(uint64_t& currentParentId, uint64_t newParentId);

    /**
     * @brief Method to get all field nodes within a certain range around a geographical point
     * @param location: Location representing the center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    DataTypes::Experimental::NodeIdToGeoLocationMap getNodeIdsInRange(const DataTypes::Experimental::GeoLocation& location,
                                                                      double radius);

#ifdef S2DEF
    /**
     * @brief get a fixed location node's geolocation from the downloaded index
     * @param nodeId: the id of the fixed location node
     * @return the geolocation of the node or nullopt if no node with the replied id was found in the node index
     */
    static std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation>
    getNodeGeoLocation(uint64_t nodeId, std::unordered_map<uint64_t, S2Point> neighbourWorkerIdToLocationMap);

    /**
     * @brief download the the field node locations within the configured distance around the devices position. If the list of the
     * downloaded positions is non empty, delete the old spatial index and replace it with the new data.
     * @param currentLocation : the device position
     * @param neighbourWorkerIdToLocationMap : the map on worker ids to be updated.
     * @param neighbourWorkerSpatialIndex : the spatial index to be updated
     * @return true if the received list of node positions was not empty
     */
    bool updateNeighbourWorkerInformation(const DataTypes::Experimental::GeoLocation& currentLocation,
                                          std::unordered_map<uint64_t, S2Point>& neighbourWorkerIdToLocationMap,
                                          S2PointIndex<uint64_t>& neighbourWorkerSpatialIndex);

    /**
     * @brief checks if the supplied position is less then the defined threshold away from the fringe of the area covered by the
     * nodes which are currently in the neighbouring worker spatial index.
     * @param centroidOfNeighbouringWorkerSpatialIndex : an optional containing the center of the area covered by the
     * current spatial index or nullopt if not such index has been downloaded yet.
     * @param currentWaypoint: current location of this worker
     * @return true if the device is close to the fringe and the index should be updated
     */
    bool shouldUpdateNeighbouringWorkerInformation(const std::optional<S2Point>& centroidOfNeighbouringWorkerSpatialIndex,
                                                   const DataTypes::Experimental::Waypoint& currentWaypoint);

    /**
     * @brief Fetch the next reconnect point where this worker needs to connect
     * @param reconnectSchedule an optional containing the current reconnect schedule or nullopt if no schedule has
     * been calculated
     * @param currentOwnLocation This worker location
     * @param currentParentLocation Current parent location
     * @param neighbourWorkerSpatialIndex a spatial index containing other workers in the vicinity which could be
     * potential new parents
     * @return nothing if no reconnection point is available else returns the new reconnection point
     */
    std::optional<NES::Spatial::Mobility::Experimental::ReconnectPoint>
    getNextReconnectPoint(std::optional<ReconnectSchedule>& reconnectSchedule,
                          const DataTypes::Experimental::GeoLocation& currentOwnLocation,
                          const std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation>& currentParentLocation,
                          const S2PointIndex<uint64_t>& neighbourWorkerSpatialIndex);

    /**
     * @brief checks if the position supplied as an argument is further than the configured threshold from the last position
     * that was transmitted to the coordinator.
     * @param lastTransmittedLocation: the last device location that was transmitted to the coordinator
     * @param currentWaypoint: current waypoint of the worker node
     * @return true if the distance is larger than the threshold
     */
    bool shouldSendCurrentWaypointToCoordinator(const std::optional<S2Point>& lastTransmittedLocation,
                                                const DataTypes::Experimental::GeoLocation& currentLocation);

    /**
     * @brief retrieves the id of the closest node within the supplied radius in the neighbouring workers index if such a node exists.
     * @param currentOwnLocation a GeoLocation marking the center of the area to be searched
     * @param radius The maximum distance between the supplied location and any result nodes
     * @param neighbourWorkerSpatialIndex a spatial index containing other workers in the vicinity
     * @return An optional containing the node id of the closest node or nullopt if no node could be found with the radius
     */
    std::optional<uint64_t> getClosestNodeId(const DataTypes::Experimental::GeoLocation& currentOwnLocation,
                                             S1Angle radius,
                                             const S2PointIndex<uint64_t>& neighbourWorkerSpatialIndex);
#endif

    /**
     * @brief this function runs in its own thread and will periodically perform the following tasks:
     * 1. keep the coordinator updated about this worker's position
     * 2. compute the reconnect schedule
     * 3. reconnect to a new parent whenever conditions are met
     * The conditions under which a node will reconnect include:
     * Case 1: If the workers cannot retrieve any location information about its current parent and there is at least 1 other node
     * within lass distance than the default coverage radius, the worker will reconnect to the closest node within that distance
     * Case 2: If the workers current parent is further away then the default coverage radius from the worker:
     *      Case 2.1: If there is a scheduled reconnect and the reconnect node is closer than the current parent, the worker will
     *      connect to the node scheduled for reconnect.
     *      Case 2.2: If there is no scheduled reconnect but there is another node within the default coverage radius, the worker
     *      will reconnect to the closest node in that radius.
     */
    //FIXME: current assumption is just one parent per mobile worker
    void run(std::vector<uint64_t> currentParentWorkerIds);

    //configuration
    uint64_t updateInterval;
    double nodeInfoDownloadRadius;

#ifdef S2DEF
    S1Angle locationUpdateThreshold;
    S1Angle coveredRadiusWithoutThreshold;
    S1Angle defaultCoverageRadiusAngle;
#endif

    std::atomic<bool> isRunning{};
    std::shared_ptr<std::thread> workerMobilityHandlerThread;

    Runtime::NodeEnginePtr nodeEngine;
    LocationProviderPtr locationProvider;
    ReconnectSchedulePredictorPtr reconnectSchedulePredictor;
    CoordinatorRPCCLientPtr coordinatorRpcClient;
};
using WorkerMobilityHandlerPtr = std::shared_ptr<WorkerMobilityHandler>;
}// namespace Mobility::Experimental
}// namespace Spatial
}// namespace NES

#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_WORKERMOBILITYHANDLER_HPP_
