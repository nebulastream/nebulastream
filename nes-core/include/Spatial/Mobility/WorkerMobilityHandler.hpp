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
    //todo: update doc for cosntr
    /**
     * Constructor
     * @param worker The worker to which this instance belongs
     * @param coordinatorRpcClient This workers rpc client for communicating with the coordinator
     * @param mobilityConfiguration the configuration containing settings related to the operation of the mobile device
     */
    explicit WorkerMobilityHandler(
        std::vector<uint64_t> currentParentWorkerIds,
        LocationProviderPtr locationProvider,
        CoordinatorRPCCLientPtr coordinatorRpcClient,
        Runtime::NodeEnginePtr nodeEngine,
        const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration);

    /**
     * @brief start runs in its own thread and will periodically perform the following tasks:
     * 1. keep the coordinator updated about this worker's position
     * 2. compute the reconnect schedule
     * 3. reconnect to a new parent whenever conditions are met
     //todo: explain conditions
     */
    void start();

    /**
     * tell the thread which executes start() to exit the update loop and stop execution
     * @return true if the thread was running, false if no such thread was running
     */
    bool stop();

    //todo: create issue for reconfig method for outside changes:

    //todo: remove
    /**
     * @brief: Perform a reconnect to change this workers parent in the topology to the closest node in the local node index and
     * update devicePositionTuplesAtLastReconnect, ParentId and currentParentLocation.
     * @param ownLocation: This workers current location
     */
    bool reconnectToClosestNode(const DataTypes::Experimental::GeoLocation& ownLocation, S1Angle maxDistance);

    //todo: remove
    /**
     * @brief check if the device has moved closer than the threshold to the edge of the area covered by the current local
     * spatial index. If so download new node data around the current location
     * @param currentLocation : the current location of the mobile device
     * @return true if the index was updated, false if the device is still further than the threshold away from the edge.
     */
    void updateDownloadedNodeIndex(const DataTypes::Experimental::GeoLocation& currentLocation);

  private:
    /**
     * @brief check if the device has moved further than the defined threshold from the last position that was communicated to the coordinator
     * and if so, send the new location and the time it was recorded to the coordinator and safe it as the last transmitted position
     */
    void sendLocationUpdate(const DataTypes::Experimental::Waypoint &currentWaypoint);

    /**
     * @brief inform the WorkerMobilityHandler about the latest scheduled reconnect. If the supplied reconnect data differs
     * from the previous prediction, it will be sent to the coordinator and also saved as a member of this object
     * @param scheduledReconnect : an optional containing a tuple made up of the id of the expected new parent, the expected
     * Location where the reconnect will happen, and the expected time of the reconnect. Or nullopt in case no prediction
     * exists.
     * @return true if the the supplied prediction differed from the previous prediction. false if the value did not change
     * and therefore no update was sent to the coordinator
     */
    bool sendNextPredictedReconnect(const std::optional<Mobility::Experimental::ReconnectPrediction>& scheduledReconnect);

    /**
     * @brief Buffer outgoing data, perform reconnect and unbuffer data once reconnect succeeded
     * @param oldParent : the mobile workers old parent
     * @param newParent : the mobile workers new parent
     * @return true if the reconnect was successful
     */
    bool triggerReconnectionRoutine(uint64_t oldParent, uint64_t newParent);

    /**
     * @brief Method to get all field nodes within a certain range around a geographical point
     * @param coord: Location representing the center of the query area
     * @param radius: radius in km to define query area
     * @return list of node IDs and their corresponding GeographicalLocations
     */
    DataTypes::Experimental::NodeIdToGeoLocationMap getNodeIdsInRange(const DataTypes::Experimental::GeoLocation& location,
                                                                      double radius);

    /**
     * @brief download the the field node locations within the configured distance around the devices position. If the list of the
     * downloaded positions is non empty, delete the old spatial index and replace it with the new data.
     * @param currentLocation : the device position
     * @return true if the received list of node positions was not empty
     */
    bool updateNeighbourWorkerInformation(const DataTypes::Experimental::GeoLocation& currentLocation);

    /**
     * @brief checks if the supplied position is less then the defined threshold away from the fringe of the area covered by the
     * nodes which are currently in the neighbouring worker spatial index.
     * @param currentLocation: current location of this worker
     * @return true if the device is close to the fringe and the index should be updated
     */
    bool shouldUpdateNeighbouringWorkerInformation(const DataTypes::Experimental::GeoLocation& currentLocation);

    /**
     * @brief Fetch the next reconnect point where this worker needs to connect
     * @param currentOwnLocation This worker location
     * @param currentParentLocation Current parent location
     * @return nothing if no reconnection point is available else returns the new reconnection point
     */
    std::optional<NES::Spatial::Mobility::Experimental::ReconnectPoint> getNextReconnectPoint(const DataTypes::Experimental::GeoLocation &currentOwnLocation,
                                                                                              const DataTypes::Experimental::GeoLocation& currentParentLocation);

    /**
     * @brief checks if the position supplied as an argument is further than the configured threshold from the last position
     * that was transmitted to the coordinator.
     * @param currentLocation
     * @return true if the distance is larger than the threshold
     */
    bool shouldSendCurrentWorkerPositionToCoordinator(const DataTypes::Experimental::GeoLocation& currentLocation);

    /**
     * @brief sets the position at which the worker was located when it last fetched an index of field nodes from the
     * coordinator. This position marks the center of the area covered by the downloaded field nodes.
     * @param currentLocation The device position at the time when the download was made
     */
    void setCentroidOfNeighbouringWorkerIndex(const DataTypes::Experimental::GeoLocation& currentLocation);

    //configuration
    //todo: adjust config
    uint64_t updateInterval;
    double nodeInfoDownloadRadius;
    S1Angle locationUpdateThreshold;
    S1Angle coveredRadiusWithoutThreshold;
    S1Angle defaultCoverageRadiusAngle;

    std::atomic<bool> isRunning{};
    S2Point lastTransmittedLocation;
    std::shared_ptr<std::thread> workerMobilityHandlerThread;

    std::unordered_map<uint64_t, S2Point> neighbourWorkerIdToLocationMap;
    S2PointIndex<uint64_t> neighbourWorkerSpatialIndex;
    std::optional<S2Point> centroidOfNeighbouringWorkerSpatialIndex;

    ReconnectSchedulePtr reconnectSchedule;

    //FIXME: current assumption is just one parent per mobile worker
    std::vector<S2Point> currentParentLocation;
    std::vector<uint64_t> currentParentWorkerIds;

    Runtime::NodeEnginePtr nodeEngine;
    LocationProviderPtr locationProvider;
    ReconnectSchedulePredictorPtr reconnectSchedulePredictor;
    CoordinatorRPCCLientPtr coordinatorRpcClient;

};
using WorkerMobilityHandlerPtr = std::shared_ptr<WorkerMobilityHandler>;
}// namespace Mobility::Experimental
}// namespace Spatial
}// namespace NES

#endif// NES_CORE_INCLUDE_SPATIAL_MOBILITY_RECONNECTCONFIGURATOR_HPP_
