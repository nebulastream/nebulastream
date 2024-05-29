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
#ifndef NES_WORKER_INCLUDE_LATENCY_WORKERLATENCYHANDLER_HPP_
#define NES_WORKER_INCLUDE_LATENCY_WORKERLATENCYHANDLER_HPP_

#include <Util/Latency/NetworkCoordinate.hpp>
#include <Util/TimeMeasurement.hpp>
#include <atomic>
#include <boost/geometry.hpp>
#include <boost/geometry/geometries/point.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>

namespace NES {
class CoordinatorRPCClient;
using CoordinatorRPCCLientPtr = std::shared_ptr<CoordinatorRPCClient>;

namespace Runtime {
class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;
}// namespace Runtime

//TODO: Move this to ReconnectSchedule after implementing it like in GeoLocations
namespace Synthetic::DataTypes::Experimental {
using NodeIdToNetworkCoordinateMap = std::unordered_map<WorkerId, NetworkCoordinate>;
}// namespace Synthetic::DataTypes::Experimental

namespace Configurations::Synthetic::Latency::Experimental {
class WorkerLatencyConfiguration;
using WorkerLatencyConfigurationPtr = std::shared_ptr<WorkerLatencyConfiguration>;
}// namespace Configurations::Synthetic::Latency::Experimental

namespace Synthetic::Latency::Experimental {

using Point = boost::geometry::model::point<double, 2, boost::geometry::cs::cartesian>;

using CoordinatePair = std::pair<Point, WorkerId>;

using NetworkCoordinateIndex = boost::geometry::index::rtree<CoordinatePair, boost::geometry::index::quadratic<16>>;

/**
* @brief This class runs in an independent thread at worker side and is responsible for latency estimation aspect of a worker.
* It has the following functions:
* 1. Updates coordinator about the current network coordinates (*whenever the coordinate is changed)
* 2. Updates coordinator about the next predicted re-connection point based on the current location.
* 3. Performs reconnection to a new base worker and informs coordinator about change to the parent worker.
* 4. Initiates mechanisms to prevent query interruption (Un-/buffering, reconfigure sink operators)
* This class is not thread safe!
*/
class WorkerLatencyHandler {
  public:
    /**
    * Constructor
    * @param networkCoordinateProvider the network coordinate provider from which the workers current coordinates can be obtained
    * @param coordinatorRpcClient This workers rpc client for communicating with the coordinator
    * @param nodeEngine this workers node engine which is needed to initiate buffering before every reconnect
    * @param latencyConfiguration the configuration containing settings related to the operation of the node
    */
    explicit WorkerLatencyHandler(
        const NetworkCoordinateProviderPtr& networkCoordinateProvider,
        CoordinatorRPCCLientPtr coordinatorRpcClient,
        Runtime::NodeEnginePtr nodeEngine,
        const Configurations::Synthetic::Latency::Experimental::WorkerLatencyConfigurationPtr& latencyConfiguration);

    /**
    * @brief starts reconnect scheduling by creating a new thread in which the
    * run() function will run
    * @param currentParentWorkerIds a list of the workers current parents
    */
    void start(const std::vector<WorkerId>& currentParentWorkerIds);

    /**
    * tell the thread which executes start() to exit the update loop and stop execution
    * @return true if the thread was running, false if no such thread was running
    */
    bool stop();

  private:
    /**
    * @brief check if the node has moved further than the defined threshold from the last network coordinate that was communicated to the coordinator
    * and if so, send the new coordinate and the time it was recorded to the coordinator and save it as the last transmitted coordinate
    * @param currentWaypoint the waypoint containing the devices current coordinate
    */
    void sendCurrentWaypoint(const DataTypes::Experimental::Waypoint& currentWaypoint);


    /**
    * @brief Method to get all nodes within a certain range around a network coordinate point
    * @param coordinate: Network coordinate representing the center of the query area
    * @param radius: radius in ms to define query area
    * @return list of node IDs and their corresponding Network Coordinates
    */
    DataTypes::Experimental::NodeIdToNetworkCoordinateMap getNodeIdsInRange(const DataTypes::Experimental::NetworkCoordinate& coordinate,
                                                                      double radius);
    /**
    * @brief get a node's network coordinate from the downloaded index
    * @param nodeId: the id of the node
    * @return the network coordinate of the node or nullopt if no node with the replied id was found in the node index
    */
    static std::optional<NES::Synthetic::DataTypes::Experimental::NetworkCoordinate>
    getNodeNetworkCoordinate(WorkerId nodeId, std::unordered_map<WorkerId, Point> neighbourWorkerIdToNetworkCoordinateMap);

    /**
    * @brief download the the node coordinates within the configured distance (in the synthetic coordinate space)
    * around the devices coordinate. If the list of the downloaded coordinates is non empty,
    * delete the old synthetic index and replace it with the new data.
    * @param currentCoordinate : the device's coordinate in the synthetic space
    * @param neighborWorkerIdToCoordinateMap : the map on worker ids to be updated.
    * @param neighbourWorkerSyntheticIndex : the synthetic index to be updated
    * @return true if the received list of node positions was not empty
    */
    bool updateNeighbourWorkerInformation(const DataTypes::Experimental::NetworkCoordinate& currentCoordinate,
                                          std::unordered_map<WorkerId, Point>& neighborWorkerIdToCoordinateMap,
                                          NetworkCoordinateIndex& neighbourWorkerSyntheticIndex);

    /**
    * @brief checks if the position supplied as an argument is further than the configured threshold from the last coordinate
    * that was transmitted to the coordinator.
    * @param lastTransmittedCoordinate: the last device coordinate that was transmitted to the coordinator
    * @param currentWaypoint: current waypoint of the worker node
    * @return true if the distance is larger than the threshold
    */
    bool shouldSendCurrentWaypointToCoordinator(const std::optional<Point>& lastTransmittedCoordinate,
                                                const DataTypes::Experimental::NetworkCoordinate& currentCoordinate);


    /**
    * @brief this function runs in its own thread and will keep the coordinator updated about this worker's coordinate
    */
    void run(const std::vector<WorkerId>& currentParentWorkerIds);

    //configuration
    uint64_t updateInterval;
    double nodeInfoDownloadRadius;

    double networkCoordinateUpdateThreshold;
    double defaultCoverageRadius;

    std::atomic<bool> isRunning{};
    std::shared_ptr<std::thread> workerLatencyHandlerThread;

    Runtime::NodeEnginePtr nodeEngine;
    NetworkCoordinateProviderPtr networkCoordinateProvider;
    CoordinatorRPCCLientPtr coordinatorRpcClient;
};
using WorkerLatencyHandlerPtr = std::shared_ptr<WorkerLatencyHandler>;


}// namespace Synthetic::Latency::Experimental
}// namespace NES

#endif// NES_WORKER_INCLUDE_LATENCY_WORKERLATENCYHANDLER_HPP_
