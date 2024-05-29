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

#include <Configurations/Worker/WorkerLatencyConfiguration.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Latency/NetworkCoordinateProviders/NetworkCoordinateProvider.hpp>
#include <Latency/WorkerLatencyHandler.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Util/Latency/NetworkCoordinate.hpp>
#include <Util/Latency/Waypoint.hpp>
#include <Util/TopologyLinkInformation.hpp>
#include <utility>


namespace NES {
NES::Synthetic::Latency::Experimental::WorkerLatencyHandler::WorkerLatencyHandler(
    const NetworkCoordinateProviderPtr& networkCoordinateProvider,
    CoordinatorRPCClientPtr coordinatorRpcClient,
    Runtime::NodeEnginePtr nodeEngine,
    const Configurations::Synthetic::Latency::Experimental::WorkerLatencyConfigurationPtr& latencyConfiguration)
    : updateInterval(latencyConfiguration->latencyHandlerUpdateInterval),
      networkCoordinateUpdateThreshold(latencyConfiguration->sendDeviceNetworkCoordinateUpdateThreshold),
      defaultCoverageRadius(latencyConfiguration->defaultCoverageRadius.getValue()),
      isRunning(false), nodeEngine(std::move(nodeEngine)),
      networkCoordinateProvider(networkCoordinateProvider), coordinatorRpcClient(std::move(coordinatorRpcClient)) {}

bool NES::Synthetic::Latency::Experimental::WorkerLatencyHandler::updateNeighbourWorkerInformation(
    const DataTypes::Experimental::NetworkCoordinate& currentCoordinate,
    std::unordered_map<WorkerId, Point>& neighborWorkerIdToCoordinateMap,
    NetworkCoordinateIndex& neighbourWorkerSyntheticIndex) {
    if (!currentCoordinate.isValid()) {
        NES_WARNING("invalid coordinate, cannot download neighbor nodes");
        return false;
    }
    NES_DEBUG("Downloading nodes in range")
    //get current coordinate and download node information from coordinator
    auto nodeMapPtr = getNodeIdsInRange(currentCoordinate, nodeInfoDownloadRadius);

    //if we actually received nodes in our vicinity, we can clear the old nodes
    if (!nodeMapPtr.empty()) {
        neighborWorkerIdToCoordinateMap.clear();
        neighbourWorkerSyntheticIndex.clear();
    } else {
        return false;
    }

    //insert node info into synthetic index and map on node ids
    for (auto [nodeId, coordinate] : nodeMapPtr) {
        NES_TRACE("adding node {} with coordinate {} ", nodeId, coordinate.toString())
        Point cartesianCoordinates(Point(coordinate.getX1(), coordinate.getX2()));
        neighbourWorkerSyntheticIndex.insert(std::make_pair(cartesianCoordinates, nodeId));
        neighborWorkerIdToCoordinateMap.insert(
            {nodeId, std::move(cartesianCoordinates)});
    }
    return true;
}


std::optional<NES::Synthetic::DataTypes::Experimental::NetworkCoordinate>
NES::Synthetic::Latency::Experimental::WorkerLatencyHandler::getNodeNetworkCoordinate(
    WorkerId nodeId,
    std::unordered_map<WorkerId, Point> neighbourWorkerIdToNetworkCoordinateMap) {
    if (neighbourWorkerIdToNetworkCoordinateMap.contains(nodeId)) {
        return NES::Synthetic::DataTypes::Experimental::NetworkCoordinate(
                neighbourWorkerIdToNetworkCoordinateMap.at(nodeId).get<0>(),
                neighbourWorkerIdToNetworkCoordinateMap.at(nodeId).get<1>());
    }
    return std::nullopt;
}

bool NES::Synthetic::Latency::Experimental::WorkerLatencyHandler::shouldSendCurrentWaypointToCoordinator(
    const std::optional<Point>& lastTransmittedCoordinate,
    const DataTypes::Experimental::NetworkCoordinate& currentCoordinate) {
    auto currentPoint = Point(currentCoordinate.getX1(), currentCoordinate.getX2());
    return !lastTransmittedCoordinate.has_value()
        || boost::geometry::comparable_distance(currentPoint, lastTransmittedCoordinate.value()) > networkCoordinateUpdateThreshold;
}


void NES::Synthetic::Latency::Experimental::WorkerLatencyHandler::sendCurrentWaypoint(
    const DataTypes::Experimental::Waypoint& currentWaypoint) {
    coordinatorRpcClient->sendCoordinateUpdate(currentWaypoint);
}

void NES::Synthetic::Latency::Experimental::WorkerLatencyHandler::start(const std::vector<WorkerId>& currentParentWorkerIds) {
    //start periodically pulling coordinate updates and inform coordinator about coordinate changes
    if (!isRunning) {
        NES_DEBUG("Starting scheduling and network coordinate update thread")
        isRunning = true;
        workerLatencyHandlerThread = std::make_shared<std::thread>(&WorkerLatencyHandler::run, this, currentParentWorkerIds);
    } else {
        NES_WARNING("Scheduling and network coordinate update thread already running")
    }
}

void NES::Synthetic::Latency::Experimental::WorkerLatencyHandler::run(const std::vector<WorkerId>& currentParentWorkerIds) {

    std::vector<Point> currentParentCoordinates;
    std::optional<Point> lastTransmittedCoordinate = std::nullopt;
    std::unordered_map<WorkerId, Point> neighborWorkerIdToCoordinateMap;
    NetworkCoordinateIndex neighborWorkerSyntheticIndex;
    std::optional<NES::Synthetic::DataTypes::Experimental::NetworkCoordinate> currentParentCoordinate = std::nullopt;

    //FIXME: currently only one parent per worker is supported. We therefore only ever access the parent id vectors front
    auto currentParentId = currentParentWorkerIds.front();

    while (isRunning) {
        //get current device waypoint
        auto currentWaypoint = networkCoordinateProvider->getCurrentWaypoint();
        auto currentCoordinate = currentWaypoint.getCoordinate();

        //if device has not moved more than threshold, do nothing
        if (!shouldSendCurrentWaypointToCoordinator(lastTransmittedCoordinate, currentCoordinate)) {
            NES_DEBUG("device has not moved further than threshold, coordinate will not be transmitted");
            std::this_thread::sleep_for(std::chrono::milliseconds(updateInterval));
            continue;
        }

        //send coordinate update
        NES_DEBUG("device has moved further then threshold, sending the coordinate")
        sendCurrentWaypoint(currentWaypoint);
        lastTransmittedCoordinate = Point(currentWaypoint.getCoordinate().getX1(), currentWaypoint.getCoordinate().getX1());


        //sleep for the configured interval
        std::this_thread::sleep_for(std::chrono::milliseconds(updateInterval));
    }

}

//todo #3365: check if we need insert poison pill: request processor service for reference
bool NES::Synthetic::Latency::Experimental::WorkerLatencyHandler::stop() {
    if (!isRunning) {
        return false;
    }
    isRunning = false;
    if (workerLatencyHandlerThread->joinable()) {
        workerLatencyHandlerThread->join();
        return true;
    }
    return false;
}

NES::Synthetic::DataTypes::Experimental::NodeIdToNetworkCoordinateMap
NES::Synthetic::Latency::Experimental::WorkerLatencyHandler::getNodeIdsInRange(
    const DataTypes::Experimental::NetworkCoordinate& coordinate,
    double radius) {
    if (!coordinatorRpcClient) {
        NES_WARNING("worker has no coordinator rpc client, cannot download node index");
        return {};
    }
    auto nodeVector = coordinatorRpcClient->getNodeIdsInNetworkRange(coordinate, radius);
    return DataTypes::Experimental::NodeIdToNetworkCoordinateMap{nodeVector.begin(), nodeVector.end()};
}
}// namespace NES