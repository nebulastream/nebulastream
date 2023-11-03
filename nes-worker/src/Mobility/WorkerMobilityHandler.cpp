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

#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Mobility/LocationProviders/LocationProvider.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedule.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
#include <Mobility/WorkerMobilityHandler.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Mobility/ReconnectPoint.hpp>
#include <Util/Mobility/S2Utilities.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <utility>

#ifdef S2DEF
#include <s2/s1angle.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2point.h>
#endif

namespace NES {
NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::WorkerMobilityHandler(
    const LocationProviderPtr& locationProvider,
    CoordinatorRPCClientPtr coordinatorRpcClient,
    Runtime::NodeEnginePtr nodeEngine,
    const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration)
    : updateInterval(mobilityConfiguration->mobilityHandlerUpdateInterval),
      nodeInfoDownloadRadius(mobilityConfiguration->nodeInfoDownloadRadius), isRunning(false), nodeEngine(std::move(nodeEngine)),
      locationProvider(locationProvider), coordinatorRpcClient(std::move(coordinatorRpcClient)) {
#ifdef S2DEF
    locationUpdateThreshold = S2Earth::MetersToAngle(mobilityConfiguration->sendDevicePositionUpdateThreshold);
    coveredRadiusWithoutThreshold =
        S2Earth::MetersToAngle(nodeInfoDownloadRadius - mobilityConfiguration->nodeIndexUpdateThreshold.getValue());
    defaultCoverageRadiusAngle = S2Earth::MetersToAngle(mobilityConfiguration->defaultCoverageRadius.getValue());
    reconnectSchedulePredictor = std::make_shared<ReconnectSchedulePredictor>(mobilityConfiguration);
#endif
}

#ifdef S2DEF
bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::updateNeighbourWorkerInformation(
    const DataTypes::Experimental::GeoLocation& currentLocation,
    std::unordered_map<uint64_t, S2Point>& neighbourWorkerIdToLocationMap,
    S2PointIndex<uint64_t>& neighbourWorkerSpatialIndex) {
    if (!currentLocation.isValid()) {
        NES_WARNING("invalid location, cannot download field nodes");
        return false;
    }
    NES_DEBUG("Downloading nodes in range")
    //get current position and download node information from coordinator
    //divide the download radius by 1000 to convert meters to kilometers
    auto nodeMapPtr = getNodeIdsInRange(currentLocation, nodeInfoDownloadRadius / 1000);

    //if we actually received nodes in our vicinity, we can clear the old nodes
    if (!nodeMapPtr.empty()) {
        neighbourWorkerIdToLocationMap.clear();
        neighbourWorkerSpatialIndex.Clear();
    } else {
        return false;
    }

    //insert node info into spatial index and map on node ids
    for (auto [nodeId, location] : nodeMapPtr) {
        NES_TRACE("adding node {} with location {} ", nodeId, location.toString())
        neighbourWorkerSpatialIndex.Add(S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude())), nodeId);
        neighbourWorkerIdToLocationMap.insert(
            {nodeId, S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude()))});
    }
    return true;
}

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::shouldUpdateNeighbouringWorkerInformation(
    const std::optional<S2Point>& centroidOfNeighbouringWorkerSpatialIndex,
    const DataTypes::Experimental::Waypoint& currentWaypoint) {

    auto currentLocation = currentWaypoint.getLocation();
    S2Point currentS2Point(S2LatLng::FromDegrees(currentLocation.getLatitude(), currentLocation.getLongitude()));
    /*check if we have moved close enough to the edge of the area covered by the current node index so the we need to
    download new node information */
    return !centroidOfNeighbouringWorkerSpatialIndex
        || S1Angle(currentS2Point, centroidOfNeighbouringWorkerSpatialIndex.value()) > coveredRadiusWithoutThreshold;
}
#endif

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::sendNextPredictedReconnect(
    const std::optional<NES::Spatial::Mobility::Experimental::ReconnectSchedule>& scheduledReconnects,
    const std::optional<NES::Spatial::Mobility::Experimental::ReconnectSchedule>& removedReconnects) {
    NES_DEBUG("WorkerMobilityHandler: sending predicted reconnect points")
    std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint> addList;
    std::vector<NES::Spatial::Mobility::Experimental::ReconnectPoint> removeList;
    if (scheduledReconnects) {
        addList = scheduledReconnects->getReconnectVector();
    }
    if (removedReconnects) {
        removeList = removedReconnects->getReconnectVector();
    }
    return coordinatorRpcClient->sendReconnectPrediction(addList, removeList);
}

#ifdef S2DEF
std::optional<NES::Spatial::Mobility::Experimental::ReconnectPoint>
NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::getNextReconnectPoint(
    std::optional<ReconnectSchedule>& reconnectSchedule,
    const DataTypes::Experimental::GeoLocation& currentOwnLocation,
    const std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation>& currentParentLocation,
    const S2PointIndex<uint64_t>& neighbourWorkerSpatialIndex) {

    //if the current parent location is not known, try reconnecting to the closest node
    if (!currentParentLocation.has_value()) {
        auto closesNodeId = getClosestNodeId(currentOwnLocation, defaultCoverageRadiusAngle, neighbourWorkerSpatialIndex);
        if (closesNodeId) {
            return NES::Spatial::Mobility::Experimental::ReconnectPoint{{}, closesNodeId.value(), 0};
        } else {
            return std::nullopt;
        }
    }

    auto currentParentPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentParentLocation.value());
    auto currentOwnPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentOwnLocation);
    S1Angle currentDistFromParent(currentOwnPoint, currentParentPoint);

    //get the next scheduled reconnect from the reconnect schedule if there is one
    std::optional<NES::Spatial::Mobility::Experimental::ReconnectPoint> nextScheduledReconnect;
    if (reconnectSchedule && !reconnectSchedule->getReconnectVector().empty()) {
        nextScheduledReconnect = reconnectSchedule->getReconnectVector().at(0);
    } else {
        nextScheduledReconnect = std::nullopt;
    }

    //check if we left the coverage of our current parent
    if (currentDistFromParent >= defaultCoverageRadiusAngle) {
        //if there is no scheduled reconnect, connect to the closest node we can find
        if (!nextScheduledReconnect) {
            auto closesNodeId = getClosestNodeId(currentOwnLocation, defaultCoverageRadiusAngle, neighbourWorkerSpatialIndex);
            if (closesNodeId) {
                return NES::Spatial::Mobility::Experimental::ReconnectPoint{{}, closesNodeId.value(), 0};
            } else {
                return std::nullopt;
            }
        } else if (S1Angle(currentOwnPoint,
                           NES::Spatial::Util::S2Utilities::geoLocationToS2Point(nextScheduledReconnect->pointGeoLocation))
                   <= currentDistFromParent) {
            //if the next scheduled parent is closer than the current parent, reconnect to the current parent
            reconnectSchedule.value().removeNextReconnect();
            NES_DEBUG("popped reconnect from schedule, remaining schedule size {}",
                      reconnectSchedule.value().getReconnectVector().size());
            return nextScheduledReconnect.value();
        }
    }
    return std::nullopt;
}

std::optional<uint64_t> NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::getClosestNodeId(
    const DataTypes::Experimental::GeoLocation& currentOwnLocation,
    S1Angle radius,
    const S2PointIndex<uint64_t>& neighbourWorkerSpatialIndex) {
    auto currentOwnPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentOwnLocation);
    S2ClosestPointQuery<uint64_t> query(&neighbourWorkerSpatialIndex);
    query.mutable_options()->set_max_distance(radius);
    S2ClosestPointQuery<int>::PointTarget target(currentOwnPoint);
    auto closestNode = query.FindClosestPoint(&target);
    if (closestNode.is_empty()) {
        return std::nullopt;
    }
    return closestNode.data();
}

std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation>
NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::getNodeGeoLocation(
    uint64_t nodeId,
    std::unordered_map<uint64_t, S2Point> neighbourWorkerIdToLocationMap) {
    if (neighbourWorkerIdToLocationMap.contains(nodeId)) {
        return NES::Spatial::Util::S2Utilities::s2pointToLocation(neighbourWorkerIdToLocationMap.at(nodeId));
    }
    return std::nullopt;
}
#endif

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::triggerReconnectionRoutine(uint64_t& currentParentId,
                                                                                             uint64_t newParentId) {
    //todo #4283: trigger reconnecting of sinks

    bool success = coordinatorRpcClient->replaceParent(currentParentId, newParentId);
    if (success) {
        //update locally saved information about parent
        currentParentId = newParentId;
    } else {
        NES_WARNING("WorkerMobilityHandler::replaceParent() failed to replace oldParent={} with newParentId={}.",
                    currentParentId,
                    newParentId);
        //todo: #3572 query coordinator for actual parent to recover from faulty state
    }

    NES_DEBUG("NesWorker::replaceParent() success={}.", success);

    return success;
}

#ifdef S2DEF
bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::shouldSendCurrentWaypointToCoordinator(
    const std::optional<S2Point>& lastTransmittedLocation,
    const DataTypes::Experimental::GeoLocation& currentLocation) {
    auto currentPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentLocation);
    return !lastTransmittedLocation.has_value()
        || S1Angle(currentPoint, lastTransmittedLocation.value()) > locationUpdateThreshold;
}
#endif

void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::sendCurrentWaypoint(
    const DataTypes::Experimental::Waypoint& currentWaypoint) {
    coordinatorRpcClient->sendLocationUpdate(currentWaypoint);
}

void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::start(std::vector<uint64_t> currentParentWorkerIds) {
    //TODO  #3365: reset state of schedule predictor
    //start periodically pulling location updates and inform coordinator about location changes
    if (!isRunning) {
        NES_DEBUG("Starting scheduling and location update thread")
        isRunning = true;
        workerMobilityHandlerThread = std::make_shared<std::thread>(&WorkerMobilityHandler::run, this, currentParentWorkerIds);
    } else {
        NES_WARNING("Scheduling and location update thread already running")
    }
}

void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::run(std::vector<uint64_t> currentParentWorkerIds) {

#ifdef S2DEF
    /**
     * Do
     *      sendLocation()
     *      updateIndex() -> where are my neighbors
     *      scheduleNextReconnectPoint()
     *      performReconnect()
     *      sendNextReconnectPoint()
     * While isRunning
     */
    std::vector<S2Point> currentParentLocations;
    std::optional<S2Point> lastTransmittedLocation = std::nullopt;
    std::unordered_map<uint64_t, S2Point> neighbourWorkerIdToLocationMap;
    S2PointIndex<uint64_t> neighbourWorkerSpatialIndex;
    std::optional<S2Point> centroidOfNeighbouringWorkerSpatialIndex;
    std::optional<ReconnectSchedule> lastPrediction = NES::Spatial::Mobility::Experimental::ReconnectSchedule::Empty();
    std::optional<NES::Spatial::DataTypes::Experimental::GeoLocation> currentParentLocation = std::nullopt;

    std::optional<ReconnectSchedule> currentReconnectSchedule;

    //FIXME: currently only one parent per worker is supported. We therefore only ever access the parent id vectors front
    auto currentParentId = currentParentWorkerIds.front();

    while (isRunning) {
        //get current device waypoint
        auto currentWaypoint = locationProvider->getCurrentWaypoint();
        auto currentLocation = currentWaypoint.getLocation();

        //if device has not moved more than threshold, do nothing
        if (!shouldSendCurrentWaypointToCoordinator(lastTransmittedLocation, currentLocation)) {
            NES_DEBUG("device has not moved further than threshold, location will not be transmitted");
            std::this_thread::sleep_for(std::chrono::milliseconds(updateInterval));
            continue;
        }

        //send location update
        NES_DEBUG("device has moved further then threshold, sending location")
        sendCurrentWaypoint(currentWaypoint);
        lastTransmittedLocation = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentWaypoint.getLocation());

        //update the neighbouring worker index if necessary
        bool indexUpdated = false;
        if (shouldUpdateNeighbouringWorkerInformation(centroidOfNeighbouringWorkerSpatialIndex, currentWaypoint)) {
            indexUpdated = updateNeighbourWorkerInformation(currentWaypoint.getLocation(),
                                                            neighbourWorkerIdToLocationMap,
                                                            neighbourWorkerSpatialIndex);
            if (indexUpdated) {
                centroidOfNeighbouringWorkerSpatialIndex = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentLocation);
                NES_TRACE("setting last index update position to {}", currentLocation.toString())
                currentParentLocation = getNodeGeoLocation(currentParentId, neighbourWorkerIdToLocationMap);
            } else {
                NES_ERROR("could not download node index")
            }
            //todo: make sure failure does not crash worker (test!)
        }

        bool reconnectScheduleWasUpdated = false;
        std::optional<ReconnectSchedule> newReconnectSchedule;
        if (currentParentLocation) {
            //calculate the reconnect schedule
            newReconnectSchedule = reconnectSchedulePredictor->getReconnectSchedule(currentWaypoint,
                                                                                    currentParentLocation.value(),
                                                                                    neighbourWorkerSpatialIndex,
                                                                                    indexUpdated);

            reconnectScheduleWasUpdated = newReconnectSchedule.has_value();
        } else if (currentReconnectSchedule.has_value()) {
            //if there is no known location for the current parent, the old reconnect schedule cannot be valid
            newReconnectSchedule = std::nullopt;
            reconnectScheduleWasUpdated = true;
        }

        //remember the last prediction we made so the coordinator can be told to undo it and record the new prediction
        if (reconnectScheduleWasUpdated) {
            lastPrediction = currentReconnectSchedule;
            currentReconnectSchedule = newReconnectSchedule;
        }

        //get the reconnect if it is to be performed now
        auto nextReconnectPoint =
            getNextReconnectPoint(currentReconnectSchedule, currentLocation, currentParentLocation, neighbourWorkerSpatialIndex);

        //perform reconnect if needed
        if (nextReconnectPoint.has_value()) {
            triggerReconnectionRoutine(currentParentId, nextReconnectPoint.value().newParentId);
            currentParentLocation = getNodeGeoLocation(currentParentId, neighbourWorkerIdToLocationMap);
        }

        //if the schedule changed, the coordinator has to be informed about the next predicted reconnect
        if (reconnectScheduleWasUpdated) {
            sendNextPredictedReconnect(currentReconnectSchedule, lastPrediction);
        }

        //sleep for the configured interval
        std::this_thread::sleep_for(std::chrono::milliseconds(updateInterval));
    }
#else
    (void) currentParentWorkerIds;
    NES_ERROR("s2 library is needed to handle worker mobility handler");
#endif
}

//todo #3365: check if we need insert poison pill: request processor service for reference
bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::stop() {
    if (!isRunning) {
        return false;
    }
    isRunning = false;
    if (workerMobilityHandlerThread->joinable()) {
        workerMobilityHandlerThread->join();
        return true;
    }
    return false;
}

NES::Spatial::DataTypes::Experimental::NodeIdToGeoLocationMap
NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::getNodeIdsInRange(
    const DataTypes::Experimental::GeoLocation& location,
    double radius) {
    if (!coordinatorRpcClient) {
        NES_WARNING("worker has no coordinator rpc client, cannot download node index");
        return {};
    }
    auto nodeVector = coordinatorRpcClient->getNodeIdsInRange(location, radius);
    return DataTypes::Experimental::NodeIdToGeoLocationMap{nodeVector.begin(), nodeVector.end()};
}
}// namespace NES