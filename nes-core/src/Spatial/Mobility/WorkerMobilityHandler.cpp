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
#include <Runtime/NodeEngine.hpp>
#include <Spatial/DataTypes/GeoLocation.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Mobility/LocationProviderCSV.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Spatial/Mobility/WorkerMobilityHandler.hpp>
#include <utility>
#include <Spatial/Mobility/ReconnectSchedulePredictor.hpp>
#include <Spatial/Mobility/ReconnectSchedule.hpp>
#ifdef S2DEF
#include <Util/Experimental/S2Utilities.hpp>
#include <s2/s1angle.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2point.h>
#endif

namespace NES {
NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::WorkerMobilityHandler(
    NesWorker& worker,
    CoordinatorRPCClientPtr coordinatorRpcClient,
    const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration)
    : worker(worker), coordinatorRpcClient(std::move(coordinatorRpcClient)) {
    locationUpdateInterval = mobilityConfiguration->sendLocationUpdateInterval;

    //todo: do we want to have seaparate intervals?
    //pathPredictionUpdateInterval = configuration->pathPredictionUpdateInterval.getValue();

    sendUpdates = false;
    coveredRadiusWithoutThreshold =
        S2Earth::MetersToAngle(nodeInfoDownloadRadius - mobilityConfiguration->nodeIndexUpdateThreshold.getValue());
#ifdef S2DEF
    reconnectSchedulePredictor = std::make_shared<ReconnectSchedulePredictor>(mobilityConfiguration);

    //get the current position and download the locations of the field nodes in the vicinity
    auto currentPosition = locationProvider->getWaypoint();
    downloadFieldNodes(currentPosition.getLocation());

    //find the current parent node among the downloaded field node data
    auto iterator = S2PointIndex<uint64_t>::Iterator(&fieldNodeIndex);
    iterator.Begin();
    while (!iterator.done()) {
        if (iterator.data() == parentId) {
            currentParentLocation = iterator.point();
            break;
        }
        iterator.Next();
    }

    //if this workers current parent could not be found among the data, reconnect planning is not possible
    if (iterator.done()) {
        NES_DEBUG("parent id does not match any field node in the coverage area. Changing parent now")
        reconnectToClosestNode(locationProvider->getCurrentWaypoint(), mobilityConfiguration->defaultCoverageRadius.getValue());
        NES_DEBUG("set parent to " << parentId);
    }


    locationUpdateThreshold = S2Earth::MetersToAngle(mobilityConfiguration->sendDevicePositionUpdateThreshold);
    if (mobilityConfiguration->pushDeviceLocationUpdates) {
        sendUpdates = true;
        sendLocationUpdateThread = std::make_shared<std::thread>(&WorkerMobilityHandler::periodicallySendLocationUpdates, this);
    }
#endif
};



bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::downloadFieldNodes(const DataTypes::Experimental::GeoLocation& currentLocation) {
#ifdef S2DEF
    if (!currentLocation.isValid()) {
        NES_WARNING("invalid location, cannot download field nodes");
        return false;
    }
    NES_DEBUG("Downloading nodes in range")
    //get current position and download node information from coordinator
    //divide the download radius by 1000 to convert meters to kilometers
    //todo: move these functions out of the location provider
    auto nodeMapPtr = locationProvider->getNodeIdsInRange(currentLocation, nodeInfoDownloadRadius / 1000);

    //if we actually received nodes in our vicinity, we can clear the old nodes
    if (!nodeMapPtr.empty()) {
        fieldNodeMap.clear();
        fieldNodeIndex.Clear();
    } else {
        return false;
    }

    //insert node info into spatial index and map on node ids
    for (auto [nodeId, location] : nodeMapPtr) {
        NES_TRACE("adding node " << nodeId << " with location " << location.toString())
        fieldNodeIndex.Add(S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude())), nodeId);
        fieldNodeMap.insert({nodeId, S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude()))});
    }

    //save the position of the update so we can check how far we have moved from there later on
    positionOfLastNodeIndexUpdate = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentLocation);
    NES_TRACE("setting last index update position to " << currentLocation.toString())
    return true;
#else
    (void) currentLocation;
    NES_WARNING("s2 library is needed to download field node information")
    return false;
#endif
}

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::reconnectToClosestNode(const DataTypes::Experimental::Waypoint& ownLocation, double maxDistance) {
#ifdef S2DEF
    if (fieldNodeIndex.num_points() == 0) {
        return false;
    }
    S2ClosestPointQuery<uint64_t> query(&fieldNodeIndex);
    query.mutable_options()->set_max_distance(S1Angle::Degrees(maxDistance));
    S2ClosestPointQuery<int>::PointTarget target(NES::Spatial::Util::S2Utilities::geoLocationToS2Point(ownLocation.getLocation()));
    auto closestNode = query.FindClosestPoint(&target);
    if (closestNode.is_empty()) {
        return false;
    }
    reconnect(parentId, closestNode.data());
    return true;
#else
    (void) ownLocation;
    return false;
#endif
}

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::updateDownloadedNodeIndex(const DataTypes::Experimental::GeoLocation& currentLocation) {
#ifdef S2DEF
    S2Point currentS2Point(S2LatLng::FromDegrees(currentLocation.getLatitude(), currentLocation.getLongitude()));

    /*check if we have moved close enough to the edge of the area covered by the current node index so the we need to
    download new node information */
    if (!positionOfLastNodeIndexUpdate
        || S1Angle(currentS2Point, positionOfLastNodeIndexUpdate.value()) > coveredRadiusWithoutThreshold) {
        //if new nodes were downloaded, make sure that the current parent becomes part of the index by adding it if it was not downloaded anyway
        if (downloadFieldNodes(currentLocation) && currentParentLocation && fieldNodeMap.count(parentId) == 0) {
            NES_DEBUG("current parent was not present in downloaded list, adding it to the index")
            fieldNodeMap.insert({parentId, currentParentLocation.value()});
            fieldNodeIndex.Add(currentParentLocation.value(), parentId);
        }
        return true;
    }
    return false;
#else
    (void) currentLocation;
    NES_WARNING("s2 library is needed to update downloaded node index")
    return false;
#endif
}

void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::HandleScheduledReconnect(ReconnectSchedulePtr reconnectSchedule) {

}

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::sendScheduledReconnect(
    const std::optional<NES::Spatial::Mobility::Experimental::ReconnectPrediction>& scheduledReconnect) {
    bool predictionChanged = false;
    if (scheduledReconnect.has_value()) {
        // the new value represents a valid prediction
        uint64_t reconnectId = scheduledReconnect.value().newParentId;
        Timestamp timestamp = scheduledReconnect.value().expectedTime;
        std::unique_lock lock(reconnectConfigMutex);
        if (!lastTransmittedReconnectPrediction.has_value()) {
            // previously there was no prediction. we now inform the coordinator that a prediction exists
            NES_DEBUG("transmitting predicted reconnect point. previous prediction did not exist")
            coordinatorRpcClient->sendReconnectPrediction(worker.getWorkerId(), scheduledReconnect.value());
            predictionChanged = true;
        } else if (reconnectId != lastTransmittedReconnectPrediction.value().newParentId
                   || timestamp != lastTransmittedReconnectPrediction.value().expectedTime) {
            // there was a previous prediction but its values differ from the current one. Inform coordinator about the new prediciton
            NES_DEBUG("transmitting predicted reconnect point. current prediction differs from previous prediction")
            coordinatorRpcClient->sendReconnectPrediction(worker.getWorkerId(), scheduledReconnect.value());
            lastTransmittedReconnectPrediction = scheduledReconnect;
            predictionChanged = true;
        }
    } else if (lastTransmittedReconnectPrediction.has_value()) {
        // a previous trajectory led to the calculation of a prediction. But there is no prediction (yet) for the current trajectory
        // inform coordinator, that the old prediction is not valid anymore
        NES_DEBUG("no reconnect point found after recalculation, telling coordinator to discard old reconnect")
        coordinatorRpcClient->sendReconnectPrediction(worker.getWorkerId(),
                                                      NES::Spatial::Mobility::Experimental::ReconnectPrediction{0, 0});
        predictionChanged = true;
    }
    lastTransmittedReconnectPrediction = scheduledReconnect;
    return predictionChanged;
}

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::reconnect(uint64_t oldParent, uint64_t newParent) {
    worker.getNodeEngine()->bufferAllData();
    //todo #3027: wait until all upstream operators have received data which has not been buffered
    //todo #3027: trigger replacement and migration of operators

    //todo: make grpc call directly here
    bool success = worker.replaceParent(oldParent, newParent);

    //todo: include do not use ref to worker
    worker.getNodeEngine()->stopBufferingAllData();
    return success;
}

#ifdef S2DEF
void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::checkThresholdAndSendLocationUpdate(const DataTypes::Experimental::Waypoint &currentWaypoint) {
    auto currentPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentWaypoint.getLocation());
    std::unique_lock lock(reconnectConfigMutex);
        //check if we moved further than the threshold. if so, tell the coordinator about the devices new position
        if (S1Angle(currentPoint, lastTransmittedLocation) > locationUpdateThreshold) {
            NES_DEBUG("device has moved further then threshold, sending location")
            coordinatorRpcClient->sendLocationUpdate(worker.getWorkerId(), std::move(currentWaypoint));
            lastTransmittedLocation = currentPoint;
        } else {
            NES_DEBUG("device has not moved further than threshold, location will not be transmitted")
        }
}

//todo: rename function bc it now not only sends but also does reconnectScheduling
void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::periodicallySendLocationUpdates() {
    //get the devices current location
    auto currentWaypoint = worker.getLocationProvider()->getCurrentWaypoint();
    auto currentPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentWaypoint.getLocation());

    //initialize sending of location updates
    NES_DEBUG("transmitting initial location")
    coordinatorRpcClient->sendLocationUpdate(worker.getWorkerId(), std::move(currentWaypoint));
    std::unique_lock lock(reconnectConfigMutex);
    lastTransmittedLocation = currentPoint;
    lock.unlock();

    //initialize reconnect scheduling
    updateDownloadedNodeIndex(locationProvider->getCurrentWaypoint().getLocation());

    //start periodically pulling location updates and inform coordinator about location changes
    while (sendUpdates) {
        currentWaypoint = worker.getLocationProvider()->getCurrentWaypoint();
        checkThresholdAndSendLocationUpdate(currentWaypoint);

        //update the spatial index if necessary
        bool indexUpdated = updateDownloadedNodeIndex(currentWaypoint.getLocation());
        //recalculate scheduling
        auto reconnectSchedule = reconnectSchedulePredictor->getReconnectSchedule(currentWaypoint.getLocation(),
                                                                                  currentParentLocation,
                                                                                  fieldNodeIndex,
                                                                                  fieldNodeMap,
                                                                                  indexUpdated);
        std::this_thread::sleep_for(std::chrono::milliseconds(locationUpdateInterval));
    }
}
#endif

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::stopPeriodicUpdating() {
    if (!sendUpdates) {
        return false;
    }
    sendUpdates = false;
    return true;
}

}// namespace NES