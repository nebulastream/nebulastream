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
#include <Spatial/Mobility/LocationProviders/LocationProviderCSV.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Spatial/Mobility/ReconnectSchedule.hpp>
#include <Spatial/Mobility/ReconnectSchedulePredictor.hpp>
#include <Spatial/Mobility/WorkerMobilityHandler.hpp>
#include <utility>
#ifdef S2DEF
#include <Util/Experimental/S2Utilities.hpp>
#include <s2/s1angle.h>
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2point.h>
#endif

namespace NES {
NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::WorkerMobilityHandler(
    uint64_t parentId,
    LocationProviderPtr locationProvider,
    CoordinatorRPCClientPtr coordinatorRpcClient,
    Runtime::NodeEnginePtr nodeEngine,
    const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& mobilityConfiguration)
    : coordinatorRpcClient(std::move(coordinatorRpcClient)),  locationProvider(locationProvider), parentId(parentId), nodeEngine(nodeEngine)  {
    locationUpdateInterval = mobilityConfiguration->sendLocationUpdateInterval;
    pathPredictionUpdateInterval = mobilityConfiguration->pathPredictionUpdateInterval.getValue();

    sendUpdates = false;
    coveredRadiusWithoutThreshold =
        S2Earth::MetersToAngle(nodeInfoDownloadRadius - mobilityConfiguration->nodeIndexUpdateThreshold.getValue());
    defaultCoverageRadiusAngle = S2Earth::MetersToAngle(mobilityConfiguration->defaultCoverageRadius.getValue());
#ifdef S2DEF
    reconnectSchedulePredictor = std::make_shared<ReconnectSchedulePredictor>(mobilityConfiguration);

    //get the current position and download the locations of the field nodes in the vicinity
    auto currentPosition = locationProvider->getWaypoint();
    //downloadFieldNodes(currentPosition.getLocation());
    updateDownloadedNodeIndex(currentPosition.getLocation());

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
        reconnectToClosestNode(locationProvider->getCurrentWaypoint().getLocation(), defaultCoverageRadiusAngle);
        NES_DEBUG("set parent to " << parentId);
    }


    locationUpdateThreshold = S2Earth::MetersToAngle(mobilityConfiguration->sendDevicePositionUpdateThreshold);
    if (mobilityConfiguration->pushDeviceLocationUpdates) {
        sendUpdates = true;
        sendLocationUpdateThread = std::make_shared<std::thread>(&WorkerMobilityHandler::startLocationUpdatingAndReconnectScheduling, this);
    }
#endif
}

void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::setPositionOfLastIndexUpdate(const DataTypes::Experimental::GeoLocation& currentLocation) {
    //save the position of the update so we can check how far we have moved from there later on
    positionOfLastNodeIndexUpdate = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentLocation);
    NES_TRACE("setting last index update position to " << currentLocation.toString())
}

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::downloadFieldNodes(const DataTypes::Experimental::GeoLocation& currentLocation) {
#ifdef S2DEF
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
    return true;
#else
    (void) currentLocation;
    NES_WARNING("s2 library is needed to download field node information")
    return false;
#endif
}

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::reconnectToClosestNode(const DataTypes::Experimental::GeoLocation& ownLocation, S1Angle maxDistance) {
#ifdef S2DEF
    if (fieldNodeIndex.num_points() == 0) {
        return false;
    }
    S2ClosestPointQuery<uint64_t> query(&fieldNodeIndex);
    query.mutable_options()->set_max_distance(maxDistance);
    S2ClosestPointQuery<int>::PointTarget target(NES::Spatial::Util::S2Utilities::geoLocationToS2Point(ownLocation));
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

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::checkNodeIndexThreshold(const DataTypes::Experimental::GeoLocation& currentLocation) {
    S2Point currentS2Point(S2LatLng::FromDegrees(currentLocation.getLatitude(), currentLocation.getLongitude()));
    /*check if we have moved close enough to the edge of the area covered by the current node index so the we need to
    download new node information */
    return !positionOfLastNodeIndexUpdate || S1Angle(currentS2Point, positionOfLastNodeIndexUpdate.value()) > coveredRadiusWithoutThreshold;
}

void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::updateDownloadedNodeIndex(const DataTypes::Experimental::GeoLocation& currentLocation) {
#ifdef S2DEF
    S2Point currentS2Point(S2LatLng::FromDegrees(currentLocation.getLatitude(), currentLocation.getLongitude()));
    //if new nodes were downloaded, make sure that the current parent becomes part of the index by adding it if it was not downloaded anyway
    bool downloadSuccesfull = downloadFieldNodes(currentLocation);
    if (downloadSuccesfull) {
        setPositionOfLastIndexUpdate(currentLocation);
        if (currentParentLocation && fieldNodeMap.count(parentId) == 0) {
            NES_DEBUG("current parent was not present in downloaded list, adding it to the index")
            fieldNodeMap.insert({parentId, currentParentLocation.value()});
            fieldNodeIndex.Add(currentParentLocation.value(), parentId);
        }
    }
#else
    (void) currentLocation;
    NES_WARNING("s2 library is needed to update downloaded node index")
#endif
}

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::sendScheduledReconnect(
    const std::optional<NES::Spatial::Mobility::Experimental::ReconnectPrediction>& scheduledReconnect) {
    if (scheduledReconnect.has_value()) {
        return coordinatorRpcClient->sendReconnectPrediction(scheduledReconnect.value());
    } else {
        NES_DEBUG("no reconnect point found after recalculation, sending empty reconnect")
        return coordinatorRpcClient->sendReconnectPrediction(NES::Spatial::Mobility::Experimental::ReconnectPrediction{0, 0});
    }
}

std::optional<uint64_t> NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::getReconnectParent(const DataTypes::Experimental::GeoLocation &currentOwnLocation, const DataTypes::Experimental::GeoLocation &currentParentLocation) {
    if (!reconnectSchedule) {
        return std::nullopt;
    }
    auto currentOwnPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentOwnLocation);
    auto currentParentPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentParentLocation);
    S1Angle currentDistFromParent(currentOwnPoint, currentParentPoint);


    std::optional<NES::Spatial::Mobility::Experimental::ReconnectPoint> nextScheduledReconnect;
    auto reconnectVector = reconnectSchedule->getReconnectVector();
    if (!reconnectVector->empty()) {
        nextScheduledReconnect = reconnectVector->at(0);
    } else {
        nextScheduledReconnect = std::nullopt;
    }
    //check if we left the coverage of our current parent
    if (currentDistFromParent >= defaultCoverageRadiusAngle) {
        //if there is not reconnect scheduled connect to the closest node we can find
        if (!nextScheduledReconnect) {
            S2ClosestPointQuery<uint64_t> query(&fieldNodeIndex);
            query.mutable_options()->set_max_distance(defaultCoverageRadiusAngle);
            S2ClosestPointQuery<int>::PointTarget target(currentOwnPoint);
            auto closestNode = query.FindClosestPoint(&target);
            if (closestNode.is_empty()) {
                return std::nullopt;
            }
            return closestNode.data();
        } else if (S1Angle(currentOwnPoint, NES::Spatial::Util::S2Utilities::geoLocationToS2Point(nextScheduledReconnect->predictedReconnectLocation)) <= currentDistFromParent) {
            //if the next scheduled parent is closer than the current parent, reconnect to the scheduled parent
            //todo: keep this here or after reconnect?
            reconnectVector->erase(reconnectVector->begin());
            return nextScheduledReconnect.value().reconnectPrediction.newParentId;
        }
    }
    return std::nullopt;
}

bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::reconnect(uint64_t oldParent, uint64_t newParent) {
    nodeEngine->bufferAllData();
    //todo #3027: wait until all upstream operators have received data which has not been buffered
    //todo #3027: trigger replacement and migration of operators

    bool success = coordinatorRpcClient->replaceParent(oldParent, newParent);
    if (!success) {
        NES_WARNING("WorkerMobilityHandler::replaceParent() failed to replace oldParent=" << oldParent
                                                                              << " with newParentId=" << newParent);
    }
    NES_DEBUG("NesWorker::replaceParent() success=" << success);

    nodeEngine->stopBufferingAllData();

    //update locally saved information about parent
    parentId = newParent;
#ifdef S2DEF
    currentParentLocation = fieldNodeMap.at(newParent);
#endif
    return success;
}

#ifdef S2DEF
bool NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::checkPositionThreshold(const DataTypes::Experimental::GeoLocation &currentLocation) {
    auto currentPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentLocation);
    std::unique_lock lock(reconnectConfigMutex);
    return S1Angle(currentPoint, lastTransmittedLocation) > locationUpdateThreshold;
}

void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::sendLocationUpdate(const DataTypes::Experimental::Waypoint &currentWaypoint) {
    auto currentPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentWaypoint.getLocation());
    std::unique_lock lock(reconnectConfigMutex);
    //check if we moved further than the threshold. if so, tell the coordinator about the devices new position
    coordinatorRpcClient->sendLocationUpdate(std::move(currentWaypoint));
    lastTransmittedLocation = currentPoint;
}

void NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::startLocationUpdatingAndReconnectScheduling() {
    //get the devices current location
    auto currentWaypoint = locationProvider->getCurrentWaypoint();
    auto currentPoint = NES::Spatial::Util::S2Utilities::geoLocationToS2Point(currentWaypoint.getLocation());

    //initialize sending of location updates
    NES_DEBUG("transmitting initial location")
    coordinatorRpcClient->sendLocationUpdate(std::move(currentWaypoint));
    std::unique_lock lock(reconnectConfigMutex);
    lastTransmittedLocation = currentPoint;
    lock.unlock();

    //initialize reconnect scheduling
    updateDownloadedNodeIndex(locationProvider->getCurrentWaypoint().getLocation());

    //start periodically pulling location updates and inform coordinator about location changes
    while (sendUpdates) {
        currentWaypoint = locationProvider->getCurrentWaypoint();
        if (!checkPositionThreshold(currentWaypoint.getLocation())) {
            //std::this_thread::sleep_for(std::chrono::milliseconds(locationUpdateInterval));
            std::this_thread::sleep_for(std::chrono::milliseconds(pathPredictionUpdateInterval));
            NES_DEBUG("device has not moved further than threshold, location will not be transmitted");
            continue;
        }
        NES_DEBUG("device has moved further then threshold, sending location")
        sendLocationUpdate(currentWaypoint);

        //update the spatial index if necessary
        bool indexUpdated = fieldNodeMap.empty() || checkNodeIndexThreshold(currentWaypoint.getLocation());
        if (indexUpdated) {
            updateDownloadedNodeIndex(currentWaypoint.getLocation());
        }

        //if the worker is not connected to a parent with a known location, continue
        if (!currentParentLocation) {
            reconnectToClosestNode(currentWaypoint.getLocation(), defaultCoverageRadiusAngle);
            //std::this_thread::sleep_for(std::chrono::milliseconds(locationUpdateInterval));
            std::this_thread::sleep_for(std::chrono::milliseconds(pathPredictionUpdateInterval));
            continue;
        }

        //todo: would it make sense to pass s2points here?
        //recalculate scheduling
        auto newReconnectSchedule = reconnectSchedulePredictor->getReconnectSchedule(currentWaypoint,
                                                                                  NES::Spatial::Util::S2Utilities::s2pointToLocation(currentParentLocation.value()),
                                                                                  fieldNodeIndex,
                                                                                  indexUpdated);

        //if the reconnect schedule did not change, there is not more to do in this iteration
        bool reconnectScheduleWasUpdated = (bool) newReconnectSchedule;
        if (newReconnectSchedule) {
            reconnectSchedule = std::move(newReconnectSchedule);
        }

        //check if a reconnect is scheduled
        auto reconnectToPerform =
            getReconnectParent(currentWaypoint.getLocation(),
                               NES::Spatial::Util::S2Utilities::s2pointToLocation(currentParentLocation.value()));
        if (reconnectToPerform) {
            reconnect(parentId, reconnectToPerform.value());
        }

        //if the schedule changed or the worker reconnected, the coordinator has to be informed about the changed schedule
        if (reconnectScheduleWasUpdated || reconnectToPerform) {
            auto reconnectVec = reconnectSchedule->getReconnectVector();
            if (!reconnectVec->empty()) {
                sendScheduledReconnect(reconnectVec->front().reconnectPrediction);
            } else {
                sendScheduledReconnect(std::nullopt);
            }
        }

        //todo: what to do if this thread dies?
        //std::this_thread::sleep_for(std::chrono::milliseconds(locationUpdateInterval));
        std::this_thread::sleep_for(std::chrono::milliseconds(pathPredictionUpdateInterval));
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

NES::Spatial::DataTypes::Experimental::NodeIdToGeoLocationMap
NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::getNodeIdsInRange(const DataTypes::Experimental::GeoLocation& location, double radius) {
    if (!coordinatorRpcClient) {
        NES_WARNING("worker has no coordinator rpc client, cannot download node index");
        return {};
    }
    auto nodeVector = coordinatorRpcClient->getNodeIdsInRange(location, radius);
    return DataTypes::Experimental::NodeIdToGeoLocationMap{nodeVector.begin(), nodeVector.end()};
}

NES::Spatial::DataTypes::Experimental::NodeIdToGeoLocationMap
NES::Spatial::Mobility::Experimental::WorkerMobilityHandler::getNodeIdsInRange(double radius) {
    auto location = locationProvider->getWaypoint().getLocation();
    if (location.isValid()) {
        return getNodeIdsInRange(std::move(location), radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}
}// namespace NES