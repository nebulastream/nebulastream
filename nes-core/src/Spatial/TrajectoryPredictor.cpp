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

#include <Util/TimeMeasurement.hpp>
#include <Spatial/TrajectoryPredictor.hpp>
#include <Spatial/LocationProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <Common/Location.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Spatial/ReconnectSchedule.hpp>
#include <Spatial/ReconnectConfigurator.hpp>
#include <Util/Experimental/S2Conversion.hpp>
#include <stdexcept>

namespace NES::Spatial::Mobility::Experimental {

TrajectoryPredictor::TrajectoryPredictor(LocationProviderPtr locationProvider, Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr configuration, uint64_t parentId) : locationProvider(locationProvider) {
    pathPredictionUpdateInterval = configuration->pathPredictionUpdateInterval.getValue();
    locationBufferSize = configuration->locationBufferSize.getValue();
    locationBufferSaveRate = configuration->locationBufferSaveRate.getValue();
    pathDistanceDeltaAngle = S2Earth::MetersToAngle(configuration->pathDistanceDelta.getValue());
    nodeInfoDownloadRadius = configuration->nodeInfoDownloadRadius.getValue();
    defaultCoverageRadiusAngle = S2Earth::MetersToAngle(configuration->defaultCoverageRadius.getValue());
    predictedPathLengthAngle = S2Earth::MetersToAngle(configuration->pathPredictionLength);
    //todo:  we might want to use a smaller search radius and add this as a parameter
    reconnectSearchRadius = S2Earth::MetersToAngle(nodeInfoDownloadRadius);
    coveredRadiusWithoutThreshold = S2Earth::MetersToAngle(nodeInfoDownloadRadius - configuration->nodeIndexUpdateThreshold.getValue());
    this->parentId = parentId;
    updatePrediction = false;
    allowedSpeedDifferenceFactor = 0.00001;
}

Mobility::Experimental::ReconnectSchedulePtr TrajectoryPredictor::getReconnectSchedule() {
    std::shared_ptr<Index::Experimental::Location> start;
    std::shared_ptr<Index::Experimental::Location> end;

    //check if a path exists and insert invalid locations if it doesn't
    if (trajectoryLine) {
        std::unique_lock lineLock(trajectoryLineMutex);
        S2LatLng startLatLng(trajectoryLine->vertices_span()[0]);
        start = std::make_shared<Index::Experimental::Location>(startLatLng.lat().degrees(), startLatLng.lng().degrees());
        S2LatLng endLatLng(trajectoryLine->vertices_span()[1]);
        end = std::make_shared<Index::Experimental::Location>(endLatLng.lat().degrees(), endLatLng.lng().degrees());
        lineLock.unlock();
    } else {
       start = std::make_shared<Index::Experimental::Location>();
       end = std::make_shared<Index::Experimental::Location>();
    }
    auto reconnectVectorPtr = std::make_shared<std::vector<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>>>(reconnectVector);
    Index::Experimental::LocationPtr indexUpdatePointer;
    std::unique_lock indexUpdatePosLock(indexUpdatePositionMutex);
    if (positionOfLastNodeIndexUpdate) {
        indexUpdatePointer = std::make_shared<Index::Experimental::Location>(Index::Experimental::s2pointToLocation(positionOfLastNodeIndexUpdate.value()));
    } else {
        indexUpdatePointer = nullptr;
    }
    indexUpdatePosLock.unlock();
    return std::make_shared<Mobility::Experimental::ReconnectSchedule>(start, end, indexUpdatePointer,
        reconnectVectorPtr);
}

bool TrajectoryPredictor::downloadFieldNodes() {
    NES_DEBUG("Downloading nodes in range")
    //get current position and download node information from coordinator
    auto positionAtUpdate = locationProvider->getLocation();
    auto nodeList = locationProvider->getNodeIdsInRange(*positionAtUpdate, nodeInfoDownloadRadius / 1000);

    //if we actually received nodes in out vicinity, we can clear the old nodes
    std::unique_lock nodeIndexLock(nodeIndexMutex);
    if (!nodeList.empty()) {
        fieldNodeMap.clear();
        fieldNodeIndex.Clear();
    } else {
        return false;
    }

    //insert note info into spatial index and map on node ids
    for (auto [nodeId, location] : nodeList) {
        NES_TRACE("adding node " << nodeId << " with location " << location.toString())
        fieldNodeIndex.Add(S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude())), nodeId);
        fieldNodeMap.insert({nodeId, S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude()))});
    }
    nodeIndexLock.unlock();

    //save the position of the update so we can check how far we have moved from there later on
    std::unique_lock positionAtUpdateLock(indexUpdatePositionMutex);
    positionOfLastNodeIndexUpdate = locationToS2Point(*positionAtUpdate);
    NES_TRACE("setting last index update position to " << positionAtUpdate->toString())
    return true;
}

void TrajectoryPredictor::setUpReconnectPlanning(ReconnectConfiguratorPtr reconnectConfigurator) {
    if (updatePrediction) {
        NES_DEBUG("there is already a prediction thread running, cannot start another one")
        return;
    }
    updatePrediction = true;
    this->reconnectConfigurator = std::move(reconnectConfigurator);
    downloadFieldNodes();
    NES_DEBUG("Parent Id is: " << parentId)

    std::unique_lock nodeIndexLock(nodeIndexMutex);
    auto iterator = S2PointIndex<uint64_t>::Iterator(&fieldNodeIndex);
    iterator.Begin();
    while(!iterator.done()) {
        if (iterator.data() == parentId) {
            currentParentLocation = iterator.point();
            break;
        }
        iterator.Next();
    }
    if (iterator.done()) {
        //todo: test this
        NES_DEBUG("parent id does not match any field node in the coverage area. Cannot start reconnect planning")
        return;
    }
    nodeIndexLock.unlock();

    //start reconnect planner thread
    locationUpdateThread = std::make_shared<std::thread>(&TrajectoryPredictor::startReconnectPlanning, this);
}

void TrajectoryPredictor::startReconnectPlanning() {
    //fill up the buffer before starting to calculate path
    for (size_t i = 0; i < locationBufferSize; ++i) {
        auto currentLocation = locationProvider->getCurrentLocation();
        locationBuffer.push_back(currentLocation);
        NES_DEBUG("added: " << locationBuffer.back().first->toString() << locationBuffer.back().second);
        std::this_thread::sleep_for(std::chrono::milliseconds(pathPredictionUpdateInterval * locationBufferSaveRate));
        updateDownloadedNodeIndex(*currentLocation.first);
    }
    NES_TRACE("Location buffer is filled");

    NES_DEBUG("Saving a location to buffer each " << locationBufferSaveRate << " location updates");
    size_t stepsSinceLastLocationSave = locationBufferSaveRate;
    S2Point nextReconnectNodeLocation;
    auto oldestKnownOwnLocation = locationBuffer.front();

    while (updatePrediction) {
        auto currentOwnLocation = locationProvider->getCurrentLocation();

        //if locationBufferSaveRate updates have been done since last save: save current location to buffer
        if (stepsSinceLastLocationSave == locationBufferSaveRate) {
            oldestKnownOwnLocation = locationBuffer.front();
            locationBuffer.pop_front();
            locationBuffer.push_back(currentOwnLocation);
            //NES_DEBUG("added: " << currentOwnLocation.first->toString() << "; " << currentOwnLocation.second);
            //NES_DEBUG("removed: " << oldestKnownOwnLocation.first->toString() << "; " << oldestKnownOwnLocation.second);
            stepsSinceLastLocationSave = 0;
            //todo: does it make sense here to update since the last position in buffer or should we use the path beginning?
        } else {
            stepsSinceLastLocationSave += 1;
        }

        //check if we deviated more than delta from the old predicted path and update it if needed
        if (updateDownloadedNodeIndex(*currentOwnLocation.first) || updatePredictedPath(oldestKnownOwnLocation.first, currentOwnLocation.first) || updateAverageMovementSpeed()) {
            NES_INFO("predicted path was recalculated")

            //because the path might have changed, the reconnect sequence needs to be recalculated
            //todo: instead of updateing right away, look at if the new trajectory stabilizes itself after a turn
            scheduleReconnects();

            //check if there are scheduled reconnects
            std::unique_lock reconnectVectorLock(reconnectVectorMutex);
            if (!reconnectVector.empty()) {
                nextReconnectNodeLocation = fieldNodeMap.at(get<0>(reconnectVector[0]));

                //update reconnect information and inform coordinator if necessary
                reconnectConfigurator->update(getNextReconnect());
            } else {
                NES_INFO("rescheduled after reconnect but there is no next reconnect in list")
                //update reconnect information and inform coordinator if necessary
                reconnectConfigurator->update(std::nullopt);
            }
        }

        std::unique_lock reconnectVectorLock(reconnectVectorMutex);
        //if we entered the coverage of the next schedules reconnect node, then reconnect
        if (!reconnectVector.empty() &&
              S1Angle(locationToS2Point(*currentOwnLocation.first),currentParentLocation) >= S1Angle(defaultCoverageRadiusAngle)) {

            auto currentOwnPoint = locationToS2Point(*currentOwnLocation.first);
            std::optional<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>> nextReconnect = getNextReconnect();
            if (S1Angle(currentOwnPoint, nextReconnectNodeLocation) <= S1Angle(defaultCoverageRadiusAngle)) {
                //reconnect and inform coordinator about upcoming reconnect
                std::unique_lock lastReconnectLock(lastReconnectTimeMutex);
                reconnectConfigurator->reconnect(parentId, get<0>(reconnectVector.front()));
                devicePositionTupleAtLastReconnect = currentOwnLocation;
                reconnectConfigurator->update(nextReconnect);

                //update locally saved information about parent
                parentId = get<0>(reconnectVector.front());
                currentParentLocation = fieldNodeMap.at(get<0>(reconnectVector[0]));
                reconnectVector.erase(reconnectVector.begin());

                //after reconnect, check if there is a next point on the schedule
                if (!reconnectVector.empty()) {
                    auto coverageEndLoc = std::get<1>(reconnectVector.front());
                    nextReconnectNodeLocation = fieldNodeMap.at(get<0>(reconnectVector[0]));
                    NES_INFO("reconnect point: " << coverageEndLoc);
                } else {
                    NES_INFO("no next reconnect scheduled")
                }
                lastReconnectLock.unlock();
            }
        }
        reconnectVectorLock.unlock();
        //sleep for the specified amount of time
        std::this_thread::sleep_for(std::chrono::milliseconds(pathPredictionUpdateInterval));
    }
}

bool TrajectoryPredictor::updateAverageMovementSpeed() {
    Timestamp bufferTravelTime = locationBuffer.back().second - locationBuffer.front().second;
    S1Angle bufferDistance(locationToS2Point(*locationBuffer.front().first), locationToS2Point(*locationBuffer.back().first));
    double meanDegreesPerNanosec = bufferDistance.degrees() / bufferTravelTime;
    if (abs(meanDegreesPerNanosec - bufferAverageMovementSpeed) > bufferAverageMovementSpeed * allowedSpeedDifferenceFactor) {
       bufferAverageMovementSpeed = meanDegreesPerNanosec;
       NES_DEBUG("average movement speed was updated to " << bufferAverageMovementSpeed)
       //NES_DEBUG("meters: " << )
       NES_DEBUG("threshhold is " << bufferAverageMovementSpeed * allowedSpeedDifferenceFactor)
       return true;
    }
    return false;
}

bool TrajectoryPredictor::stopReconnectPlanning() {
    std::unique_lock lineLock(trajectoryLineMutex);
    std::unique_lock indexPosLock(indexUpdatePositionMutex);
    std::unique_lock indexLock(nodeIndexMutex);
    std::unique_lock reconnectVecLock(reconnectVectorMutex);
    updatePrediction = false;
    if (locationUpdateThread->joinable()) {
        NES_DEBUG("TrajectoryPredictor: join reconnect planner thread");
        locationUpdateThread->join();
        //locationUpdateThread.reset();
        return true;
    } else {
        NES_ERROR("Trajectory predictor: reconnect planner thread not joinable");
        NES_THROW_RUNTIME_ERROR("error while stopping thread->join");
        return false;
    }
}

bool TrajectoryPredictor::updateDownloadedNodeIndex(Index::Experimental::Location currentLocation) {
    S2Point currentS2Point(S2LatLng::FromDegrees(currentLocation.getLatitude(), currentLocation.getLongitude()));
    std::unique_lock nodeIndexLock(nodeIndexMutex);
    if (S1Angle(currentS2Point, positionOfLastNodeIndexUpdate.value()) > coveredRadiusWithoutThreshold) {
        if (downloadFieldNodes() && fieldNodeMap.count(parentId) == 0) {
            NES_DEBUG("current parent was not present in downloaded list, adding it to the index")
            fieldNodeMap.insert({parentId, currentParentLocation});
            fieldNodeIndex.Add(currentParentLocation, parentId);
        }
        return true;
    }
    return false;
}

bool TrajectoryPredictor::updatePredictedPath(const Spatial::Index::Experimental::LocationPtr& oldLocation, const Spatial::Index::Experimental::LocationPtr& currentLocation) {
    int vertexIndex = 0;
    int* vertexIndexPtr = &vertexIndex;
    S2Point currentPoint(S2LatLng::FromDegrees(currentLocation->getLatitude(), currentLocation->getLongitude()));
    S1Angle distAngle = S2Earth::MetersToAngle(0);

    //if a predicted path exists, calculate how far the workers current location is from the path
    std::unique_lock trajectoryLock(trajectoryLineMutex);
    if (trajectoryLine) {
        auto pointOnLine = trajectoryLine->Project(currentPoint, vertexIndexPtr);
        distAngle = S1Angle(currentPoint, pointOnLine);
    }

    //if a predicted path exists and the current position is further away than delta: recompute the path
    //if no path exists: only calculate one if the location buffer is already filled
    //todo 2815: instead of just using points, calculate central points
    if ((trajectoryLine && distAngle > pathDistanceDeltaAngle) || (!trajectoryLine && locationBuffer.size() == locationBufferSize)) {
        NES_DEBUG("updating trajectory");
        S2Point oldPoint(S2LatLng::FromDegrees(oldLocation->getLatitude(), oldLocation->getLongitude()));
        S1Angle angle(oldPoint, currentPoint);
        auto extrapolatedPoint = S2::GetPointOnLine(oldPoint, currentPoint, predictedPathLengthAngle);
        //we need to extrapolate backwards aswell to make sure, that triangulation still works even if covering nodes lie behind the device
        auto backwardsExtrapolation = S2::GetPointOnLine(currentPoint, oldPoint, defaultCoverageRadiusAngle * 2);
        std::vector<S2Point> locationVector;
        locationVector.push_back(backwardsExtrapolation);
        locationVector.push_back(extrapolatedPoint);
        trajectoryLine = std::make_shared<S2Polyline>(locationVector);
        //pathBeginning = oldPoint;
        //pathEnd = extrapolatedPoint;
        //return true to indicate that the path prediction has changed
        return true;
    }

    //return false to indicate that the predicted path remains unchanged
    return false;
}

std::pair<S2Point, S1Angle> TrajectoryPredictor::findPathCoverage(const S2PolylinePtr& path, S2Point coveringNode, S1Angle coverage) {
    int vertexIndex = 0;
    auto projectedPoint = path->Project(coveringNode, &vertexIndex);
    auto distanceAngle = S1Angle(coveringNode, projectedPoint);
    NES_TRACE("distance from path in meters: " << S2Earth::ToMeters(distanceAngle))

    //if the distance is more than the coverage, it is not possible to cover the line
    if (distanceAngle > coverage) {
        NES_WARNING("no coverage possible with this node")
        return {S2Point(), S1Angle::Degrees(0)};
    }

    double divisor = cos(distanceAngle);
    if (std::isnan(divisor)) {
        NES_WARNING("divisor is NaN")
        return {S2Point(), S1Angle::Degrees(0)};
    }
    if (divisor == 0) {
        NES_WARNING("divisor is zero")
        return {S2Point(), S1Angle::Degrees(0)};
    }
    double coverageRadiansOnLine = acos(cos(coverage) / divisor);
    auto coverageAngleOnLine = S1Angle::Radians(coverageRadiansOnLine);

    auto verticeSpan = path->vertices_span();
    //the polyline always only consists of 2 points, so index 1 is its end
    S2Point coverageEnd = S2::GetPointOnLine(projectedPoint, verticeSpan[1], coverageAngleOnLine);
    return {coverageEnd, coverageAngleOnLine};
}

void TrajectoryPredictor::scheduleReconnects() {
    //after a reconnect the the coverage of the current parent needs to be calculated
    /*
    Timestamp bufferTravelTime = locationBuffer.back().second - locationBuffer.front().second;
    S1Angle bufferDistance(locationToS2Point(*locationBuffer.front().first), locationToS2Point(*locationBuffer.back().first));
    double meanDegreesPerNanosec = bufferDistance.degrees() / bufferTravelTime;
     */
    double remainingTime;
    std::unique_lock reconnectVectorLock(reconnectVectorMutex);
    std::unique_lock trajecotryLock(trajectoryLineMutex);
    reconnectVector.clear();
    //reconnectVector.erase(reconnectVector.begin(), reconnectVector.end());

        //todo: S1Angle or chordangle?
        auto reconnectionPointTuple = findPathCoverage(trajectoryLine, currentParentLocation, S1Angle(defaultCoverageRadiusAngle));
        if (reconnectionPointTuple.second.degrees() == 0) {
            return;
        }
        auto currentParentPathCoverageEnd = reconnectionPointTuple.first;
        //remainingTime = S1Angle(locationToS2Point(*locationBuffer.back().first), currentParentPathCoverageEnd).degrees() / meanDegreesPerNanosec;
        remainingTime = S1Angle(locationToS2Point(*locationBuffer.back().first), currentParentPathCoverageEnd).degrees() / bufferAverageMovementSpeed;
        endOfCoverageETA = locationBuffer.back().second + remainingTime;

    auto reconnectLocationOnPath = currentParentPathCoverageEnd;

    //todo: optimize the iterating over the nodes by distance and direction
    //todo: there is a closest point function which uses an already allocated vector, check if it makes sense performancewise
    //S1Angle currentUncoveredRemainingPathDistance(reconnectLocationOnPath, pathEnd);
    S1Angle currentUncoveredRemainingPathDistance(reconnectLocationOnPath, trajectoryLine->vertices_span()[1]);
    S1Angle minimumUncoveredRemainingPathDistance = currentUncoveredRemainingPathDistance;
    S2ClosestPointQuery<uint64_t> query(&fieldNodeIndex);
    S2Point nextReconnectLocationOnPath = reconnectLocationOnPath;
    uint64_t reconnectParentId;
    //S2Point previousReconnectLocationOnPath = locationToS2Point(*locationBuffer.back().first);
    //Timestamp previousEstimatedReconnectTime = locationBuffer.back().second;
    //todo: update this also when reconnecting or get rid of the variable
    Timestamp estimatedReconnectTime = endOfCoverageETA;
    Timestamp nextEstimatedReconnectTime;

    //todo: allow querying for longer distances to also make reconnect plans with gaps
    query.mutable_options()->set_max_distance(defaultCoverageRadiusAngle);

    //as long as the coverage achieved by the last scheduled reconnect does not reach closer than coverage to the end of the path: keep adding reconnects to the schedule
    while (currentUncoveredRemainingPathDistance > S1Angle(defaultCoverageRadiusAngle)) {
        S2ClosestPointQuery<int>::PointTarget target(reconnectLocationOnPath);
        auto closestNodeList = query.FindClosestPoints(&target);

        for (auto result : closestNodeList) {
            auto coverageTuple = findPathCoverage(trajectoryLine, result.point(), defaultCoverageRadiusAngle);
            currentUncoveredRemainingPathDistance = S1Angle(coverageTuple.first, trajectoryLine->vertices_span()[1]);
            //todo: add some delta for this check so we do not accidentally reconnect to the same node?
            if (currentUncoveredRemainingPathDistance < minimumUncoveredRemainingPathDistance) {
                nextReconnectLocationOnPath = coverageTuple.first;
                reconnectParentId = result.data();
                minimumUncoveredRemainingPathDistance = currentUncoveredRemainingPathDistance;
                //todo: implement calculation here too
                //remainingTime = S1Angle(previousReconnectLocationOnPath, reconnectLocationOnPath).degrees() / meanDegreesPerNanosec;
                //remainingTime = S1Angle(locationToS2Point(*locationBuffer.back().first), nextReconnectLocationOnPath).degrees() / meanDegreesPerNanosec;
                remainingTime = S1Angle(locationToS2Point(*locationBuffer.back().first), nextReconnectLocationOnPath).degrees() / bufferAverageMovementSpeed;
                nextEstimatedReconnectTime = locationBuffer.back().second + remainingTime;
            }
        }
        //todo: make some check here if currpoint has actually been set (maybe se tnext point t ocurrpoint in the beginning and then check if they are equal
        if (reconnectVector.empty() || nextReconnectLocationOnPath != reconnectLocationOnPath) {
            auto currLatLng = S2LatLng(reconnectLocationOnPath);
            auto currLoc =
                std::make_shared<Index::Experimental::Location>(currLatLng.lat().degrees(), currLatLng.lng().degrees());
            reconnectVector.emplace_back(reconnectParentId, currLoc, (uint64_t) estimatedReconnectTime);
            NES_DEBUG("scheduled reconnect to worker with id" << reconnectParentId)
            //previousReconnectLocationOnPath = reconnectLocationOnPath;
            reconnectLocationOnPath = nextReconnectLocationOnPath;
            estimatedReconnectTime = nextEstimatedReconnectTime;
        } else {
            NES_DEBUG("no nodes available to cover rest of path")
            break;
        }
    }
}

std::optional<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>> TrajectoryPredictor::getNextReconnect() {
    std::unique_lock lock(reconnectVectorMutex);
    if (reconnectVector.size() > 1) {
        return reconnectVector.at(0);
    } else {
        return std::nullopt;
    }
}
NES::Spatial::Index::Experimental::Location TrajectoryPredictor::getNodeLocationById(uint64_t id) {
    std::unique_lock lock(nodeIndexMutex);
    try {
        return Index::Experimental::s2pointToLocation(fieldNodeMap.at(id));
    } catch (std::out_of_range& e) {
        NES_DEBUG("trajectory predictor was queried for node which is not found in its location index")
        return {};
    }
}
size_t TrajectoryPredictor::getSizeOfSpatialIndex() {
    std::unique_lock lock(nodeIndexMutex);
    return fieldNodeMap.size();
}

std::tuple<NES::Spatial::Index::Experimental::LocationPtr, Timestamp>
TrajectoryPredictor::getLastReconnectLocationAndTime() {
    std::unique_lock lock(lastReconnectTimeMutex);
    return devicePositionTupleAtLastReconnect;
}

}
