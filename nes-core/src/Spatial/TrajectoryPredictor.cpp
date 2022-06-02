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

namespace NES::Spatial::Mobility::Experimental {

TrajectoryPredictor::TrajectoryPredictor(LocationProviderPtr locationProvider, Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr configuration, uint64_t parentId) : locationProvider(locationProvider) {
    //todo: make the names match to avoid confusion
    pathUpdateInterval = configuration->pathPredictionUpdateInterval.getValue();
    locationBufferSize = configuration->locationBufferSize.getValue();
    saveRate = configuration->locationBufferSaveRate.getValue();
    deltaAngle = S2Earth::MetersToChordAngle(configuration->pathDistanceDelta.getValue());
    nodeDownloadRadius = configuration->nodeInfoDownloadRadius.getValue();
    //nodeIndexUpdateThreshold = configuration->nodeIndexUpdateThreshold.getValue();
    defaultCoverageAngle = S2Earth::MetersToChordAngle(configuration->defaultCoverageRadius.getValue());
    predictedPathLengthAngle = S2Earth::MetersToChordAngle(configuration->pathPredictionLength);
    //todo:  we might want to use a smaller search radius and add this as a parameter
    reconnectSearchRadius = S2Earth::MetersToAngle(nodeDownloadRadius);
    coveredRadiusWithoutThreshold = S2Earth::MetersToAngle(nodeDownloadRadius - configuration->nodeIndexUpdateThreshold.getValue());
    this->parentId = parentId;
    //maxPlannedReconnects =
    //setUpReconnectPlanning();
}

Mobility::Experimental::ReconnectSchedulePtr TrajectoryPredictor::getReconnectSchedule() {
    /*
    if (!trajectoryLine) {
        NES_DEBUG("getReconnectSchedule(): predicted path does not exist")
        return std::make_shared<ReconnectSchedule>(ReconnectSchedule::Empty());
    }
     */
    S2LatLng startLatLng(pathBeginning);
    auto start = std::make_shared<Index::Experimental::Location>(startLatLng.lat().degrees(), startLatLng.lng().degrees());
    S2LatLng endLatLng(pathEnd);
    auto end = std::make_shared<Index::Experimental::Location>(endLatLng.lat().degrees(), endLatLng.lng().degrees());
    //todo: this should node be made here, instead make the reconnect vector a pointer itself
    auto vec = std::make_shared<std::vector<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>>>(reconnectVector);
    return std::make_shared<Mobility::Experimental::ReconnectSchedule>(start, end, vec);
}

bool TrajectoryPredictor::downloadFieldNodes() {
    NES_DEBUG("Downloading nodes in range")
    auto positionAtUpdate = locationProvider->getLocation();
    auto nodeList = locationProvider->getNodeIdsInRange(*positionAtUpdate, nodeDownloadRadius / 1000);

    //if we actually received nodes in out vicinity, we can clear the old nodes
    //todo: do we always want to do this?
    if (!nodeList.empty()) {
        fieldNodeMap.clear();
        fieldNodeIndex.Clear();
    } else {
        return false;
    }

    for (auto [nodeId, location] : nodeList) {
        NES_TRACE("adding node " << nodeId << " with location " << location.toString())
        fieldNodeIndex.Add(S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude())), nodeId);
        fieldNodeMap.insert({nodeId, S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude()))});
    }
    positionOfLastNodeIndexUpdate = locationToS2Point(*positionAtUpdate);
    NES_TRACE("setting last index update position to " << positionAtUpdate->toString())
    return true;
}

void TrajectoryPredictor::setUpReconnectPlanning(ReconnectConfiguratorPtr reconnectConfigurator) {
    this->reconnectConfigurator = std::move(reconnectConfigurator);
    //todo: this can also moved into the isMobile if condition?
    downloadFieldNodes();
    NES_DEBUG("Parent Id: " << parentId)
    auto elem = S2PointIndex<uint64_t>::Iterator(&fieldNodeIndex);
    elem.Begin();
    while(!elem.done()) {
        if (elem.data() == parentId) {
            currentParentLocation = elem.point();
            break;
        }
        elem.Next();
    }
    if (elem.done()) {
        //todo: test this
        NES_DEBUG("parent id does not match any field node in the coverage area. Cannot start reconnect planning")
        return;
    }
    locationUpdateThread = std::make_shared<std::thread>(&TrajectoryPredictor::startReconnectPlanning, this);
}

void TrajectoryPredictor::startReconnectPlanning() {
    //fill up the buffer before starting to calculate path
    for (size_t i = 0; i < locationBufferSize; ++i) {
        auto currentLocation = locationProvider->getCurrentLocation();
        locationBuffer.push_back(currentLocation);
        NES_DEBUG("added: " << locationBuffer.back().first->toString() << locationBuffer.back().second);
        std::this_thread::sleep_for(std::chrono::milliseconds(pathUpdateInterval * saveRate));
        updateDownloadedNodeIndex(*currentLocation.first);
    }
    NES_TRACE("Location buffer is filled");

    NES_DEBUG("Saving a location to buffer each " << saveRate << " location updates");
    size_t stepsSinceLastLocationSave = saveRate;
    S2Point nextReconnectNodeLocation;
    auto oldestKnownOwnLocation = locationBuffer.front();

    while (true) {
        auto currentOwnLocation = locationProvider->getCurrentLocation();

        //if locationBufferSaveRate updates have been done since last save: save current location to buffer
        if (stepsSinceLastLocationSave == saveRate) {
            oldestKnownOwnLocation = locationBuffer.front();
            locationBuffer.pop_front();
            locationBuffer.push_back(currentOwnLocation);
            NES_DEBUG("added: " << currentOwnLocation.first->toString() << "; " << currentOwnLocation.second);
            NES_DEBUG("removed: " << oldestKnownOwnLocation.first->toString() << "; " << oldestKnownOwnLocation.second);
            stepsSinceLastLocationSave = 0;
            //todo: does it make sense here to update since the last position in buffer or should we use the path beginning?
        } else {
            stepsSinceLastLocationSave += 1;
        }

        //check if we deviated more than delta from the old predicted path and update it if needed
        if (updateDownloadedNodeIndex(*currentOwnLocation.first) || updatePredictedPath(oldestKnownOwnLocation.first, currentOwnLocation.first)) {
            NES_INFO("predicted path was recalculated")

            //we just recalculated the path, so the end of coverage of our current parent might have changed
            currentParentPathCoverageEnd = std::nullopt;
            //because the path might have changed, the reconnect sequence needs to be recalculated
            //todo: instead of updateing right away, look at if the new trajectory stabilizes itself after a turn
            scheduleReconnects();

            //check if there are scheduled reconnects
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

        //if we entered the coverage of the next schedules reconnect node, then reconnect
        if (!reconnectVector.empty() && S1Angle(S2Point(S2LatLng::FromDegrees(currentOwnLocation.first->getLatitude(),
                                                                              currentOwnLocation.first->getLongitude())),
                                                nextReconnectNodeLocation) < S1Angle(defaultCoverageAngle)) {

            //reconnect and inform coordinator about upcoming reconnect
            std::optional<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>> nextReconnect = getNextReconnect();
            //todo: pute the update and reconnect here in one call
            reconnectConfigurator->reconnect(parentId, get<0>(reconnectVector.front()));
            reconnectConfigurator->update(nextReconnect);

            //update locally saved information about parent
            parentId = get<0>(reconnectVector.front());
            currentParentLocation = fieldNodeMap.at(get<0>(reconnectVector[0]));
            currentParentPathCoverageEnd = std::nullopt;
            reconnectVector.erase(reconnectVector.begin());

            //after reconnect, check if there is a next point on the schedule
            if (!reconnectVector.empty()) {
                auto coverageEndLoc = std::get<1>(reconnectVector.front());
                //todo: maybe save them as s2points right away to avoid converting the each time
                currentParentPathCoverageEnd =
                    S2Point(S2LatLng::FromDegrees(coverageEndLoc->getLatitude(), coverageEndLoc->getLongitude()));
                nextReconnectNodeLocation = fieldNodeMap.at(get<0>(reconnectVector[0]));
                NES_INFO("reconnect point: " << coverageEndLoc);
            } else {
                NES_INFO("no next reconnect scheduled")
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(pathUpdateInterval));
    }
}

bool TrajectoryPredictor::updateDownloadedNodeIndex(Index::Experimental::Location currentLocation) {
    S2Point currentS2Point(S2LatLng::FromDegrees(currentLocation.getLatitude(), currentLocation.getLongitude()));
    if (S1Angle(currentS2Point, positionOfLastNodeIndexUpdate) > coveredRadiusWithoutThreshold) {
        if (downloadFieldNodes() && fieldNodeMap.count(parentId) == 0) {
            NES_DEBUG("current parent was not present in downloaded list, adding it to the index")
            //todo: put a tuples into the spatial index instead to get rid of the map
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
    S1ChordAngle distAngle = S2Earth::MetersToChordAngle(0);

    //if a predicted path exists, calculate how far the workers current location is from the path
    if (trajectoryLine) {
        auto pointOnLine = trajectoryLine->Project(currentPoint, vertexIndexPtr);
        NES_DEBUG("point on line" << pointOnLine);
        distAngle = S1ChordAngle(currentPoint, pointOnLine);
        NES_DEBUG("dist: " << S2Earth::ToMeters(distAngle));
    }

    //if a predicted path exists and the current position is furhter awy than delta: recompute the path
    //if no path exists: only calculate one if the location buffer is already filled
    //todo 2815: instead of just using points, calculate central points
    if ((trajectoryLine && distAngle > deltaAngle) || (!trajectoryLine && locationBuffer.size() == locationBufferSize)) {
        NES_DEBUG("updating trajectory");
        S2Point oldPoint(S2LatLng::FromDegrees(oldLocation->getLatitude(), oldLocation->getLongitude()));
        S1Angle angle(oldPoint, currentPoint);
        auto extrapolatedPoint = S2::GetPointOnLine(oldPoint, currentPoint, predictedPathLengthAngle);
        std::vector<S2Point> locationVector;
        locationVector.push_back(oldPoint);
        locationVector.push_back(extrapolatedPoint);
        trajectoryLine = std::make_shared<S2Polyline>(locationVector);
        pathBeginning = oldPoint;
        pathEnd = extrapolatedPoint;
        //return true to indicate the the path prediction has changed
        return true;
    }

    //return false to indicate that the predicted path remains unchanged
    return false;
}

std::pair<S2Point, S1Angle> TrajectoryPredictor::findPathCoverage(S2PolylinePtr path, S2Point coveringNode, S1Angle coverage) {
    int vertexIndex = 0;
    auto projectedPoint = path->Project(coveringNode, &vertexIndex);
    auto distanceAngle = S1Angle(coveringNode, projectedPoint);
    NES_DEBUG("distance from path in meters: " << S2Earth::ToMeters(distanceAngle))

    //if the distance is more than the coverage, it is not possible to cover the line
    if (distanceAngle > coverage) {
        NES_WARNING("no coverage possible with this node")
        return {S2Point(), S1Angle::Degrees(0)};
    }

    double divisor = cos(distanceAngle);
    if (std::isnan(divisor)) {
        NES_WARNING("divisor is NaN")
    }
    if (divisor == 0) {
        NES_WARNING("divisor is zero")
    }
    double coverageRadiansOnLine = acos(cos(coverage) / divisor);
    NES_DEBUG("coverage radians on line: " << coverageRadiansOnLine)
    auto coverageAngleOnLine = S1Angle::Radians(coverageRadiansOnLine);

    auto verticeSpan = path->vertices_span();
    NES_INFO("projected point" << S2LatLng(projectedPoint))
    //the polyline always only consists of 2 points, so index 1 is its end
    S2Point coverageEnd = S2::GetPointOnLine(projectedPoint, verticeSpan[1], coverageAngleOnLine);
    NES_INFO("coverage end" << S2LatLng(coverageEnd))
    NES_INFO("coverage angle on line" << coverageAngleOnLine.degrees())
    return {coverageEnd, coverageAngleOnLine};
}

//todo: check if we want to use parameters instead of attributes
void TrajectoryPredictor::scheduleReconnects() {
    //after a reconnect the the coverage of the current parent needs to be calculated
    if (!currentParentPathCoverageEnd.has_value()) {
        //todo: S1Angle or chordangle?
        auto reconnectionPointTuple = findPathCoverage(trajectoryLine, currentParentLocation, S1Angle(defaultCoverageAngle));
        currentParentPathCoverageEnd = reconnectionPointTuple.first;
        //todo: use angle to calculate approx arrival time here
        (void) endOfCoverageETA;
    }

    reconnectVector.clear();
    auto reconnectLocationOnPath = currentParentPathCoverageEnd.value();

    //todo: optimize the iterating over the nodes by distance and direction
    //todo: there is a closest point function which uses an already allocated vector, check if it makes sense performancewise
    S1Angle currentUncoveredRemainingPathDistance(reconnectLocationOnPath, pathEnd);
    S1Angle minimumUncoveredRemainingPathDistance = currentUncoveredRemainingPathDistance;
    S2ClosestPointQuery<uint64_t> query(&fieldNodeIndex);
    S2Point nextReconnectLocationOnPath = reconnectLocationOnPath;
    uint64_t reconnectParentId;

    query.mutable_options()->set_max_distance(defaultCoverageAngle);

    //as long as the coverage achieved by the last scheduled reconnect does not reach closer than coverage to the end of the path: keep adding reconnects to the schedule
    while (currentUncoveredRemainingPathDistance > S1Angle(defaultCoverageAngle)) {
        S2ClosestPointQuery<int>::PointTarget target(reconnectLocationOnPath);
        auto closestNodeList = query.FindClosestPoints(&target);

        for (auto result : closestNodeList) {
            auto coverageTuple = findPathCoverage(trajectoryLine, result.point(), S1Angle(defaultCoverageAngle));
            currentUncoveredRemainingPathDistance = S1Angle(coverageTuple.first, pathEnd);
            //todo: add some delta for this check so we do not accidentally reconnect to the same node?
            if (currentUncoveredRemainingPathDistance < minimumUncoveredRemainingPathDistance) {
                nextReconnectLocationOnPath = coverageTuple.first;
                reconnectParentId = result.data();
                minimumUncoveredRemainingPathDistance = currentUncoveredRemainingPathDistance;
            }
        }
        //todo: make some check here if currpoint has actually been set (maybe se tnext point t ocurrpoint in the beginning and then check if they are equal
        if (nextReconnectLocationOnPath != reconnectLocationOnPath) {
            auto currLatLng = S2LatLng(reconnectLocationOnPath);
            auto currLoc =
                std::make_shared<Index::Experimental::Location>(currLatLng.lat().degrees(), currLatLng.lng().degrees());
            //todo: add timestamp calculation
            reconnectVector.emplace_back(reconnectParentId, currLoc, (uint64_t) 0);
            NES_DEBUG("scheduled reconnect to worker with id" << reconnectParentId)
            reconnectLocationOnPath = nextReconnectLocationOnPath;
        } else {
            NES_WARNING("no nodes available to cover rest of path")
            break;
        }
    }
}
S2Point TrajectoryPredictor::locationToS2Point(Index::Experimental::Location location) {
    return {S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude())};
}

Index::Experimental::Location TrajectoryPredictor::s2pointToLocation(S2Point point) {
    S2LatLng latLng(point);
    return {latLng.lat().degrees(), latLng.lng().degrees()};
}

std::optional<std::tuple<uint64_t, Index::Experimental::LocationPtr, Timestamp>> TrajectoryPredictor::getNextReconnect() {
    if (reconnectVector.size() > 1) {
        return reconnectVector.at(1);
    } else {
        return std::nullopt;
    }
}
}
