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

#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>
#include <Spatial/Index/Location.hpp>
#include <Spatial/Index/Waypoint.hpp>
#include <Spatial/Mobility/LocationProvider.hpp>
#include <Spatial/Mobility/ReconnectConfigurator.hpp>
#include <Spatial/Mobility/ReconnectPoint.hpp>
#include <Spatial/Mobility/ReconnectPrediction.hpp>
#include <Spatial/Mobility/ReconnectSchedule.hpp>
#include <Spatial/Mobility/TrajectoryPredictor.hpp>
#include <Util/Experimental/S2Utilities.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <stdexcept>
#include <utility>

namespace NES::Spatial::Mobility::Experimental {

TrajectoryPredictor::TrajectoryPredictor(
    LocationProviderPtr locationProvider,
    const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration,
    uint64_t parentId)
    : locationProvider(std::move(locationProvider)) {
#ifdef S2DEF
    if (!(configuration->defaultCoverageRadius.getValue() < configuration->nodeIndexUpdateThreshold.getValue()
          < configuration->nodeInfoDownloadRadius.getValue())) {
        NES_FATAL_ERROR("Default Coverage Radius: "
                        << configuration->defaultCoverageRadius.getValue()
                        << ", node index update threshold: " << configuration->nodeIndexUpdateThreshold.getValue()
                        << ", node info download radius: " << configuration->nodeInfoDownloadRadius.getValue() << std::endl
                        << "These values will lead to gaps in reconnect planning. Exiting");
        exit(EXIT_FAILURE);
    }
    pathPredictionUpdateInterval = configuration->pathPredictionUpdateInterval.getValue();
    locationBufferSize = configuration->locationBufferSize.getValue();
    locationBufferSaveRate = configuration->locationBufferSaveRate.getValue();
    pathDistanceDeltaAngle = S2Earth::MetersToAngle(configuration->pathDistanceDelta.getValue());
    nodeInfoDownloadRadius = configuration->nodeInfoDownloadRadius.getValue();
    defaultCoverageRadiusAngle = S2Earth::MetersToAngle(configuration->defaultCoverageRadius.getValue());
    predictedPathLengthAngle = S2Earth::MetersToAngle(configuration->pathPredictionLength);
    coveredRadiusWithoutThreshold =
        S2Earth::MetersToAngle(nodeInfoDownloadRadius - configuration->nodeIndexUpdateThreshold.getValue());
    this->parentId = parentId;
    updatePrediction = false;
    speedDifferenceThresholdFactor = configuration->speedDifferenceThresholdFactor.getValue();
    bufferAverageMovementSpeed = 0;
    std::unique_lock lock(lastReconnectTupleMutex);
    lastReconnectWaypoint = std::make_shared<Index::Experimental::Waypoint>(Index::Experimental::Waypoint::invalid());
#else
    (void) configuration;
    (void) parentId;
    NES_FATAL_ERROR("cannot construct trajectory predictor without s2 library");
    exit(EXIT_FAILURE);
#endif
}

Mobility::Experimental::ReconnectSchedulePtr TrajectoryPredictor::getReconnectSchedule() {
#ifdef S2DEF
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
        //todo #2918: make create method
        start = std::make_shared<Index::Experimental::Location>();
        end = std::make_shared<Index::Experimental::Location>();
    }

    std::unique_lock reconnectVectorLock(reconnectVectorMutex);
    //get a shared ptr to a vector containing predicted reconnect inf oconsisting of expected parent id, expected reconnect location and expected time
    reconnectVectorLock.unlock();

    /*get a pointer to the position of the device at the time the local field node index was updated
    if no such update has happened so for, set the pointer to nullptr */
    Index::Experimental::LocationPtr indexUpdatePointer;
    std::unique_lock indexUpdatePosLock(indexUpdatePositionMutex);
    if (positionOfLastNodeIndexUpdate) {
        indexUpdatePointer = std::make_shared<Index::Experimental::Location>(
            Spatial::Util::S2Utilities::s2pointToLocation(positionOfLastNodeIndexUpdate.value()));
    } else {
        indexUpdatePointer = std::make_shared<Index::Experimental::Location>();
    }
    indexUpdatePosLock.unlock();

    //construct a schedule object and return it
    return std::make_shared<Mobility::Experimental::ReconnectSchedule>(parentId, start, end, indexUpdatePointer, reconnectVector);
#else
    NES_WARNING("trying to get reconnect schedule but s2 library is not used")
    return nullptr;
#endif
}

bool TrajectoryPredictor::downloadFieldNodes(Index::Experimental::LocationPtr currentLocation) {
#ifdef S2DEF
    if (!currentLocation || !currentLocation->isValid()) {
        NES_WARNING("invalid location, cannot download field nodes");
        return false;
    }
    NES_DEBUG("Downloading nodes in range")
    //get current position and download node information from coordinator
    //divide the download radius by 1000 to convert meters to kilometers
    auto nodeMapPtr = locationProvider->getNodeIdsInRange(currentLocation, nodeInfoDownloadRadius / 1000);

    //if we actually received nodes in our vicinity, we can clear the old nodes
    std::unique_lock nodeIndexLock(nodeIndexMutex);
    if (nodeMapPtr && !nodeMapPtr->empty()) {
        fieldNodeMap.clear();
        fieldNodeIndex.Clear();
    } else {
        return false;
    }

    //insert node info into spatial index and map on node ids
    for (auto [nodeId, location] : *nodeMapPtr) {
        NES_TRACE("adding node " << nodeId << " with location " << location.toString())
        fieldNodeIndex.Add(S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude())), nodeId);
        fieldNodeMap.insert({nodeId, S2Point(S2LatLng::FromDegrees(location.getLatitude(), location.getLongitude()))});
    }
    nodeIndexLock.unlock();

    //save the position of the update so we can check how far we have moved from there later on
    std::unique_lock positionAtUpdateLock(indexUpdatePositionMutex);
    positionOfLastNodeIndexUpdate = Spatial::Util::S2Utilities::locationToS2Point(*currentLocation);
    NES_TRACE("setting last index update position to " << currentLocation->toString())
    return true;
#else
    (void) currentLocation;
    NES_WARNING("s2 library is needed to download field node information")
    return false;
#endif
}

void TrajectoryPredictor::setUpReconnectPlanning(ReconnectConfiguratorPtr reconnectConfigurator) {
#ifdef S2DEF
    if (updatePrediction) {
        NES_WARNING("there is already a prediction thread running, cannot start another one")
        return;
    }

    //set this boolean to tell the locationUpdate thread to keep looping after it started
    //the thread will only terminate when this is set to false
    updatePrediction = true;

    //set the reconnect configurator so it can be called in case of a reconnect
    this->reconnectConfigurator = std::move(reconnectConfigurator);

    //get the current position and download the locations of the field nodes in the vicinity
    auto currentPosition = locationProvider->getWaypoint();
    downloadFieldNodes(currentPosition->getLocation());

    //find the current parent node among the downloaded field node data
    std::unique_lock nodeIndexLock(nodeIndexMutex);
    auto iterator = S2PointIndex<uint64_t>::Iterator(&fieldNodeIndex);
    iterator.Begin();
    while (!iterator.done()) {
        if (iterator.data() == parentId) {
            currentParentLocation = iterator.point();
            break;
        }
        iterator.Next();
    }
    nodeIndexLock.unlock();

    std::unique_lock vectorLock(reconnectVectorMutex);
    reconnectVector = std::make_shared<std::vector<ReconnectPointPtr>>();
    vectorLock.unlock();

    //if this workers current parent could not be found among the data, reconnect planning is not possible
    if (iterator.done()) {
        NES_DEBUG("parent id does not match any field node in the coverage area. Changing parent now")
        reconnectToClosestNode(locationProvider->getCurrentWaypoint());
        NES_DEBUG("set parent to " << parentId);
    }

    //start reconnect planner thread
    locationUpdateThread = std::make_shared<std::thread>(&TrajectoryPredictor::startReconnectPlanning, this);
#else
    (void) reconnectConfigurator;
    NES_WARNING("s2 library is needed to start reconnect planning")
#endif
}

void TrajectoryPredictor::startReconnectPlanning() {
#ifdef S2DEF
    updateDownloadedNodeIndex(locationProvider->getCurrentWaypoint()->getLocation());
    //fill up the buffer before starting to calculate path
    while (locationBuffer.size() < locationBufferSize) {
        auto currentLocation = locationProvider->getCurrentWaypoint();
        if (!locationBuffer.empty() && *currentLocation->getLocation() == *locationBuffer.back()->getLocation()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(pathPredictionUpdateInterval));
            continue;
        }
        locationBuffer.push_back(currentLocation);
        NES_DEBUG("added: " << locationBuffer.back()->getLocation()->toString() << ", "
                            << locationBuffer.back()->getTimestamp().value());
        std::this_thread::sleep_for(std::chrono::milliseconds(pathPredictionUpdateInterval * locationBufferSaveRate));
    }
    NES_TRACE("Location buffer is filled");

    NES_DEBUG("Saving a location to buffer each " << locationBufferSaveRate << " location updates");
    //set steps to max to trigger a location save right away
    size_t stepsSinceLastLocationSave = locationBufferSaveRate;
    S2Point nextReconnectNodeLocation;
    auto oldestKnownOwnLocation = locationBuffer.front();

    while (updatePrediction) {
        auto currentOwnLocation = locationProvider->getCurrentWaypoint();
        if (!locationBuffer.empty() && *currentOwnLocation->getLocation() == *locationBuffer.back()->getLocation()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(pathPredictionUpdateInterval));
            continue;
        }

        //if locationBufferSaveRate updates have been done since last save: save current location to buffer and reset save counter
        if (stepsSinceLastLocationSave == locationBufferSaveRate) {
            oldestKnownOwnLocation = locationBuffer.front();
            locationBuffer.pop_front();
            locationBuffer.push_back(currentOwnLocation);
            stepsSinceLastLocationSave = 0;
        } else {
            ++stepsSinceLastLocationSave;
        }

        //update the spatial index if necessary
        bool indexUpdated = updateDownloadedNodeIndex(currentOwnLocation->getLocation());
        //check if we deviated more than delta from the old predicted path and update it if needed
        bool pathUpdated = updatePredictedPath(oldestKnownOwnLocation->getLocation(), currentOwnLocation->getLocation());
        //update average movement speed
        bool speedChanges = updateAverageMovementSpeed();
        //if any of the input data for the reconnect prediction has changed, the scheduled reconnects need to be recalculated
        if (indexUpdated || pathUpdated || speedChanges) {
            NES_INFO("reconnect prediction data has changed")
            //todo #2815: instead of updating right away, look at if the new trajectory stabilizes itself after a turn
            scheduleReconnects();

            //check if scheduled reconnects exist and inform the coordinator about any changes
            std::unique_lock reconnectVectorLock(reconnectVectorMutex);
            if (!reconnectVector->empty()) {
                nextReconnectNodeLocation = fieldNodeMap.at(reconnectVector->at(0)->reconnectPrediction.expectedNewParentId);
                auto updatedReconnectPoint = getNextPredictedReconnect();
                std::optional<NES::Spatial::Mobility::Experimental::ReconnectPrediction> updatedPrediction;
                if (updatedReconnectPoint) {
                    updatedPrediction = updatedReconnectPoint->reconnectPrediction;
                } else {
                    updatedPrediction = {};
                }
                reconnectConfigurator->updateScheduledReconnect(updatedPrediction);
            } else {
                NES_INFO("rescheduled after reconnect but there is no next reconnect in list")
                reconnectConfigurator->updateScheduledReconnect(std::nullopt);
            }
        }

        std::unique_lock reconnectVectorLock(reconnectVectorMutex);

        //check if we are connected to a parent with a knwon position of if we can connect to one
        if (currentParentLocation || reconnectToClosestNode(currentOwnLocation)) {
            //if the we left the coverage radius of our current parent, check if the next node is close enough to reconnect
            S1Angle currentDistFromParent(Spatial::Util::S2Utilities::locationToS2Point(*currentOwnLocation->getLocation()),
                                          currentParentLocation.value());

            //if we are connected to a parent somewhere else, and have no reconnect data, connect to the closest node we can find
            if (reconnectVector->empty() && currentDistFromParent > defaultCoverageRadiusAngle) {
                reconnectToClosestNode(currentOwnLocation);
            }

            if (!reconnectVector->empty() && currentDistFromParent >= defaultCoverageRadiusAngle) {
                auto currentOwnPoint = Spatial::Util::S2Utilities::locationToS2Point(*currentOwnLocation->getLocation());

                //if the next expected parent is closer than the current one: reconnect
                if (S1Angle(currentOwnPoint, nextReconnectNodeLocation) <= currentDistFromParent) {
                    //reconnect and inform coordinator about upcoming reconnect
                    auto newParentId = reconnectVector->front()->reconnectPrediction.expectedNewParentId;
                    reconnect(newParentId, currentOwnLocation);
                    reconnectVector->erase(reconnectVector->begin());

                    //after reconnect, check if there is a next point on the schedule
                    if (!reconnectVector->empty()) {
                        auto coverageEndLoc = reconnectVector->front()->predictedReconnectLocation;
                        nextReconnectNodeLocation =
                            fieldNodeMap.at(reconnectVector->at(0)->reconnectPrediction.expectedNewParentId);
                        NES_INFO("reconnect point: " << coverageEndLoc.toString());
                    } else {
                        NES_INFO("no next reconnect scheduled")
                    }
                }
            }
        }
        reconnectVectorLock.unlock();
        //sleep for the specified amount of time
        std::this_thread::sleep_for(std::chrono::milliseconds(pathPredictionUpdateInterval));
    }
#else
    NES_WARNING("s2 library is needed to start reconnect planning")
#endif
}

void TrajectoryPredictor::reconnect(uint64_t newParentId, const Spatial::Index::Experimental::WaypointPtr& ownLocation) {
    std::unique_lock lastReconnectLock(lastReconnectTupleMutex);
    //todo #2918: pass pointer here
    reconnectConfigurator->reconnect(parentId, newParentId);
    lastReconnectWaypoint = ownLocation;

    auto nextReconnect = getNextPredictedReconnect();
    std::optional<NES::Spatial::Mobility::Experimental::ReconnectPrediction> updatedPrediction;
    if (nextReconnect) {
        updatedPrediction = nextReconnect->reconnectPrediction;
    } else {
        updatedPrediction = {};
    }
    reconnectConfigurator->updateScheduledReconnect(updatedPrediction);

    //update locally saved information about parent
    parentId = newParentId;
#ifdef S2DEF
    currentParentLocation = fieldNodeMap.at(newParentId);
#endif
}

bool TrajectoryPredictor::reconnectToClosestNode(const Index::Experimental::WaypointPtr& ownLocation) {
#ifdef S2DEF
    std::unique_lock nodeIndexLock(nodeIndexMutex);
    if (fieldNodeIndex.num_points() == 0) {
        return false;
    }
    S2ClosestPointQuery<uint64_t> query(&fieldNodeIndex);
    query.mutable_options()->set_max_distance(defaultCoverageRadiusAngle);
    S2ClosestPointQuery<int>::PointTarget target(Spatial::Util::S2Utilities::locationToS2Point(*ownLocation->getLocation()));
    auto closestNode = query.FindClosestPoint(&target);
    if (closestNode.is_empty()) {
        return false;
    }
    reconnect(closestNode.data(), ownLocation);
    return true;
#else
    (void) ownLocation;
    return false;
#endif
}

bool TrajectoryPredictor::updateAverageMovementSpeed() {
#ifdef S2DEF
    //calculate the movement speed based on the locations and timestamps in the locationBuffer
    Timestamp bufferTravelTime = locationBuffer.back()->getTimestamp().value() - locationBuffer.front()->getTimestamp().value();
    S1Angle bufferDistance(Spatial::Util::S2Utilities::locationToS2Point(*locationBuffer.front()->getLocation()),
                           Spatial::Util::S2Utilities::locationToS2Point(*locationBuffer.back()->getLocation()));
    double meanDegreesPerNanosec = bufferDistance.degrees() / bufferTravelTime;

    //check if there is a speed difference which surpasses the threshold compared to the previously calculated speed
    //if this is the case, update the value
    if (abs(meanDegreesPerNanosec - bufferAverageMovementSpeed) > bufferAverageMovementSpeed * speedDifferenceThresholdFactor) {
        bufferAverageMovementSpeed = meanDegreesPerNanosec;
        NES_TRACE("average movement speed was updated to " << bufferAverageMovementSpeed)
        NES_TRACE("threshhold is " << bufferAverageMovementSpeed * speedDifferenceThresholdFactor)
        return true;
    }
    return false;
#else
    NES_WARNING("s2 library is needed to update average movement speed")
    return false;
#endif
}

bool TrajectoryPredictor::stopReconnectPlanning() {
    if (!updatePrediction) {
        return false;
    }

    //unset update flag to let the update loop finish
    updatePrediction = false;
    return true;
}

bool TrajectoryPredictor::updateDownloadedNodeIndex(Index::Experimental::LocationPtr currentLocation) {
#ifdef S2DEF
    S2Point currentS2Point(S2LatLng::FromDegrees(currentLocation->getLatitude(), currentLocation->getLongitude()));
    std::unique_lock nodeIndexLock(nodeIndexMutex);

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

bool TrajectoryPredictor::updatePredictedPath(const Spatial::Index::Experimental::LocationPtr& newPathStart,
                                              const Spatial::Index::Experimental::LocationPtr& currentLocation) {
#ifdef S2DEF
    //if path end and beginning are the same location, we cannot construct a path out of that data
    if (*newPathStart == *currentLocation) {
        return false;
    }
    int vertexIndex = 0;
    int* vertexIndexPtr = &vertexIndex;
    S2Point currentPoint = Util::S2Utilities::locationToS2Point(*currentLocation);
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
    if ((trajectoryLine && distAngle > pathDistanceDeltaAngle)
        || (!trajectoryLine && locationBuffer.size() == locationBufferSize)) {
        NES_DEBUG("updating trajectory");
        S2Point oldPoint = Util::S2Utilities::locationToS2Point(*newPathStart);
        auto extrapolatedPoint = S2::GetPointOnLine(oldPoint, currentPoint, predictedPathLengthAngle);
        //we need to extrapolate backwards as well to make sure, that triangulation still works even if covering nodes lie behind the device
        auto backwardsExtrapolation = S2::GetPointOnLine(currentPoint, oldPoint, defaultCoverageRadiusAngle * 2);
        trajectoryLine = std::make_shared<S2Polyline>(std::vector({backwardsExtrapolation, extrapolatedPoint}));
        return true;
    }

    //return false to indicate that the predicted path remains unchanged
    return false;
#else
    (void) newPathStart;
    (void) currentLocation;
    NES_WARNING("s2 library is needed to update predicted path")
    return false;
#endif
}

#ifdef S2DEF
std::pair<S2Point, S1Angle>
TrajectoryPredictor::findPathCoverage(const S2PolylinePtr& path, S2Point coveringNode, S1Angle coverage) {
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
#endif

void TrajectoryPredictor::scheduleReconnects() {
#ifdef S2DEF
    if (!currentParentLocation) {
        return;
    }
    double remainingTime;
    std::unique_lock reconnectVectorLock(reconnectVectorMutex);
    std::unique_lock trajecotryLock(trajectoryLineMutex);
    std::unique_lock indexLock(nodeIndexMutex);
    reconnectVector->clear();

    //find the end of path coverage of our curent parent
    auto reconnectionPointTuple =
        findPathCoverage(trajectoryLine, currentParentLocation.value(), S1Angle(defaultCoverageRadiusAngle));
    if (reconnectionPointTuple.second.degrees() == 0) {
        return;
    }
    auto currentParentPathCoverageEnd = reconnectionPointTuple.first;

    //find the expected time of arrival at the end of coverage of our current parent
    remainingTime = S1Angle(Spatial::Util::S2Utilities::locationToS2Point(*locationBuffer.back()->getLocation()),
                            currentParentPathCoverageEnd)
                        .degrees()
        / bufferAverageMovementSpeed;
    auto endOfCoverageETA = locationBuffer.back()->getTimestamp().value() + remainingTime;

    auto reconnectLocationOnPath = currentParentPathCoverageEnd;

    //initialize loop variables
    S1Angle currentUncoveredRemainingPathDistance(reconnectLocationOnPath, trajectoryLine->vertices_span()[1]);
    S1Angle minimumUncoveredRemainingPathDistance = currentUncoveredRemainingPathDistance;
    S2ClosestPointQuery<uint64_t> query(&fieldNodeIndex);
    S2Point nextReconnectLocationOnPath = reconnectLocationOnPath;
    uint64_t reconnectParentId;
    Timestamp estimatedReconnectTime = endOfCoverageETA;
    Timestamp nextEstimatedReconnectTime;

    query.mutable_options()->set_max_distance(defaultCoverageRadiusAngle);

    //as long as the coverage achieved by the last scheduled reconnect does not reach closer than coverage to the end of the path: keep adding reconnects to the schedule
    while (currentUncoveredRemainingPathDistance > S1Angle(defaultCoverageRadiusAngle)) {

        //find nodes which cover reconnectLocationOnPath
        S2ClosestPointQuery<int>::PointTarget target(reconnectLocationOnPath);
        auto closestNodeList = query.FindClosestPoints(&target);

        //iterate over all nodes which cover the reconnect location to find out which one will give us the longest coverage in the direction of the path end point
        for (auto result : closestNodeList) {
            //calculate how much of the path will remain uncovered if we pick this node
            auto coverageTuple = findPathCoverage(trajectoryLine, result.point(), defaultCoverageRadiusAngle);
            currentUncoveredRemainingPathDistance = S1Angle(coverageTuple.first, trajectoryLine->vertices_span()[1]);

            //if the distance that remains uncovered is less then the current minimum, pick this node as the new optimal choice
            if (currentUncoveredRemainingPathDistance < minimumUncoveredRemainingPathDistance) {
                nextReconnectLocationOnPath = coverageTuple.first;
                reconnectParentId = result.data();
                minimumUncoveredRemainingPathDistance = currentUncoveredRemainingPathDistance;
                remainingTime = S1Angle(Spatial::Util::S2Utilities::locationToS2Point(*locationBuffer.back()->getLocation()),
                                        nextReconnectLocationOnPath)
                                    .degrees()
                    / bufferAverageMovementSpeed;
                nextEstimatedReconnectTime = locationBuffer.back()->getTimestamp().value() + remainingTime;
            }
        }

        //if we found a reconnect which is different from the last one on the list, add it to the vector as soon as we
        if (nextReconnectLocationOnPath.operator!=(reconnectLocationOnPath)) {
            auto currLatLng = S2LatLng(reconnectLocationOnPath);
            auto currLoc =
                std::make_shared<Index::Experimental::Location>(currLatLng.lat().degrees(), currLatLng.lng().degrees());
            reconnectVector->emplace_back(std::make_shared<NES::Spatial::Mobility::Experimental::ReconnectPoint>(
                NES::Spatial::Mobility::Experimental::ReconnectPoint{
                    *currLoc,
                    NES::Spatial::Mobility::Experimental::ReconnectPrediction{reconnectParentId,
                                                                              (uint64_t) estimatedReconnectTime}}));
            NES_DEBUG("scheduled reconnect to worker with id" << reconnectParentId)
            reconnectLocationOnPath = nextReconnectLocationOnPath;
            estimatedReconnectTime = nextEstimatedReconnectTime;
        } else {
            NES_DEBUG("no nodes available to cover rest of path")
            break;
        }
    }
#else
    NES_WARNING("s2 library is required to schedule reconnects")
#endif
}

std::shared_ptr<ReconnectPoint> TrajectoryPredictor::getNextPredictedReconnect() {
    //if no reconnect vector exists, return a nullpointer
    if (!reconnectVector) {
        NES_WARNING("Trying to obtain next predicted reconnect, but reconnect vector does not exist");
        return {};
    }
    std::unique_lock lock(reconnectVectorMutex);
    if (reconnectVector->size() > 1) {
        return reconnectVector->at(0);
    } else {
        return {};
    }
}

#ifdef S2DEF
NES::Spatial::Index::Experimental::Location TrajectoryPredictor::getNodeLocationById(uint64_t id) {
    std::unique_lock lock(nodeIndexMutex);
    try {
        return Spatial::Util::S2Utilities::s2pointToLocation(fieldNodeMap.at(id));
    } catch (std::out_of_range& e) {
        NES_DEBUG("trajectory predictor was queried for node which is not found in its location index")
        return {};
    }
}
size_t TrajectoryPredictor::getSizeOfSpatialIndex() {
    std::unique_lock lock(nodeIndexMutex);
    return fieldNodeMap.size();
}
#endif

NES::Spatial::Index::Experimental::WaypointPtr TrajectoryPredictor::getLastReconnectLocationAndTime() {
    std::unique_lock lock(lastReconnectTupleMutex);
    return lastReconnectWaypoint;
}
}// namespace NES::Spatial::Mobility::Experimental
