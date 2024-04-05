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
#include <Mobility/LocationProviders/LocationProvider.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedule.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
#include <Util/Mobility/ReconnectPoint.hpp>
#include <Util/Mobility/S2Utilities.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Mobility/WorkerMobilityHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <stdexcept>

NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::ReconnectSchedulePredictor(
    const Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration) {
#ifdef S2DEF
    nodeInfoDownloadRadius = configuration->nodeInfoDownloadRadius.getValue();
    if (configuration->defaultCoverageRadius.getValue() > configuration->nodeIndexUpdateThreshold.getValue()) {
        NES_FATAL_ERROR("Default Coverage Radius: {} is bigger than the node index update threshold: {}. this would lead to "
                        "nodes not being not discoverable although they are in range. Exiting",
                        configuration->defaultCoverageRadius.getValue(),
                        configuration->nodeIndexUpdateThreshold.getValue());
        exit(EXIT_FAILURE);
    }
    if (configuration->nodeIndexUpdateThreshold.getValue() > nodeInfoDownloadRadius) {
        NES_FATAL_ERROR("Node info download radius: {} is smaller than the node index update threshold: {}. this would lead to "
                        "downloading node info after every location update. Exiting",
                        nodeInfoDownloadRadius,
                        configuration->nodeIndexUpdateThreshold.getValue());
        exit(EXIT_FAILURE);
    }

    locationBufferSize = configuration->locationBufferSize.getValue();
    locationBufferSaveRate = configuration->locationBufferSaveRate.getValue();
    pathDistanceDeltaAngle = S2Earth::MetersToAngle(configuration->pathDistanceDelta.getValue());
    predictedPathLengthAngle = S2Earth::MetersToAngle(configuration->pathPredictionLength);
    defaultCoverageRadiusAngle = S2Earth::MetersToAngle(configuration->defaultCoverageRadius.getValue());
    speedDifferenceThresholdFactor = configuration->speedDifferenceThresholdFactor.getValue();
    bufferAverageMovementSpeed = 0;
    stepsSinceLastLocationSave = 0;
#else
    (void) configuration;
    NES_FATAL_ERROR("cannot construct trajectory predictor without s2 library");
    exit(EXIT_FAILURE);
#endif
}

NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictorPtr
NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::create(
    [[maybe_unused]] const NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr& configuration) {
#ifdef S2DEF
    return std::make_shared<NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor>(configuration);
#else
    return nullptr;
#endif
}

#ifdef S2DEF
std::optional<NES::Spatial::Mobility::Experimental::ReconnectSchedule>
NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::getReconnectSchedule(
    const DataTypes::Experimental::Waypoint& currentOwnLocation,
    const DataTypes::Experimental::GeoLocation& parentLocation,
    const S2PointIndex<uint64_t>& fieldNodeIndex,
    bool isIndexUpdated) {
    //if the device location has not changed, there are no new calculations to be made
    if (!locationBuffer.empty() && currentOwnLocation.getLocation() == locationBuffer.back().getLocation()) {
        NES_DEBUG("Location has not changed, do not recalculate schedule");
        return std::nullopt;
    }

    //if the location buffer is not filled yet, do not schedule anything
    if (locationBuffer.size() < locationBufferSize) {
        if (stepsSinceLastLocationSave == locationBufferSaveRate) {
            locationBuffer.push_back(currentOwnLocation);
            stepsSinceLastLocationSave = 0;
            NES_DEBUG("Location buffer is not filled yet, do not recalculate schedule");
        } else {
            ++stepsSinceLastLocationSave;
        }
        return std::nullopt;
    }

    //if locationBufferSaveRate updates have been done since last save: save current location to buffer and reset save counter
    auto oldestKnownOwnLocation = locationBuffer.front();
    if (stepsSinceLastLocationSave == locationBufferSaveRate) {
        oldestKnownOwnLocation = locationBuffer.front();
        locationBuffer.pop_front();
        locationBuffer.push_back(currentOwnLocation);
        stepsSinceLastLocationSave = 0;
    } else {
        ++stepsSinceLastLocationSave;
    }

    //check if we deviated more than delta from the old predicted path and update it if needed
    bool isPathUpdated = updatePredictedPath(oldestKnownOwnLocation.getLocation(), currentOwnLocation.getLocation());
    //update average movement speed
    bool isSpeedChanged = updateAverageMovementSpeed();
    //if any of the input data for the reconnect prediction has changed, the scheduled reconnects need to be recalculated

    if (isIndexUpdated || isPathUpdated || isSpeedChanged) {
        NES_INFO("reconnect prediction data has changed");
        //todo #2815: instead of updating right away, look at if the new trajectory stabilizes itself after a turn
        scheduleReconnects(NES::Spatial::Util::S2Utilities::geoLocationToS2Point(parentLocation), fieldNodeIndex);
        return ReconnectSchedule(reconnectPoints);
    }
    return std::nullopt;
}
#endif

bool NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::updateAverageMovementSpeed() {
#ifdef S2DEF
    //calculate the movement speed based on the locations and timestamps in the locationBuffer
    Timestamp bufferTravelTime = locationBuffer.back().getTimestamp().value() - locationBuffer.front().getTimestamp().value();
    S1Angle bufferDistance(Spatial::Util::S2Utilities::geoLocationToS2Point(locationBuffer.front().getLocation()),
                           Spatial::Util::S2Utilities::geoLocationToS2Point(locationBuffer.back().getLocation()));
    double meanDegreesPerNanosec = bufferDistance.degrees() / bufferTravelTime;

    //check if there is a speed difference which surpasses the threshold compared to the previously calculated speed
    //if this is the case, update the value
    if (abs(meanDegreesPerNanosec - bufferAverageMovementSpeed) > bufferAverageMovementSpeed * speedDifferenceThresholdFactor) {
        bufferAverageMovementSpeed = meanDegreesPerNanosec;
        NES_TRACE("average movement speed was updated to {}", bufferAverageMovementSpeed);
        NES_TRACE("threshhold is {}", bufferAverageMovementSpeed * speedDifferenceThresholdFactor);
        return true;
    }
    return false;
#else
    NES_WARNING("s2 library is needed to update average movement speed");
    return false;
#endif
}

bool NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::updatePredictedPath(
    const Spatial::DataTypes::Experimental::GeoLocation& newPathStart,
    const Spatial::DataTypes::Experimental::GeoLocation& currentLocation) {
#ifdef S2DEF
    //if path end and beginning are the same location, we cannot construct a path out of that data
    if (newPathStart == currentLocation) {
        return false;
    }
    int vertexIndex = 0;
    int* vertexIndexPtr = &vertexIndex;
    S2Point currentPoint = Util::S2Utilities::geoLocationToS2Point(currentLocation);
    S1Angle distAngle = S2Earth::MetersToAngle(0);

    //if a predicted path exists, calculate how far the workers current location is from the path
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
        S2Point oldPoint = Util::S2Utilities::geoLocationToS2Point(newPathStart);
        auto extrapolatedPoint = S2::GetPointOnLine(oldPoint, currentPoint, predictedPathLengthAngle);
        //we need to extrapolate backwards as well to make sure, that triangulation still works even if covering nodes lie behind the device
        auto backwardsExtrapolation = S2::GetPointOnLine(currentPoint, oldPoint, defaultCoverageRadiusAngle * 2);
        trajectoryLine = S2Polyline({backwardsExtrapolation, extrapolatedPoint});
        return true;
    }

    //return false to indicate that the predicted path remains unchanged
    return false;
#else
    (void) newPathStart;
    (void) currentLocation;
    NES_WARNING("s2 library is needed to update predicted path");
    return false;
#endif
}

#ifdef S2DEF
std::pair<S2Point, S1Angle>
NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::findPathCoverage(const S2Polyline& path,
                                                                                   S2Point coveringNode,
                                                                                   S1Angle coverage) {
    int vertexIndex = 0;
    auto projectedPoint = path.Project(coveringNode, &vertexIndex);
    auto distanceAngle = S1Angle(coveringNode, projectedPoint);
    NES_TRACE("distance from path in meters: {}", S2Earth::ToMeters(distanceAngle));

    //if the distance is more than the coverage, it is not possible to cover the line
    if (distanceAngle > coverage) {
        NES_WARNING("no coverage possible with this node");
        return {S2Point(), S1Angle::Degrees(0)};
    }

    double divisor = cos(distanceAngle);
    if (std::isnan(divisor)) {
        NES_WARNING("divisor is NaN");
        return {S2Point(), S1Angle::Degrees(0)};
    }
    if (divisor == 0) {
        NES_WARNING("divisor is zero");
        return {S2Point(), S1Angle::Degrees(0)};
    }

    double coverageRadiansOnLine = acos(cos(coverage) / divisor);
    auto coverageAngleOnLine = S1Angle::Radians(coverageRadiansOnLine);

    auto verticeSpan = path.vertices_span();
    //the polyline always only consists of 2 points, so index 1 is its end
    S2Point coverageEnd = S2::GetPointOnLine(projectedPoint, verticeSpan[1], coverageAngleOnLine);
    return {coverageEnd, coverageAngleOnLine};
}
#endif

#ifdef S2DEF
bool NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::scheduleReconnects(
    const S2Point& currentParentLocation,
    const S2PointIndex<uint64_t>& fieldNodeIndex) {
    double remainingTime;
    reconnectPoints.clear();
    if (!trajectoryLine) {
        return false;
    }

    //find the end of path coverage of our curent parent
    auto reconnectionPointTuple =
        findPathCoverage(trajectoryLine.value(), currentParentLocation, S1Angle(defaultCoverageRadiusAngle));
    if (reconnectionPointTuple.second.degrees() == 0) {
        return false;
    }
    auto currentParentPathCoverageEnd = reconnectionPointTuple.first;

    //find the expected time of arrival at the end of coverage of our current parent
    remainingTime = S1Angle(Spatial::Util::S2Utilities::geoLocationToS2Point(locationBuffer.back().getLocation()),
                            currentParentPathCoverageEnd)
                        .degrees()
        / bufferAverageMovementSpeed;
    auto endOfCoverageETA = locationBuffer.back().getTimestamp().value() + remainingTime;

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
            auto coverageTuple = findPathCoverage(trajectoryLine.value(), result.point(), defaultCoverageRadiusAngle);
            currentUncoveredRemainingPathDistance = S1Angle(coverageTuple.first, trajectoryLine->vertices_span()[1]);

            //if the distance that remains uncovered is less then the current minimum, pick this node as the new optimal choice
            if (currentUncoveredRemainingPathDistance < minimumUncoveredRemainingPathDistance) {
                nextReconnectLocationOnPath = coverageTuple.first;
                reconnectParentId = result.data();
                minimumUncoveredRemainingPathDistance = currentUncoveredRemainingPathDistance;
                remainingTime = S1Angle(Spatial::Util::S2Utilities::geoLocationToS2Point(locationBuffer.back().getLocation()),
                                        nextReconnectLocationOnPath)
                                    .degrees()
                    / bufferAverageMovementSpeed;
                nextEstimatedReconnectTime = locationBuffer.back().getTimestamp().value() + remainingTime;
            }
        }

        //if we found a reconnect which is different from the last one on the list, add it to the vector as soon as we
        if (nextReconnectLocationOnPath.operator!=(reconnectLocationOnPath)) {
            auto currLatLng = S2LatLng(reconnectLocationOnPath);
            auto currLoc =
                std::make_shared<DataTypes::Experimental::GeoLocation>(currLatLng.lat().degrees(), currLatLng.lng().degrees());
            reconnectPoints.emplace_back(
                NES::Spatial::Mobility::Experimental::ReconnectPoint{*currLoc, reconnectParentId, estimatedReconnectTime});
            NES_DEBUG("scheduled reconnect to worker with id{}", reconnectParentId);
            reconnectLocationOnPath = nextReconnectLocationOnPath;
            estimatedReconnectTime = nextEstimatedReconnectTime;
        } else {
            NES_DEBUG("no nodes available to cover rest of path");
            break;
        }
    }
    return true;
}
#endif
