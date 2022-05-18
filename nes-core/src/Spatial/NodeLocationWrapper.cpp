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
#include <GRPC/CoordinatorRPCClient.hpp>
#include <Spatial/LocationProviderCSV.hpp>
#include <Spatial/NodeLocationWrapper.hpp>
#include <Util/Logger/Logger.hpp>
#include <Spatial/ReconnectSchedule.hpp>
#include <Configurations/Worker/WorkerMobilityConfiguration.hpp>

#include <s2/s2point.h>
#include <s2/s2polyline.h>
#include <s2/s2earth.h>

namespace NES::Spatial::Mobility::Experimental {

NodeLocationWrapper::NodeLocationWrapper(bool isMobile, Index::Experimental::Location fieldNodeLoc) {
    this->isMobile = isMobile;
    this->fixedLocationCoordinates = std::make_shared<Index::Experimental::Location>(fieldNodeLoc);
}

NodeLocationWrapper::NodeLocationWrapper(bool isMobile, Index::Experimental::Location fieldNodeLoc,
                                         NES::Configurations::Spatial::Mobility::Experimental::WorkerMobilityConfigurationPtr configuration) :
NodeLocationWrapper(isMobile, fieldNodeLoc) {
    //(void) configuration;
    //todo: make the names match to avoid confusion
    pathUpdateInterval = configuration->pathPredictionUpdateInterval.getValue();
    locBufferSize = configuration->locationBufferSize.getValue();
    saveRate = configuration->saveRate.getValue();
    deltaAngle = S2Earth::MetersToChordAngle(configuration->pathDistanceDelta.getValue());
    nodeDownloadRadius = configuration->nodeDownloadRadius.getValue();
    nodeIndexUpdateThreshold = configuration->nodeIndexUpdateThreshold.getValue();
    defaultCoverageAngle = S2Earth::MetersToChordAngle(configuration->defaultCoverageRadius.getValue());
    predictedPathLengthAngle = S2Earth::MetersToChordAngle(configuration->pathPredictionLength);
}

bool NodeLocationWrapper::createLocationProvider(LocationProviderType type, std::string config) {
    if (config.empty()) {
        NES_FATAL_ERROR("isMobile flag is set, but there is no proper configuration for the location source. exiting");
        exit(EXIT_FAILURE);
    }

    switch (type) {
        case LocationProviderType::NONE:
            NES_FATAL_ERROR("Trying to create location provider but provider type is set to NONE")
            exit(EXIT_FAILURE);
        case LocationProviderType::CSV: locationProvider = std::make_shared<LocationProviderCSV>(config); break;
        case LocationProviderType::INVALID:
            NES_FATAL_ERROR("Trying to create location provider but provider type is invalid")
            exit(EXIT_FAILURE);
    }

    return true;
}

void NodeLocationWrapper::setUpReconnectPlanning() {
    //todo: get these values from config instead of const values:
    //deltaAngle = S1ChordAngle(kPathDistanceDeltaMeters);

    auto nodeList = getNodeIdsInRange(nodeDownloadRadius);
    for (auto elem : nodeList) {
        auto loc = elem.second;
        //todo: set the return type of the vector to be in the sma order as the parameters of add
        fieldNodeIndex.Add(S2Point(S2LatLng::FromDegrees(loc.getLatitude(), loc.getLongitude())), elem.first);
    }
    if (isMobile) {
        locationUpdateThread = std::make_shared<std::thread>(&NodeLocationWrapper::startReconnectPlanning, this);
    }
}

bool NodeLocationWrapper::isFieldNode() { return fixedLocationCoordinates->isValid() && !isMobile; }

bool NodeLocationWrapper::isMobileNode() const { return isMobile; };

bool NodeLocationWrapper::setFixedLocationCoordinates(const Index::Experimental::Location& geoLoc) {
    if (isMobile) {
        return false;
    }
    fixedLocationCoordinates = std::make_shared<Index::Experimental::Location>(geoLoc);
    return true;
}

Index::Experimental::LocationPtr NodeLocationWrapper::getLocation() {
    if (isMobile) {
        if (locationProvider) {
            NES_DEBUG("Node location wrapper returning mobile node location: "
                      << locationProvider->getCurrentLocation().first->getLatitude() << ", "
                      << locationProvider->getCurrentLocation().first->getLongitude())
            return locationProvider->getCurrentLocation().first;
        }
        //if the node is mobile, but there is no location Source, return invalid
        NES_WARNING("Node is mobile but does not have a location source");
        return std::make_shared<Spatial::Index::Experimental::Location>();
    }
    return fixedLocationCoordinates;
}

Mobility::Experimental::ReconnectSchedulePtr NodeLocationWrapper::getReconnectSchedule() {
    S2LatLng startLatLng(pathBeginning);
    auto start = std::make_shared<Index::Experimental::Location>(startLatLng.lat().degrees(), startLatLng.lng().degrees());
    S2LatLng endLatLng(pathEnd);
    auto end = std::make_shared<Index::Experimental::Location>(endLatLng.lat().degrees(), endLatLng.lng().degrees());
    return std::make_shared<Mobility::Experimental::ReconnectSchedule>(start, end, reconnectVector);
}

std::vector<std::pair<uint64_t, Index::Experimental::Location>>
NodeLocationWrapper::getNodeIdsInRange(Index::Experimental::Location coord, double radius) {
    return coordinatorRpcClient->getNodeIdsInRange(coord, radius);
}

std::vector<std::pair<uint64_t, Index::Experimental::Location>> NodeLocationWrapper::getNodeIdsInRange(double radius) {
    auto coord = getLocation();
    if (coord->isValid()) {
        return getNodeIdsInRange(*coord, radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}

[[noreturn]] void NodeLocationWrapper::startReconnectPlanning() {
    for (size_t i = 0; i < locBufferSize; ++i) {
        //todo: probably this if condition can be removed later
        if (locationProvider != nullptr) {
            locationBuffer.push_back(locationProvider->getCurrentLocation());
            NES_DEBUG("added: " << locationBuffer.back().first->toString() << locationBuffer.back().second);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(pathUpdateInterval * saveRate));
    }
    NES_DEBUG("buffer save rate: " << saveRate);
    NES_TRACE("Location buffer is filled");
    size_t stepsSinceLastSave = saveRate;
    while (true) {
        auto newLoc = locationProvider->getCurrentLocation();
        if (stepsSinceLastSave == saveRate) {
            auto oldLoc = locationBuffer.front();
            locationBuffer.pop_front();
            locationBuffer.push_back(newLoc);
            NES_DEBUG("added: " << newLoc.first->toString() << "; " << newLoc.second);
            NES_DEBUG("removed: " << oldLoc.first->toString() << "; " << oldLoc.second);
            stepsSinceLastSave = 0;
            //todo: does it make sense here to update since the last position in buffer or should we use the path beginning?
            updatePredictedPath(oldLoc.first, newLoc.first);
        } else {
            stepsSinceLastSave += 1;
        }
        //todo: check how far we are from las index update and download new nodes
        std::this_thread::sleep_for(std::chrono::milliseconds(pathUpdateInterval));
    }
}

void NodeLocationWrapper::updatePredictedPath(Spatial::Index::Experimental::LocationPtr oldLoc, Spatial::Index::Experimental::LocationPtr currentLoc) {
    int vertexIndex = 0;
    int* vertexIndexPtr = &vertexIndex;
    S2Point currentPoint(S2LatLng::FromDegrees(currentLoc->getLatitude(), currentLoc->getLongitude()));
    S1ChordAngle distAngle = S2Earth::MetersToChordAngle(0);
    if (trajectoryLine) {
        auto pointOnLine = trajectoryLine->Project(currentPoint, vertexIndexPtr);
        NES_DEBUG("point on line" << pointOnLine);
        distAngle = S1ChordAngle(currentPoint, pointOnLine);
        NES_DEBUG("dist: " << S2Earth::ToMeters(distAngle));
    }
    //todo: check if we are using size in the right way in the second half of hte condition
    //todo: instead of just using points, calculate centroids
    //todo: instead of updateing right away, look at if the new trajectory stabilizes itself after a turn

    if ((trajectoryLine && distAngle > deltaAngle) || (!trajectoryLine && locationBuffer.size() == locBufferSize)) {
        NES_DEBUG("updating trajectory");
        S2Point oldPoint(S2LatLng::FromDegrees(oldLoc->getLatitude(), oldLoc->getLongitude()));
        S1Angle angle(oldPoint, currentPoint);
        auto extrapolatedPoint = S2::GetPointOnLine(oldPoint, currentPoint, predictedPathLengthAngle);
        std::vector<S2Point> locVec;
        locVec.push_back(oldPoint);
        locVec.push_back(extrapolatedPoint);
        trajectoryLine = std::make_shared<S2Polyline>(locVec);
        pathBeginning = oldPoint;
        pathEnd = extrapolatedPoint;
    }
}

std::pair<S2Point, S1Angle> NodeLocationWrapper::findPathCoverage(S2PolylinePtr path, S2Point coveringNode, S1Angle coverage) {
    int vertexIndex = 0;
    auto projectedPoint = path->Project(coveringNode, &vertexIndex);
    auto distAngle = S1Angle(coveringNode, projectedPoint);

    double coverageRadiansOnLine = acos(cos(coverage) / cos(distAngle));
    auto coverageAngleOnLine = S1Angle::Radians(coverageRadiansOnLine);

    //the polyline always only consists of 2 points, so index 1 is its end
    auto vertSpan = path->vertices_span();
    NES_DEBUG("projected point" << S2LatLng(projectedPoint))
    S2Point coverageEnd = S2::GetPointOnLine(projectedPoint, vertSpan[1], coverageAngleOnLine);
    return {coverageEnd, coverageAngleOnLine};
}

void NodeLocationWrapper::scheduleReconnects(const NES::Spatial::Index::Experimental::LocationPtr& currentLoc) {
    S2Point currentPoint(S2LatLng::FromDegrees(currentLoc->getLatitude(), currentLoc->getLongitude()));
    //todo: there is a mistek in the thinking for the following line: we cannot measure the full coverage from our current loc
    //instead we need to use find path coverage
    //S2Point nextReconnectPoint = S2::GetPointOnLine(currentPoint, pathEnd, S1Angle::Degrees(kDefaultCoverageMeters));

    double distToReconnect = 0;
    /*
    //todo: what did i mean with this condition?
    while (distToReconnect < kPathLengthMeters) {
        //todo make a cap around next reconnect and divide it in half
        //todo: take all nodes within this cap

    }
     */
}

NES::Spatial::Mobility::Experimental::LocationProviderPtr NodeLocationWrapper::getLocationProvider() {
    return locationProvider;
}

void NodeLocationWrapper::setCoordinatorRPCCLient(CoordinatorRPCClientPtr coordinatorClient) {
    coordinatorRpcClient = coordinatorClient;
}
}// namespace NES::Spatial::Mobility::Experimental
