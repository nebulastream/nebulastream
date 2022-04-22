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
#include "Geolocation/WorkerGeospatialInfo.hpp"
#include "Geolocation/LocationSourceCSV.hpp"
#include "Util/Logger/Logger.hpp"
#include <GRPC/CoordinatorRPCClient.hpp>
#include <s2/s2point.h>
#include <s2/s2polyline.h>

namespace NES::Experimental::Mobility {

WorkerGeospatialInfo::WorkerGeospatialInfo(bool isMobile, GeographicalLocation fieldNodeLoc ) {
    this->isMobile = isMobile;
    this->fixedLocationCoordinates = std::make_shared<NES::Experimental::Mobility::GeographicalLocation>(fieldNodeLoc);
}

bool WorkerGeospatialInfo::createLocationSource(NES::Experimental::Mobility::LocationSource::Type type, std::string config) {
    if (config.empty()) {
        NES_FATAL_ERROR("isMobile flag is set, but there is no proper configuration for the location source. exiting");
        exit(EXIT_FAILURE);
    }

    switch (type) {
        case NES::Experimental::Mobility::LocationSource::csv:
            locationSource = std::make_shared<NES::Experimental::Mobility::LocationSourceCSV>(config);
            break;
    }

    return true;
}

void WorkerGeospatialInfo::setRPCClient(CoordinatorRPCClientPtr rpcClientPtr) {
    this->coordinatorRpcClient = rpcClientPtr;
    auto nodeList = getNodeIdsInRange(kSaveNodesRange);
    for (auto elem : nodeList) {
        auto loc = elem.second;
        //todo: set the return type of the vector to be in the sma order as the parameters of add
        fieldNodeIndex.Add(S2Point(S2LatLng::FromDegrees(loc.getLatitude(), loc.getLongitude())), elem.first);
    }
    if (isMobile) {
        updateInterval = kDefaultUpdateInterval;
        locBufferSize = kDefaultLocBufferSize;
        saveRate = kDefaultSaveRate;
        locationUpdateThread = std::make_shared<std::thread>(([this]() {
        NES_DEBUG("NesWorker: starting location updating");
        startReconnectPlanning(locBufferSize, updateInterval, saveRate);
        NES_DEBUG("NesWorker: stop updating location");
        }));
    }
}

bool WorkerGeospatialInfo::isFieldNode() { return fixedLocationCoordinates->isValid() && !isMobile; }

bool WorkerGeospatialInfo::isMobileNode() const { return isMobile; };

bool WorkerGeospatialInfo::setFixedLocationCoordinates(const NES::Experimental::Mobility::GeographicalLocation& geoLoc) {
    if (isMobile) {
        return false;
    }
    fixedLocationCoordinates = std::make_shared<NES::Experimental::Mobility::GeographicalLocation>(geoLoc);
    return true;
}

NES::Experimental::Mobility::GeographicalLocation WorkerGeospatialInfo::getGeoLoc() {
    if (isMobile) {
        if (locationSource) {
            return locationSource->getCurrentLocation().first;
        }
        //if the node is mobile, but there is no location Source, return invalid
        NES_WARNING("Node is mobile but does not have a location source");
        return {};
    }
    return *fixedLocationCoordinates;
}

std::vector<std::pair<uint64_t, NES::Experimental::Mobility::GeographicalLocation>> WorkerGeospatialInfo::getNodeIdsInRange(NES::Experimental::Mobility::GeographicalLocation coord, double radius) {
    return coordinatorRpcClient->getNodeIdsInRange(coord, radius);
}

std::vector<std::pair<uint64_t, NES::Experimental::Mobility::GeographicalLocation>> WorkerGeospatialInfo::getNodeIdsInRange(double radius) {
    auto coord = getGeoLoc();
    if (coord.isValid()) {
        return getNodeIdsInRange(coord, radius);
    }
    NES_WARNING("Trying to get the nodes in the range of a node without location");
    return {};
}

[[noreturn]] void WorkerGeospatialInfo::startReconnectPlanning(size_t locBufferSize, uint64_t updateInterval, size_t bufferSaveRate) {
    for (size_t i = 0; i < locBufferSize; ++i) {
       locationBuffer.push_back(locationSource->getCurrentLocation());
       NES_DEBUG("added: " << locationBuffer.back().first.toString() << locationBuffer.back().second);
        std::this_thread::sleep_for(std::chrono::milliseconds(updateInterval * bufferSaveRate));
    }
    NES_DEBUG("buffer save rate: " << bufferSaveRate);
    NES_TRACE("Location buffer is filled");
    size_t stepsSinceLastSave = bufferSaveRate;
    while (true) {
        auto newLoc = locationSource->getCurrentLocation();
        if (stepsSinceLastSave == bufferSaveRate) {
            auto oldLoc = locationBuffer.front();
            locationBuffer.pop_front();
            locationBuffer.push_back(newLoc);
            NES_DEBUG("added: " << newLoc.first.toString() << "; " << newLoc.second);
            NES_DEBUG("removed: " << oldLoc.first.toString() << "; " << oldLoc.second);
            stepsSinceLastSave = 0;
            //todo: does it make sense here to update since the last position in buffer or should we use the path beginning?
            updatePredictedPath(oldLoc.first, newLoc.first);
        } else {
            stepsSinceLastSave += 1;
        }
        //todo: check how far we are from las index update and download new nodes
        std::this_thread::sleep_for(std::chrono::milliseconds(updateInterval));
    }
}

void WorkerGeospatialInfo::updatePredictedPath(GeographicalLocation oldLoc, GeographicalLocation currentLoc) {
    int vertexIndex = 0;
    int* vertexIndexPtr = &vertexIndex;
    S2Point currentPoint(S2LatLng::FromDegrees(currentLoc.getLatitude(), currentLoc.getLongitude()));
    double dist = 0;
    if (trajectoryLine) {
        auto pointOnLine = trajectoryLine->Project(currentPoint, vertexIndexPtr);
        NES_DEBUG("point on line" << pointOnLine);
        dist = S1ChordAngle(currentPoint, pointOnLine).degrees();
        NES_DEBUG("dist: " << dist);
    }
    //todo: check if we are using size in the right way in the second half of hte condition
    //todo: instead of just using points, calculate centroids
    //todo: instead of updateing right away, look at if the new trajectory stabilizes itself after a turn
    if ((trajectoryLine && dist > kPathDistanceDelta) || (!trajectoryLine && locationBuffer.size() == locBufferSize)) {
        NES_DEBUG("updating trajectory");
        S2Point oldPoint(S2LatLng::FromDegrees(oldLoc.getLatitude(), oldLoc.getLongitude()));
        //todo: if we only extend by a little, we can use a chord angle. what is the performance difference?
        //S1ChordAngle angle(oldPoint, currentPoint);
        S1Angle angle(oldPoint, currentPoint);
        auto extrapolatedPoint = S2::GetPointOnLine(oldPoint, currentPoint, angle * kPathExtensionFactor);
        std::vector<S2Point> locVec;
        locVec.push_back(oldPoint);
        locVec.push_back(extrapolatedPoint);
        trajectoryLine = std::make_shared<S2Polyline>(locVec);
        pathBeginning = oldPoint;
    }
}
/*
double hav(double x) {

}
 */

std::pair<S2Point, S1Angle> WorkerGeospatialInfo::findPathCoverage(S2PolylinePtr path, S2Point coveringNode, S1Angle coverage) {
    int vertexIndex = 0;
    auto projectedPoint = path->Project(coveringNode, &vertexIndex);
    auto distAngle = S1Angle(coveringNode, projectedPoint);
    NES_DEBUG("distangle : " << distAngle.degrees())

    double coverageRadiansOnLine = acos(cos(coverage) / cos(distAngle));
    auto coverageAngleOnLine = S1Angle::Radians(coverageRadiansOnLine);

    //the polyline always only consists of 2 points, so index 1 is its end
    auto vertSpan = path->vertices_span();
    NES_DEBUG("span " << S2LatLng(vertSpan[0]) << ", " << S2LatLng(vertSpan[1]))
    NES_DEBUG("projected point" << S2LatLng(projectedPoint))
    NES_DEBUG("covering node " << S2LatLng(coveringNode))
    NES_DEBUG("coverage boundary " << S2LatLng(S2::GetPointOnLine(coveringNode, projectedPoint, coverage)))
    S2Point coverageEnd = S2::GetPointOnLine(projectedPoint, vertSpan[1], coverageAngleOnLine);
    return {coverageEnd, coverageAngleOnLine};
}
}// namespace NES::Experimental::Mobility
