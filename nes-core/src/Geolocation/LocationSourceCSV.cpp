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
#include <Geolocation/LocationSourceCSV.hpp>
#include <iostream>
#include <fstream>
#include <Util/TimeMeasurement.hpp>
#include <Util/Logger/Logger.hpp>
#ifdef S2DEF
#include <s2/s2point.h>
#include <s2/s2polyline.h>
#endif

namespace NES {

LocationSourceCSV::LocationSourceCSV(std::string csvPath) {
    std::string csvLine;
    std::ifstream inputStream(csvPath);
    std::string locString;
    std::string timeString;

    startTime = getTimestamp();

    //read locations and time offsets from csv
    while (std::getline(inputStream, csvLine)) {
        std::stringstream stringStream(csvLine);
            getline(stringStream, locString, ';');
            getline(stringStream, timeString, ';');
            Timestamp time = std::stoul(timeString);
            NES_DEBUG("Read from csv: " << locString << ", " << time);

            //add startTime to the offset obtained from csv to get absolute timestamp
            time += startTime;

            std::pair waypoint(GeographicalLocation::fromString(locString), time);
            waypoints.push_back(waypoint);
        }
        //set first csv entry as next waypoint
        nextWaypoint = waypoints.begin();
}

std::pair<GeographicalLocation, Timestamp> LocationSourceCSV::getLastKnownLocation() {
    Timestamp requestTime = getTimestamp();

    //find the waypoint with the lowest timestamp greater than requestTime
    while (nextWaypoint->second < requestTime && nextWaypoint != waypoints.end()) {
        nextWaypoint = std::next(nextWaypoint);
    }
    auto prevWaypoint = std::prev(nextWaypoint);

#ifdef S2DEF
    //if we already reached the last position in the file, our location will fixed at that point for all future requests
    if (nextWaypoint == waypoints.end()) {
        NES_DEBUG("Last waypoint reached, do not interpolate, node will stay in this position for the rest of the simulation");
        return {prevWaypoint->first, requestTime};
    }

    //we interpolate the position where the device would be at requestTime if it moved with unchanging velocity between the 2 points
    S2Point prev(S2LatLng::FromDegrees(prevWaypoint->first.getLatitude(), prevWaypoint->first.getLongitude()));
    S2Point post(S2LatLng::FromDegrees(nextWaypoint->first.getLatitude(), nextWaypoint->first.getLongitude()));
    std::vector<S2Point> pointVec;
    pointVec.push_back(prev);
    pointVec.push_back(post);

    S2Polyline path(pointVec);

    Timestamp requestOffset = requestTime - prevWaypoint->second;
    Timestamp nextPointOffset = nextWaypoint->second - prevWaypoint->second;
    double fraction = (double) requestOffset / (double) nextPointOffset;

    S2LatLng resultS2(path.Interpolate(fraction));
    GeographicalLocation result(resultS2.lat().degrees(), resultS2.lng().degrees());

    NES_DEBUG("Retrieving s2-interpolated location");
    NES_DEBUG("Location: " << result.toString() << "; Time: " << prevWaypoint->second)

    return {result, requestTime};
#else
    NES_DEBUG("S2 not used, returning most recently passed waypoint from csv")
    NES_DEBUG("Location: " << prevWaypoint->first.toString() << "; Time: " << prevWaypoint->second)
    return *prevWaypoint;
#endif
}

Timestamp LocationSourceCSV::getStarttime() const {
    return startTime;
}

}
