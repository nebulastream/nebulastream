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
#include <Spatial/Index/Location.hpp>
#include <Spatial/Mobility/LocationProviderCSV.hpp>
#include <Util/Experimental/S2Utilities.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TimeMeasurement.hpp>
#include <fstream>
#include <iostream>
#ifdef S2DEF
#include <s2/s2point.h>
#include <s2/s2polyline.h>
#endif

namespace NES::Spatial::Mobility::Experimental {

LocationProviderCSV::LocationProviderCSV(const std::string& csvPath) : LocationProviderCSV(csvPath, 0) {}

LocationProviderCSV::LocationProviderCSV(const std::string& csvPath, Timestamp simulatedStartTime)
    : LocationProvider(Index::Experimental::NodeType::MOBILE_NODE, {}) {
    if (simulatedStartTime == 0) {
        startTime = getTimestamp();
    } else {
        startTime = simulatedStartTime;
    }
    readMovementSimulationDataFromCsv(csvPath);
}

void LocationProviderCSV::readMovementSimulationDataFromCsv(const std::string& csvPath) {
    std::string csvLine;
    std::ifstream inputStream(csvPath);
    std::string latitudeString;
    std::string longitudeString;
    std::string timeString;
    nextWaypoint = 0;

    NES_DEBUG("Started csv location source at " << startTime)

    //read locations and time offsets from csv, calculate absolute timestamps from offsets by adding start time
    while (std::getline(inputStream, csvLine)) {
        std::stringstream stringStream(csvLine);
        getline(stringStream, latitudeString, ',');
        getline(stringStream, longitudeString, ',');
        getline(stringStream, timeString, ',');
        Timestamp time = std::stoul(timeString);
        NES_TRACE("Read from csv: " << latitudeString << ", " << longitudeString << ", " << time);

        //add startTime to the offset obtained from csv to get absolute timestamp
        time += startTime;

        //construct a pair containing a location and the time at which the device is at exactly that point
        // and sve it to a vector containing all waypoints
        std::pair waypoint(std::make_shared<Index::Experimental::Location>(
                               Index::Experimental::Location(std::stod(latitudeString), std::stod(longitudeString))),
                           time);
        waypoints.push_back(waypoint);
    }
    if (waypoints.empty()) {
        NES_WARNING("No data in CSV, cannot start location provider");
        exit(EXIT_FAILURE);
    }
    NES_DEBUG("read " << waypoints.size() << " waypoints from csv");
    NES_DEBUG("first timestamp is " << waypoints.front().second << ", last timestamp is " << waypoints.back().second)
    //set first csv entry as the next wypoint
}

std::pair<Index::Experimental::LocationPtr, Timestamp> LocationProviderCSV::getCurrentLocation() {
    //get the time the request is made so we can compare it to the timestamps in the list of waypoints
    Timestamp requestTime = getTimestamp();

    //find the waypoint with the smallest timestamp greater than requestTime
    //this point is the next waypoint on the way ahead of us
    while (nextWaypoint < waypoints.size() && getWaypointAt(nextWaypoint).second < requestTime) {
        nextWaypoint++;
    }

    //if the first waypoint is still in the future, simulate the device to be resting at that position until the specified timestamp
    if (nextWaypoint == 0) {
        return {getWaypointAt(nextWaypoint).first, requestTime};
    }

    //find the last point behind us on the way
    auto prevWaypoint = nextWaypoint - 1;

#ifdef S2DEF
    //if we already reached the last position in the file, we assume that the device stays in that location and does not move further
    //we therefore keep returning the location of the last timestamp, without looking at the request time
    if (nextWaypoint == waypoints.size()) {
        NES_DEBUG("Last waypoint reached, do not interpolate, node will stay in this position for the rest of the simulation");
        return {getWaypointAt(prevWaypoint).first, requestTime};
    }

    if (*getWaypointAt(prevWaypoint).first == *getWaypointAt(nextWaypoint).first) {
        return {getWaypointAt(prevWaypoint).first, requestTime};
    }
    //if we have not reached the final position yet, we draw the path between the last waypoint we passed and the next waypoint ahead of us
    //as an s2 polyline
    S2Point prev = Util::S2Utilities::locationToS2Point(*getWaypointAt(prevWaypoint).first);
    S2Point post = Util::S2Utilities::locationToS2Point(*getWaypointAt(nextWaypoint).first);
    S2Polyline path({prev, post});

    //calculate the time that passed since visiting the last waypoint
    Timestamp requestOffset = requestTime - getWaypointAt(prevWaypoint).second;
    //calculate the complete travel time between that last visited waypoint and the waypoint ahead of us
    Timestamp nextPointOffset = getWaypointAt(nextWaypoint).second - getWaypointAt(prevWaypoint).second;
    //we want to simulate the device to be traveling at constant speed between prevWaypoint and nextWaypoint
    //and calculate the fraction of the complete distance between the points which was already traveled at requestTime
    double fraction = (double) requestOffset / (double) nextPointOffset;

    //we use the fraction to interpolate the point on path where the device is located if it
    //travels at constant speed from prevWaypoint to nextWaypoint
    S2LatLng resultS2(path.Interpolate(fraction));
    auto result = Index::Experimental::Location(resultS2.lat().degrees(), resultS2.lng().degrees());

    NES_TRACE("Retrieving s2-interpolated location");
    NES_TRACE("Location: " << result.toString() << "; Time: " << getWaypointAt(prevWaypoint).second)

    return {std::make_shared<Index::Experimental::Location>(result), requestTime};
#else
    //if the s2 library is not available we return the time and place of the previous waypoint as our last known position.
    NES_TRACE("S2 not used, returning most recently passed waypoint from csv")
    NES_TRACE("Location: " << getWaypointAt(prevWaypoint).first->toString() << "; Time: " << getWaypointAt(prevWaypoint).second)
    return *prevWaypoint;
#endif
}

Timestamp LocationProviderCSV::getStartTime() const { return startTime; }

const std::vector<std::pair<Index::Experimental::LocationPtr, Timestamp>>& LocationProviderCSV::getWaypoints() const {
    return waypoints;
}
Waypoint LocationProviderCSV::getWaypointAt(size_t index) {
    return waypoints.at(index);
}
}// namespace NES::Spatial::Mobility::Experimental
