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
#include <Spatial/Index/Waypoint.hpp>
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
        auto waypoint = std::make_shared<Index::Experimental::Waypoint>(
            Index::Experimental::Location(std::stod(latitudeString), std::stod(longitudeString)),
            time);
        waypoints.push_back(waypoint);
    }
    if (waypoints.empty()) {
        NES_WARNING("No data in CSV, cannot start location provider");
        exit(EXIT_FAILURE);
    }
    NES_DEBUG("read " << waypoints.size() << " waypoints from csv");
    NES_DEBUG("first timestamp is " << waypoints.front()->getTimestamp().value() << ", last timestamp is "
                                    << waypoints.back()->getTimestamp().value())
    //set first csv entry as the next wypoint
}

Index::Experimental::WaypointPtr LocationProviderCSV::getCurrentWaypoint() {
    //get the time the request is made so we can compare it to the timestamps in the list of waypoints
    Timestamp requestTime = getTimestamp();

    //find the waypoint with the smallest timestamp greater than requestTime
    //this point is the next waypoint on the way ahead of us
    while (nextWaypoint < waypoints.size() && getWaypointAt(nextWaypoint)->getTimestamp().value() < requestTime) {
        nextWaypoint++;
    }

    //if the first waypoint is still in the future, simulate the device to be resting at that position until the specified timestamp
    if (nextWaypoint == 0) {
        return std::make_shared<Index::Experimental::Waypoint>(*getWaypointAt(nextWaypoint)->getLocation(), requestTime);
    }

    //find the last point behind us on the way
    auto prevWaypoint = nextWaypoint - 1;

    NES_TRACE("Location: " << getWaypointAt(prevWaypoint)->getLocation()->toString()
                           << "; Time: " << getWaypointAt(prevWaypoint)->getTimestamp().value())
    return getWaypointAt(prevWaypoint);
}

Timestamp LocationProviderCSV::getStartTime() const { return startTime; }

const std::vector<Index::Experimental::WaypointPtr>& LocationProviderCSV::getWaypoints() const { return waypoints; }
Index::Experimental::WaypointPtr LocationProviderCSV::getWaypointAt(size_t index) { return waypoints.at(index); }
}// namespace NES::Spatial::Mobility::Experimental
