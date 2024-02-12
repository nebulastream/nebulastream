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

#include <Exceptions/LocationProviderException.hpp>
#include <Mobility/LocationProviders/LocationProviderCSV.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/GeoLocation.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/TimeMeasurement.hpp>
#include <fstream>

namespace NES::Spatial::Mobility::Experimental {

LocationProviderCSV::LocationProviderCSV(const std::string& csvPath) : LocationProviderCSV(csvPath, 0) {}

LocationProviderCSV::LocationProviderCSV(const std::string& csvPath, Timestamp simulatedStartTime)
    : LocationProvider(Spatial::Experimental::SpatialType::MOBILE_NODE, {}), nextWaypointIndex(0), csvPath(std::move(csvPath)) {
    if (simulatedStartTime == 0) {
        startTime = getTimestamp();
    } else {
        startTime = simulatedStartTime;
    }
}

void LocationProviderCSV::loadMovementSimulationDataFromCsv() {
    std::string csvLine;
    std::ifstream inputStream(csvPath);
    std::string latitudeString;
    std::string longitudeString;
    std::string timeString;
    std::basic_string<char> delimiter = {','};

    NES_DEBUG("Started csv location source at {}", startTime)

    //read locations and time offsets from csv, calculate absolute timestamps from offsets by adding start time
    while (std::getline(inputStream, csvLine)) {
        std::stringstream stringStream(csvLine);
        std::vector<std::string> values;
        try {
            values = NES::Util::splitWithStringDelimiter<std::string>(csvLine, delimiter);
        } catch (std::exception& e) {
            std::string errorString =
                std::string("An error occurred while splitting delimiter of waypoint CSV. ERROR: ") + strerror(errno) +  " exception=" + e.what();
            NES_ERROR("LocationProviderCSV:  {}", errorString);
            throw Spatial::Exception::LocationProviderException(errorString);
        }
        if (values.size() != 3) {
            std::string errorString =
                std::string("LocationProviderCSV: could not read waypoints from csv, expected 3 columns but input file has ")
                + std::to_string(values.size()) + std::string(" columns");
            NES_ERROR("LocationProviderCSV:  {}", errorString);
            throw Spatial::Exception::LocationProviderException(errorString);
        }
        latitudeString = values[0];
        longitudeString = values[1];
        timeString = values[2];

        Timestamp time;
        double latitude;
        double longitude;
        try {
            time = std::stoul(timeString);
            latitude = std::stod(latitudeString);
            longitude = std::stod(longitudeString);
        } catch (std::exception& e) {
            std::string errorString = std::string("An error occurred while creating the waypoint. ERROR: ") + strerror(errno) + " exception=" + e.what();
            NES_ERROR("LocationProviderCSV: {}", errorString);
            throw Spatial::Exception::LocationProviderException(errorString);
        }
        NES_TRACE("Read from csv: {}, {}, {}", latitudeString, longitudeString, time);

        //add startTime to the offset obtained from csv to get absolute timestamp
        time += startTime;

        //construct a pair containing a location and the time at which the device is at exactly that point
        // and add it to a vector containing all waypoints
        auto waypoint = DataTypes::Experimental::Waypoint(DataTypes::Experimental::GeoLocation(latitude, longitude), time);
        waypoints.push_back(waypoint);
    }
    if (waypoints.empty()) {
        auto errorString = std::string("No data in CSV, cannot start location provider");
        NES_ERROR("LocationProviderCSV: {}", errorString);
        throw Spatial::Exception::LocationProviderException(errorString);
    }
    NES_DEBUG("read {} waypoints from csv", waypoints.size());
    NES_DEBUG("first timestamp is {}, last timestamp is {}",
              waypoints.front().getTimestamp().value(),
              waypoints.back().getTimestamp().value());
    //set first csv entry as the next waypoint
}

DataTypes::Experimental::Waypoint LocationProviderCSV::getCurrentWaypoint() {
    if (waypoints.empty()) {
        loadMovementSimulationDataFromCsv();
    }
    //get the time the request is made so we can compare it to the timestamps in the list of waypoints
    Timestamp requestTime = getTimestamp();

    //find the waypoint with the smallest timestamp greater than requestTime
    //this point is the next waypoint on the way ahead of us
    while (nextWaypointIndex < waypoints.size() && getWaypointAt(nextWaypointIndex).getTimestamp().value() < requestTime) {
        nextWaypointIndex++;
    }

    //Check if next waypoint is still initialized as 0.
    //Set the current waypoint as the first location in the csv file
    if (nextWaypointIndex == 0) {
        return {getWaypointAt(nextWaypointIndex).getLocation(), requestTime};
    }

    //find the last point behind us on the way
    auto currentWaypointIndex = nextWaypointIndex - 1;
    auto currentWayPoint = getWaypointAt(currentWaypointIndex);
    NES_TRACE("Location: {}; Time:{}", currentWayPoint.getLocation().toString(), currentWayPoint.getTimestamp().value());
    return currentWayPoint;
}

Timestamp LocationProviderCSV::getStartTime() const { return startTime; }

DataTypes::Experimental::Waypoint LocationProviderCSV::getWaypointAt(size_t index) { return waypoints.at(index); }
}// namespace NES::Spatial::Mobility::Experimental
