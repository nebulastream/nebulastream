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

#include <Exceptions/NetworkCoordinateProviderException.hpp>
#include <Latency/NetworkCoordinateProviders/NetworkCoordinateProviderCSV.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Latency/NetworkCoordinate.hpp>
#include <Util/Latency/Waypoint.hpp>
#include <Util/TimeMeasurement.hpp>
#include <fstream>

namespace NES::Synthetic::Latency::Experimental {

NetworkCoordinateProviderCSV::NetworkCoordinateProviderCSV(const std::string& csvPath) : NetworkCoordinateProviderCSV(csvPath, 0) {}

NetworkCoordinateProviderCSV::NetworkCoordinateProviderCSV(const std::string& csvPath, Timestamp simulatedStartTime)
    : NetworkCoordinateProvider(Synthetic::Experimental::SyntheticType::NC_ENABLED, {}), csvPath(csvPath) {
    if (simulatedStartTime == 0) {
        startTime = getTimestamp();
    } else {
        startTime = simulatedStartTime;
    }
}

void NetworkCoordinateProviderCSV::loadNetworkCoordinateSimulationDataFromCsv() {
    std::string csvLine;
    std::ifstream inputStream(csvPath);
    std::string x1String;
    std::string x2String;
    std::string timeString;
    std::basic_string<char> delimiter = {','};

    NES_DEBUG("Started csv network coordinate source at {}", startTime)

    //read network coordinates and time offsets from csv, calculate absolute timestamps from offsets by adding start time
    while (std::getline(inputStream, csvLine)) {
        std::stringstream stringStream(csvLine);
        std::vector<std::string> values;
        try {
            values = NES::Util::splitWithStringDelimiter<std::string>(csvLine, delimiter);
        } catch (std::exception& exception) {
            std::string errorString = std::string("An error occurred while splitting delimiter of waypoint CSV. ERROR: ")
                + strerror(errno) + " message=" + exception.what();
            NES_ERROR("NetworkCoordinateProviderCSV:  {}", errorString);
            throw Synthetic::Exception::NetworkCoordinateProviderException(errorString);
        }
        //TODO: The size of the value will be dynamic in the future, since the number of dimensions can be more than 2
        if (values.size() != 3) {
            std::string errorString =
                std::string("NetworkCoordinateProviderCSV: could not read waypoints from csv, expected 3 columns but input file has ")
                + std::to_string(values.size()) + std::string(" columns");
            NES_ERROR("NetworkCoordinateProviderCSV:  {}", errorString);
            throw Synthetic::Exception::NetworkCoordinateProviderException(errorString);
        }
        x1String = values[0];
        x2String = values[1];
        timeString = values[2];

        Timestamp time;
        double x1;
        double x2;
        try {
            time = std::stoul(timeString);
            x1 = std::stod(x1String);
            x2 = std::stod(x2String);
        } catch (std::exception& exception) {
            std::string errorString = std::string("An error occurred while creating the waypoint. ERROR: ") + strerror(errno)
                + " message=" + exception.what();
            NES_ERROR("NetworkCoordinateProviderCSV: {}", errorString);
            throw Synthetic::Exception::NetworkCoordinateProviderException(errorString);
        }
        NES_TRACE("Read from csv: {}, {}, {}", x1String, x2String, time);

        //add startTime to the offset obtained from csv to get absolute timestamp
        time += startTime;

        //construct a pair containing a coordinate and the time at which the device is at exactly that point
        // and add it to a vector containing all waypoints
        auto waypoint = DataTypes::Experimental::Waypoint(DataTypes::Experimental::NetworkCoordinate(x1, x2), time);
        waypoints.push_back(waypoint);
    }
    if (waypoints.empty()) {
        auto errorString = std::string("No data in CSV, cannot start network coordinate provider");
        NES_ERROR("NetworkCoordinateProviderCSV: {}", errorString);
        throw Synthetic::Exception::NetworkCoordinateProviderException(errorString);
    }
    NES_DEBUG("read {} waypoints from csv", waypoints.size());
    NES_DEBUG("first timestamp is {}, last timestamp is {}",
              waypoints.front().getTimestamp().value(),
              waypoints.back().getTimestamp().value());
    //set first csv entry as the next waypoint
}

DataTypes::Experimental::Waypoint NetworkCoordinateProviderCSV::getCurrentWaypoint() {
    if (waypoints.empty()) {
        loadNetworkCoordinateSimulationDataFromCsv();
    }
    //get the time the request is made so we can compare it to the timestamps in the list of waypoints
    Timestamp requestTime = getTimestamp();

    //find the waypoint with the smallest timestamp greater than requestTime
    //this point is the next waypoint on the way ahead of us
    while (nextWaypointIndex < waypoints.size() && getWaypointAt(nextWaypointIndex).getTimestamp().value() < requestTime) {
        nextWaypointIndex++;
    }

    //Check if next waypoint is still initialized as 0.
    //Set the current waypoint as the first network coordinate in the csv file
    if (nextWaypointIndex == 0) {
        return {getWaypointAt(nextWaypointIndex).getCoordinate(), requestTime};
    }

    //find the last point behind us on the way
    auto currentWaypointIndex = nextWaypointIndex - 1;
    auto currentWayPoint = getWaypointAt(currentWaypointIndex);
    NES_TRACE("Network Coordinate: {}; Time:{}", currentWayPoint.getCoordinate().toString(), currentWayPoint.getTimestamp().value());
    return currentWayPoint;
}

Timestamp NetworkCoordinateProviderCSV::getStartTime() const { return startTime; }

DataTypes::Experimental::Waypoint NetworkCoordinateProviderCSV::getWaypointAt(size_t index) { return waypoints.at(index); }
}// namespace NES::Synthetic::Latency::Experimental
