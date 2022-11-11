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
#include <gtest/gtest.h>

#include <Spatial/Mobility/LocationProviderCSV.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/TimeMeasurement.hpp>
#include <chrono>
#include <ranges>
#include <thread>

namespace NES {

class LocationProviderCSVTest : public testing::Test {
  using Waypoint = std::pair<NES::Spatial::Index::Experimental::LocationPtr, Timestamp>;
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoSourceCSV.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationProviderCSV test class.");
    }

    void SetUp() override {}

    void checkMovement(std::string csvPath, Timestamp startTime, size_t timesToCheckEndLocation, Timestamp sleepTime, Timestamp timeError, std::shared_ptr<NES::Spatial::Mobility::Experimental::LocationProviderCSV> locationProvider) {
        std::vector<Waypoint> waypoints = getWaypointsFromCsv(csvPath, startTime);
        NES_DEBUG("Read " << waypoints.size() << " waypoints from csv");

        //set the end time
        auto loopTime = waypoints.back().second + timeError;

        Timestamp beforeQuery = getTimestamp();
        //std::pair currentTuple = locationProvider.getCurrentLocation();
        auto [currentDeviceLocation, currentDeviceTime] = locationProvider->getCurrentLocation();
        Timestamp afterQuery = getTimestamp();

        NES::Spatial::Index::Experimental::Location lastWaypoint;

        while (afterQuery < loopTime) {
            NES_TRACE("Device is at location: " << currentDeviceLocation->toString());
            for (auto it = waypoints.cbegin(); it != waypoints.end(); ++it) {
                auto [waypointLocation, waypointTime] = *it;
                if (afterQuery < waypointTime - timeError) {
                    if (it == waypoints.cbegin()) {
                        NES_DEBUG("Movement has not started yet. Checking if device is in initial position");
                        EXPECT_EQ(waypointLocation, currentDeviceLocation);
                        break;
                    }
                    auto [previousWaypointLocation, previousWaypointTime] = *std::prev(it);
                    if (beforeQuery > previousWaypointTime + timeError) {
                        auto waypointNumber = std::distance(waypoints.cbegin(), it);
                        NES_DEBUG("run checks for path from waypoint " <<  waypointNumber - 1 << " to " << waypointNumber)
                        auto deviceLat = currentDeviceLocation->getLatitude();
                        auto deviceLng = currentDeviceLocation->getLongitude();
                        auto prevLat = previousWaypointLocation->getLatitude();
                        auto prevLng = previousWaypointLocation->getLongitude();
                        auto nextLat = waypointLocation->getLatitude();
                        auto nextLng = waypointLocation->getLongitude();

                        EXPECT_TRUE((prevLat <= deviceLat && deviceLat < nextLat) ||
                                    prevLat >= deviceLat && deviceLat > nextLat);
                        EXPECT_TRUE((prevLng <= deviceLng && deviceLng < nextLng) ||
                                    prevLng >= deviceLng && deviceLng > nextLng);
                        break;
                    }
                    if (it == waypoints.cend() && afterQuery > waypointTime + timeError) {
                        NES_DEBUG("Device has reached its final position");
                        EXPECT_EQ(waypointLocation, currentDeviceLocation);
                        break;
                    }
                }
                std::this_thread::sleep_for(std::chrono::nanoseconds(sleepTime));
                beforeQuery = getTimestamp();
                auto currentTuple = locationProvider->getCurrentLocation();
                currentDeviceLocation = currentTuple.first;
                currentDeviceTime = currentTuple.second;
                afterQuery = getTimestamp();
            }
        }

        auto currentTuple = locationProvider->getCurrentLocation();
        currentDeviceLocation = currentTuple.first;
        auto endPosition = waypoints.back().first;
        for (size_t i = 0; i < timesToCheckEndLocation; ++i) {
            NES_DEBUG("checking if device remains in end position, check " << i + 1 << " out of " << timesToCheckEndLocation);
            EXPECT_EQ(*currentDeviceLocation, *endPosition);
            std::this_thread::sleep_for(std::chrono::nanoseconds(sleepTime));
            auto currentTuple = locationProvider->getCurrentLocation();
            currentDeviceLocation = currentTuple.first;
        }
    }

    std::vector<Waypoint> getWaypointsFromCsv(const std::string& csvPath, Timestamp startTime) {
        std::vector<Waypoint> waypoints;
        std::string csvLine;
        std::ifstream inputStream(csvPath);
        std::string latitudeString;
        std::string longitudeString;
        std::string timeString;

        NES_DEBUG("Creating list of waypoints with startTime" << startTime)

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
            std::pair waypoint(std::make_shared<NES::Spatial::Index::Experimental::Location>(
                                   NES::Spatial::Index::Experimental::Location(std::stod(latitudeString), std::stod(longitudeString))),
                               time);
            waypoints.push_back(waypoint);
        }
        return waypoints;
    }



    static void TearDownTestCase() { NES_INFO("Tear down LocationProviderCSV test class."); }
};

TEST_F(LocationProviderCSVTest, testCsvMovement) {
    NES::Logger::getInstance()->setLogLevel(NES::LogLevel::LOG_TRACE);
    //todo: make location provider csv be mobile spatial type by default?
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    //NES::Spatial::Mobility::Experimental::LocationProviderCSV sourceCsv(std::string(TEST_DATA_DIRECTORY) + "testLocations.csv");
    auto sourceCsv = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(std::string(TEST_DATA_DIRECTORY) + "testLocations.csv");
    auto startTime = sourceCsv->getStartTime();
    //start check with 10ms sleep interval and 1ms tolerated time error
    checkMovement(csvPath, startTime, 4, 10000000, 1000000, sourceCsv);
    /*
    auto timefirstLoc = startTime;
    auto timesecloc = startTime + 100000000;
    auto timethirdloc = startTime + 200000000;
    auto timefourthloc = startTime + 300000000;
    auto loopTime = startTime + 400000000;

    double pos1lat = 52.55227464714949;
    double pos1lng = 13.351743136322877;

    double pos2lat = 52.574709862890394;
    double pos2lng = 13.419206057808077;

    double pos3lat = 52.61756571840606;
    double pos3lng = 13.505980882863446;

    double pos4lat = 52.67219559419452;
    double pos4lng = 13.591124924963108;

    Timestamp beforeQuery = getTimestamp();
    std::pair currentTuple = sourceCsv.getCurrentLocation();
    Timestamp afterQuery = getTimestamp();

#ifdef S2DEF
    while (afterQuery < loopTime) {
        if (afterQuery < timesecloc) {
            NES_DEBUG("checking first loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos1lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos1lng);
            EXPECT_LT(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_LT(currentTuple.first->getLongitude(), pos2lng);
        } else if (beforeQuery >= timesecloc && afterQuery < timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos2lng);
            EXPECT_LT(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_LT(currentTuple.first->getLongitude(), pos3lng);
        } else if (beforeQuery >= timethirdloc && afterQuery < timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos3lng);
            EXPECT_LT(currentTuple.first->getLatitude(), pos4lat);
            EXPECT_LT(currentTuple.first->getLongitude(), pos4lng);
        } else if (beforeQuery >= timefourthloc) {
            NES_DEBUG("checking fourth loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos4lat, pos4lng));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        beforeQuery = getTimestamp();
        currentTuple = sourceCsv.getCurrentLocation();
        afterQuery = getTimestamp();
    }

#else
    while (afterQuery < loopTime) {
        if (afterQuery < timesecloc) {
            NES_DEBUG("checking first loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos1lat, pos1lng));
        } else if (beforeQuery > timesecloc && afterQuery <= timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos2lat, pos2lng));
        } else if (beforeQuery > timethirdloc && afterQuery <= timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos3lat, pos3lng));
        } else if (beforeQuery > timefourthloc) {
            NES_DEBUG("checking fourth loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos4lat, pos4lng));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        beforeQuery = getTimestamp();
        currentTuple = sourceCsv.getCurrentLocation();
        afterQuery = getTimestamp();
    }
#endif
     */
}

TEST_F(LocationProviderCSVTest, testCsvMovementWithSimulatedLocationInFuture) {
    Timestamp offset = 400000000;
    auto currTime = getTimestamp();
    Timestamp simulatedStartTime = currTime + offset;
    NES::Spatial::Mobility::Experimental::LocationProviderCSV sourceCsv(std::string(TEST_DATA_DIRECTORY) + "testLocations.csv",
                                                                        simulatedStartTime);
    auto startTime = sourceCsv.getStartTime();
    EXPECT_EQ(startTime, simulatedStartTime);
    auto timefirstLoc = startTime;
    auto timesecloc = startTime + 100000000;
    auto timethirdloc = startTime + 200000000;
    auto timefourthloc = startTime + 300000000;
    auto loopTime = startTime + 400000000;

    double pos1lat = 52.55227464714949;
    double pos1lng = 13.351743136322877;

    double pos2lat = 52.574709862890394;
    double pos2lng = 13.419206057808077;

    double pos3lat = 52.61756571840606;
    double pos3lng = 13.505980882863446;

    double pos4lat = 52.67219559419452;
    double pos4lng = 13.591124924963108;

    Timestamp beforeQuery = getTimestamp();
    std::pair currentTuple = sourceCsv.getCurrentLocation();
    Timestamp afterQuery = getTimestamp();

#ifdef S2DEF
    while (afterQuery < loopTime) {
        if (afterQuery < timesecloc) {
            NES_DEBUG("checking first loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos1lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos1lng);
            EXPECT_LT(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_LT(currentTuple.first->getLongitude(), pos2lng);
        } else if (beforeQuery >= timesecloc && afterQuery < timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos2lng);
            EXPECT_LT(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_LT(currentTuple.first->getLongitude(), pos3lng);
        } else if (beforeQuery >= timethirdloc && afterQuery < timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos3lng);
            EXPECT_LT(currentTuple.first->getLatitude(), pos4lat);
            EXPECT_LT(currentTuple.first->getLongitude(), pos4lng);
        } else if (beforeQuery >= timefourthloc) {
            NES_DEBUG("checking fourth loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos4lat, pos4lng));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        beforeQuery = getTimestamp();
        currentTuple = sourceCsv.getCurrentLocation();
        afterQuery = getTimestamp();
    }

#else
    while (afterQuery < loopTime) {
        if (afterQuery < timesecloc) {
            NES_DEBUG("checking first loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos1lat, pos1lng));
        } else if (beforeQuery > timesecloc && afterQuery <= timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos2lat, pos2lng));
        } else if (beforeQuery > timethirdloc && afterQuery <= timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos3lat, pos3lng));
        } else if (beforeQuery > timefourthloc) {
            NES_DEBUG("checking fourth loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos4lat, pos4lng));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        beforeQuery = getTimestamp();
        currentTuple = sourceCsv.getCurrentLocation();
        afterQuery = getTimestamp();
    }
#endif
}

TEST_F(LocationProviderCSVTest, testCsvMovementWithSimulatedLocationInPast) {
    Timestamp offset = -100000000;
    auto currTime = getTimestamp();
    Timestamp simulatedStartTime = currTime + offset;
    NES::Spatial::Mobility::Experimental::LocationProviderCSV sourceCsv(std::string(TEST_DATA_DIRECTORY) + "testLocations.csv",
                                                                        simulatedStartTime);
    auto startTime = sourceCsv.getStartTime();
    EXPECT_EQ(startTime, simulatedStartTime);
    auto timefirstLoc = startTime;
    auto timesecloc = startTime + 100000000;
    auto timethirdloc = startTime + 200000000;
    auto timefourthloc = startTime + 300000000;
    auto loopTime = startTime + 400000000;

    double pos1lat = 52.55227464714949;
    double pos1lng = 13.351743136322877;

    double pos2lat = 52.574709862890394;
    double pos2lng = 13.419206057808077;

    double pos3lat = 52.61756571840606;
    double pos3lng = 13.505980882863446;

    double pos4lat = 52.67219559419452;
    double pos4lng = 13.591124924963108;

    Timestamp beforeQuery = getTimestamp();
    std::pair currentTuple = sourceCsv.getCurrentLocation();
    Timestamp afterQuery = getTimestamp();

#ifdef S2DEF
    while (afterQuery < loopTime) {
        if (afterQuery < timesecloc) {
            NES_DEBUG("checking first loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos1lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos1lng);
            EXPECT_LT(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_LT(currentTuple.first->getLongitude(), pos2lng);
        } else if (beforeQuery >= timesecloc && afterQuery < timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos2lng);
            EXPECT_LT(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_LT(currentTuple.first->getLongitude(), pos3lng);
        } else if (beforeQuery >= timethirdloc && afterQuery < timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos3lng);
            EXPECT_LT(currentTuple.first->getLatitude(), pos4lat);
            EXPECT_LT(currentTuple.first->getLongitude(), pos4lng);
        } else if (beforeQuery >= timefourthloc) {
            NES_DEBUG("checking fourth loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos4lat, pos4lng));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        beforeQuery = getTimestamp();
        currentTuple = sourceCsv.getCurrentLocation();
        afterQuery = getTimestamp();
    }

#else
    while (afterQuery < loopTime) {
        if (afterQuery < timesecloc) {
            NES_DEBUG("checking first loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos1lat, pos1lng));
        } else if (beforeQuery > timesecloc && afterQuery <= timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos2lat, pos2lng));
        } else if (beforeQuery > timethirdloc && afterQuery <= timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos3lat, pos3lng));
        } else if (beforeQuery > timefourthloc) {
            NES_DEBUG("checking fourth loc")
            EXPECT_EQ(*(currentTuple.first), NES::Spatial::Index::Experimental::Location(pos4lat, pos4lng));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        beforeQuery = getTimestamp();
        currentTuple = sourceCsv.getCurrentLocation();
        afterQuery = getTimestamp();
    }
#endif
}
}// namespace NES
