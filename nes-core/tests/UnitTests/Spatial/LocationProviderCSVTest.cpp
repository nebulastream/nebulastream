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
#include <thread>

namespace NES {

class LocationProviderCSVTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoSourceCSV.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationProviderCSV test class.");
    }

    void SetUp() override {}

    static void TearDownTestCase() { NES_INFO("Tear down LocationProviderCSV test class."); }
};

TEST_F(LocationProviderCSVTest, testCsvMovement) {
    //todo: make location provider csv be mobile spatial type by default?
    NES::Spatial::Mobility::Experimental::LocationProviderCSV sourceCsv(std::string(TEST_DATA_DIRECTORY) + "testLocations.csv");
    auto startTime = sourceCsv.getStarttime();
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
            EXPECT_LE(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_LE(currentTuple.first->getLongitude(), pos2lng);
        } else if (beforeQuery > timesecloc && afterQuery <= timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos2lng);
            EXPECT_LE(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_LE(currentTuple.first->getLongitude(), pos3lng);
        } else if (beforeQuery > timethirdloc && afterQuery <= timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos3lng);
            EXPECT_LE(currentTuple.first->getLatitude(), pos4lat);
            EXPECT_LE(currentTuple.first->getLongitude(), pos4lng);
        } else if (beforeQuery > timefourthloc) {
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

TEST_F(LocationProviderCSVTest, testCsvMovementWithSimulatedLocationInFuture) {
    Timestamp offset = 400000000;
    auto currTime = getTimestamp();
    Timestamp simulatedStartTime = currTime + offset;
    NES::Spatial::Mobility::Experimental::LocationProviderCSV sourceCsv(std::string(TEST_DATA_DIRECTORY) + "testLocations.csv", simulatedStartTime);
    auto startTime = sourceCsv.getStarttime();
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
            EXPECT_LE(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_LE(currentTuple.first->getLongitude(), pos2lng);
        } else if (beforeQuery > timesecloc && afterQuery <= timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos2lng);
            EXPECT_LE(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_LE(currentTuple.first->getLongitude(), pos3lng);
        } else if (beforeQuery > timethirdloc && afterQuery <= timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos3lng);
            EXPECT_LE(currentTuple.first->getLatitude(), pos4lat);
            EXPECT_LE(currentTuple.first->getLongitude(), pos4lng);
        } else if (beforeQuery > timefourthloc) {
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
    NES::Spatial::Mobility::Experimental::LocationProviderCSV sourceCsv(std::string(TEST_DATA_DIRECTORY) + "testLocations.csv", simulatedStartTime);
    auto startTime = sourceCsv.getStarttime();
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
            EXPECT_LE(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_LE(currentTuple.first->getLongitude(), pos2lng);
        } else if (beforeQuery > timesecloc && afterQuery <= timethirdloc) {
            NES_DEBUG("checking second loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos2lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos2lng);
            EXPECT_LE(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_LE(currentTuple.first->getLongitude(), pos3lng);
        } else if (beforeQuery > timethirdloc && afterQuery <= timefourthloc) {
            NES_DEBUG("checking third loc")
            EXPECT_GE(currentTuple.first->getLatitude(), pos3lat);
            EXPECT_GE(currentTuple.first->getLongitude(), pos3lng);
            EXPECT_LE(currentTuple.first->getLatitude(), pos4lat);
            EXPECT_LE(currentTuple.first->getLongitude(), pos4lng);
        } else if (beforeQuery > timefourthloc) {
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
