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

#include <NesBaseTest.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Mobility/LocationProviders/LocationProviderCSV.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

namespace NES {

class LocationProviderCSVTest : public Testing::NESBaseTest {

  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoSourceCSV.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationProviderCSV test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { NES::Testing::NESBaseTest::SetUp(); }

    static void TearDownTestCase() { NES_INFO("Tear down LocationProviderCSV test class."); }
};

TEST_F(LocationProviderCSVTest, testInvalidCsv) {
    //test nonexistent file
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "non_existent_file.csv";
    auto locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), Exceptions::RuntimeException);

    //test empty file
    csvPath = std::string(TEST_DATA_DIRECTORY) + "emptyFile.csv";
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), Exceptions::RuntimeException);

    //test invalid number formats
    csvPath = std::string(TEST_DATA_DIRECTORY) + "invalidLatitudeFormat.csv";
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), Exceptions::RuntimeException);
    csvPath = std::string(TEST_DATA_DIRECTORY) + "invalidLongitudeFormat.csv";
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), Exceptions::RuntimeException);
    csvPath = std::string(TEST_DATA_DIRECTORY) + "invalidOffsetFormat.csv";
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), Exceptions::RuntimeException);

    //test invalid column numbers
    csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocationsNotEnoughColumns.csv";
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), Exceptions::RuntimeException);
}


TEST_F(LocationProviderCSVTest, testCsvMovement) {
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    auto locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);

    //Get the expected waypoints
    auto expectedWayPoints = getWaypointsFromCsv(csvPath, locationProvider->getStartTime());

    auto currentTimeStamp = getTimestamp();
    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last location
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) / 2;

    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> actualWayPoints;
    while (currentTimeStamp <= endTime) {
        auto currentWayPoint = locationProvider->getCurrentWaypoint();
        if (actualWayPoints.empty() || currentWayPoint.getLocation() != actualWayPoints.back().getLocation()) {
            actualWayPoints.emplace_back(currentWayPoint);
        }
        currentTimeStamp = getTimestamp();
    }

    ASSERT_TRUE(!actualWayPoints.empty());

    //check the ordering of the existing waypoints
    auto expectedIt = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {
        while (expectedIt != expectedWayPoints.cend() && expectedIt->getTimestamp().value() != actualIt->getTimestamp().value()) {
            expectedIt++;
        }
        NES_DEBUG("comparing actual waypoint " << std::distance(actualWayPoints.cbegin(), actualIt) <<
                  " to expected waypoint " << std::distance(expectedWayPoints.cbegin(), expectedIt));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedIt, expectedWayPoints.cend());

        //because the timestamps match, the location must match as well
        EXPECT_EQ(expectedIt->getLocation(), actualIt->getLocation()) ;
    }
}

TEST_F(LocationProviderCSVTest, testCsvMovementWithSimulatedLocationInFuture) {
    Timestamp offset = 400000000;
    auto currentTimeStamp = getTimestamp();
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    Timestamp simulatedStartTime = currentTimeStamp + offset;
    auto locationProvider =
        std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath, simulatedStartTime);
    auto startTime = locationProvider->getStartTime();
    ASSERT_EQ(startTime, simulatedStartTime);

    //Get the expected waypoints
    auto expectedWayPoints = getWaypointsFromCsv(csvPath, locationProvider->getStartTime());
    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last location
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) / 2;

    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> actualWayPoints;
    auto initialExpectedWaypoint = expectedWayPoints.front();
    while (currentTimeStamp <= endTime) {
        auto currentWayPoint = locationProvider->getCurrentWaypoint();
        if (actualWayPoints.empty() || currentWayPoint.getLocation() != actualWayPoints.back().getLocation()) {
            actualWayPoints.emplace_back(currentWayPoint);
        }

        //if the movement has not started yet, expect initial location
        if (currentWayPoint.getTimestamp().value() <= initialExpectedWaypoint.getTimestamp().value()) {
            NES_DEBUG("comparing initial location");
            EXPECT_EQ(initialExpectedWaypoint.getLocation(), currentWayPoint.getLocation());
        }
        currentTimeStamp = getTimestamp();
    }

    ASSERT_TRUE(!actualWayPoints.empty());

    //check the ordering of the existing waypoints
    auto expectedIt = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {

        //skip comparing initial location as it might have differing timestamp
        if (actualIt->getLocation() == expectedWayPoints.front().getLocation()) {
            continue;
        }

        while (expectedIt != expectedWayPoints.cend() && expectedIt->getTimestamp().value() != actualIt->getTimestamp().value()) {
            expectedIt++;
        }
        NES_DEBUG("comparing actual waypoint " << std::distance(actualWayPoints.cbegin(), actualIt) <<
                                               " to expected waypoint " << std::distance(expectedWayPoints.cbegin(), expectedIt));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedIt, expectedWayPoints.cend());

        //because the timestamps match, the location must match as well
        EXPECT_EQ(expectedIt->getLocation(), actualIt->getLocation()) ;
    }
}

TEST_F(LocationProviderCSVTest, testCsvMovementWithSimulatedLocationInPast) {
    Timestamp offset = -100000000;
    auto currentTimeStamp = getTimestamp();
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    Timestamp simulatedStartTime = currentTimeStamp + offset;
    auto locationProvider =
        std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath, simulatedStartTime);
    auto startTime = locationProvider->getStartTime();
    ASSERT_EQ(startTime, simulatedStartTime);


    //Get the expected waypoints
    auto expectedWayPoints = getWaypointsFromCsv(csvPath, locationProvider->getStartTime());

    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last location
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) / 2;

    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> actualWayPoints;
    while (currentTimeStamp <= endTime) {
        auto currentWayPoint = locationProvider->getCurrentWaypoint();
        if (actualWayPoints.empty() || currentWayPoint.getLocation() != actualWayPoints.back().getLocation()) {
            actualWayPoints.emplace_back(currentWayPoint);
        }
        currentTimeStamp = getTimestamp();
    }

    ASSERT_TRUE(!actualWayPoints.empty());

    //check the ordering of the existing waypoints
    auto expectedIt = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {
        while (expectedIt != expectedWayPoints.cend() && expectedIt->getTimestamp().value() != actualIt->getTimestamp().value()) {
            expectedIt++;
        }
        NES_DEBUG("comparing actual waypoint " << std::distance(actualWayPoints.cbegin(), actualIt) <<
                                               " to expected waypoint " << std::distance(expectedWayPoints.cbegin(), expectedIt));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedIt, expectedWayPoints.cend());

        //because the timestamps match, the location must match as well
        EXPECT_EQ(expectedIt->getLocation(), actualIt->getLocation()) ;
    }
}
}// namespace NES
