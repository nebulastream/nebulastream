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

#include <BaseIntegrationTest.hpp>
#include <Exceptions/LocationProviderException.hpp>
#include <Spatial/DataTypes/Waypoint.hpp>
#include <Spatial/Mobility/LocationProviders/LocationProviderCSV.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

namespace NES {

class LocationProviderCSVTest : public Testing::BaseUnitTest {

  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoSourceCSV.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationProviderCSV test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Testing::BaseUnitTest::SetUp();
        std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> waypoints;
        waypoints.push_back({{52.55227464714949, 13.351743136322877}, 0});
        waypoints.push_back({{2.574709862890394, 13.419206057808077}, 1000000000});
        waypoints.push_back({{2.61756571840606, 13.505980882863446}, 2000000000});
        waypoints.push_back({{2.67219559419452, 13.591124924963108}, 3000000000});

        auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
        writeWaypointsToCsv(csvPath, waypoints);
    }

    static void TearDownTestCase() {
        NES_INFO("Tear down LocationProviderCSV test class.");
        auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
        remove(csvPath.c_str());
    }
};

TEST_F(LocationProviderCSVTest, testInvalidCsv) {
    //test nonexistent file
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "non_existent_file.csv";
    remove(csvPath.c_str());
    auto locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), NES::Spatial::Exception::LocationProviderException);

    //test empty file
    csvPath = std::string(TEST_DATA_DIRECTORY) + "emptyFile.csv";
    remove(csvPath.c_str());
    std::ofstream emptyOut(csvPath);
    emptyOut.close();
    ASSERT_FALSE(emptyOut.fail());
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), NES::Spatial::Exception::LocationProviderException);

    //test invalid number formats
    csvPath = std::string(TEST_DATA_DIRECTORY) + "invalidLatitudeFormat.csv";
    remove(csvPath.c_str());
    std::ofstream invalidLatFormatFile(csvPath);
    invalidLatFormatFile << "ire22, 13.419206057808077, 1000000000";
    invalidLatFormatFile.close();
    ASSERT_FALSE(invalidLatFormatFile.fail());
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), NES::Spatial::Exception::LocationProviderException);

    csvPath = std::string(TEST_DATA_DIRECTORY) + "invalidLongitudeFormat.csv";
    remove(csvPath.c_str());
    std::ofstream invalidLngFormatFile(csvPath);
    invalidLngFormatFile << "52.61756571840606, g13.505980882863446, 2000000000";
    invalidLngFormatFile.close();
    ASSERT_FALSE(invalidLngFormatFile.fail());
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), NES::Spatial::Exception::LocationProviderException);

    csvPath = std::string(TEST_DATA_DIRECTORY) + "invalidOffsetFormat.csv";
    remove(csvPath.c_str());
    std::ofstream invalidOffsetFormatFile(csvPath);
    invalidOffsetFormatFile << "52.55227464714949, 13.351743136322877, h0";
    invalidOffsetFormatFile.close();
    ASSERT_FALSE(invalidOffsetFormatFile.fail());
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), NES::Spatial::Exception::LocationProviderException);

    //test invalid column numbers
    csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocationsNotEnoughColumns.csv";
    remove(csvPath.c_str());
    std::ofstream notEnoughColumnsFile(csvPath);
    notEnoughColumnsFile << "52.574709862890394, 13.0";
    notEnoughColumnsFile.close();
    ASSERT_FALSE(notEnoughColumnsFile.fail());
    locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = locationProvider->getCurrentWaypoint(), NES::Spatial::Exception::LocationProviderException);
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
    auto expectedItr = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {
        while (expectedItr != expectedWayPoints.cend()
               && expectedItr->getTimestamp().value() != actualIt->getTimestamp().value()) {
            expectedItr++;
        }
        NES_DEBUG("comparing actual waypoint {} to expected waypoint {}",
                  std::distance(actualWayPoints.cbegin(), actualIt),
                  std::distance(expectedWayPoints.cbegin(), expectedItr));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedItr, expectedWayPoints.cend());

        //because the timestamps match, the location must match as well
        EXPECT_EQ(expectedItr->getLocation(), actualIt->getLocation());
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
    auto expectedItr = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {

        //skip comparing initial location as it might have differing timestamp
        if (actualIt->getLocation() == expectedWayPoints.front().getLocation()) {
            continue;
        }

        while (expectedItr != expectedWayPoints.cend()
               && expectedItr->getTimestamp().value() != actualIt->getTimestamp().value()) {
            expectedItr++;
        }
        NES_DEBUG("comparing actual waypoint {} to expected waypoint {}",
                  std::distance(actualWayPoints.cbegin(), actualIt),
                  std::distance(expectedWayPoints.cbegin(), expectedItr));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedItr, expectedWayPoints.cend());

        //because the timestamps match, the location must match as well
        EXPECT_EQ(expectedItr->getLocation(), actualIt->getLocation());
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
    auto expectedItr = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {
        while (expectedItr != expectedWayPoints.cend()
               && expectedItr->getTimestamp().value() != actualIt->getTimestamp().value()) {
            expectedItr++;
        }
        NES_DEBUG("comparing actual waypoint {} to expected waypoint {}",
                  std::distance(actualWayPoints.cbegin(), actualIt),
                  std::distance(expectedWayPoints.cbegin(), expectedItr));
        //only if an unexpected location was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedItr, expectedWayPoints.cend());

        //because the timestamps match, the location must match as well
        EXPECT_EQ(expectedItr->getLocation(), actualIt->getLocation());
    }
}
}// namespace NES
