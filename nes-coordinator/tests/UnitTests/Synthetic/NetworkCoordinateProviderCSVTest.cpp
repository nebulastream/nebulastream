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
#include <Exceptions/NetworkCoordinateProviderException.hpp>
#include <Latency/NetworkCoordinateProviders/NetworkCoordinateProviderCSV.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Latency/Waypoint.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

namespace NES {

class NetworkCoordinateProviderCSVTest : public Testing::BaseUnitTest {

  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("NetworkCoordinateProviderCSV.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NetworkCoordinateProviderCSVTest test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override {
        NES::Testing::BaseUnitTest::SetUp();
        std::vector<NES::Synthetic::DataTypes::Experimental::Waypoint> waypoints;
        waypoints.push_back({{0, 0}, 0});
        waypoints.push_back({{50, -40}, 1000000000});
        waypoints.push_back({{40, -30}, 2000000000});
        waypoints.push_back({{20, -10}, 3000000000});

        auto csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "testCoordinates.csv";
        writeNCWaypointsToCSV(csvPath, waypoints);
    }

    static void TearDownTestCase() {
        NES_INFO("Tear down NetworkCoordinateProviderCSV test class.");
        auto csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "testCoordinates.csv";
        remove(csvPath.c_str());
    }
};

TEST_F(NetworkCoordinateProviderCSVTest, testInvalidCsv) {
    //test nonexistent file
    auto csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "non_existent_file.csv";
    remove(csvPath.c_str());
    auto networkCoordinateProvider = std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = networkCoordinateProvider->getCurrentWaypoint(), NES::Synthetic::Exception::NetworkCoordinateProviderException);

    //test empty file
    csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "emptyFile.csv";
    remove(csvPath.c_str());
    std::ofstream emptyOut(csvPath);
    emptyOut.close();
    ASSERT_FALSE(emptyOut.fail());
    networkCoordinateProvider = std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = networkCoordinateProvider->getCurrentWaypoint(), NES::Synthetic::Exception::NetworkCoordinateProviderException);



    //test invalid number formats
    csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "invalidX1Format.csv";
    remove(csvPath.c_str());
    std::ofstream invalidLatFormatFile(csvPath);
    invalidLatFormatFile << "ire22, 15, 1000000000";
    invalidLatFormatFile.close();
    ASSERT_FALSE(invalidLatFormatFile.fail());
    networkCoordinateProvider = std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = networkCoordinateProvider->getCurrentWaypoint(), NES::Synthetic::Exception::NetworkCoordinateProviderException);


    csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "invalidX2Format.csv";
    remove(csvPath.c_str());
    std::ofstream invalidLngFormatFile(csvPath);
    invalidLngFormatFile << "52, g13.505980882863446, 2000000000";
    invalidLngFormatFile.close();
    ASSERT_FALSE(invalidLngFormatFile.fail());
    networkCoordinateProvider = std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = networkCoordinateProvider->getCurrentWaypoint(), NES::Synthetic::Exception::NetworkCoordinateProviderException);

    //test invalid column numbers
    csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "testLocationsNotEnoughColumns.csv";
    remove(csvPath.c_str());
    std::ofstream notEnoughColumnsFile(csvPath);
    notEnoughColumnsFile << "52, 13.0";
    notEnoughColumnsFile.close();
    ASSERT_FALSE(notEnoughColumnsFile.fail());
    networkCoordinateProvider = std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderCSV>(csvPath);
    ASSERT_THROW(auto waypoint = networkCoordinateProvider->getCurrentWaypoint(), NES::Synthetic::Exception::NetworkCoordinateProviderException);
}

TEST_F(NetworkCoordinateProviderCSVTest, testCsvMovement) {
    auto csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "testCoordinates.csv";
    auto networkCoordinateProvider = std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderCSV>(csvPath);

    //Get the expected waypoints
    auto expectedWayPoints = getNCWaypointsFromCSV(csvPath, networkCoordinateProvider->getStartTime());

    auto currentTimeStamp = getTimestamp();
    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last coordinate
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) / 2;

    std::vector<NES::Synthetic::DataTypes::Experimental::Waypoint> actualWayPoints;
    while (currentTimeStamp <= endTime) {
        auto currentWayPoint = networkCoordinateProvider->getCurrentWaypoint();
        if (actualWayPoints.empty() || currentWayPoint.getCoordinate() != actualWayPoints.back().getCoordinate()) {
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
        //only if an unexpected coordinate was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedItr, expectedWayPoints.cend());

        //because the timestamps match, the location must match as well
        EXPECT_EQ(expectedItr->getCoordinate(), actualIt->getCoordinate());
    }
}

TEST_F(NetworkCoordinateProviderCSVTest, testCsvMovementWithSimulatedLocationInFuture) {
    Timestamp offset = 400000000;
    Timestamp currentTimeStamp = getTimestamp() + offset;
    auto csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "testCoordinates.csv";
    auto networkCoordinateProvider =
        std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderCSV>(csvPath, currentTimeStamp);
    auto startTime = networkCoordinateProvider->getStartTime();
    ASSERT_EQ(startTime, currentTimeStamp);

    //Get the expected waypoints
    auto expectedWayPoints = getNCWaypointsFromCSV(csvPath, networkCoordinateProvider->getStartTime());
    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last coordinate
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) / 2;

    std::vector<NES::Synthetic::DataTypes::Experimental::Waypoint> actualWayPoints;
    auto initialExpectedWaypoint = expectedWayPoints.front();
    while (currentTimeStamp <= endTime) {
        auto currentWayPoint = networkCoordinateProvider->getCurrentWaypoint();
        if (actualWayPoints.empty() || currentWayPoint.getCoordinate() != actualWayPoints.back().getCoordinate()) {
            actualWayPoints.emplace_back(currentWayPoint);
        }

        //if the movement has not started yet, expect initial coordinate
        if (currentWayPoint.getTimestamp().value() <= initialExpectedWaypoint.getTimestamp().value()) {
            EXPECT_EQ(initialExpectedWaypoint.getCoordinate(), currentWayPoint.getCoordinate());
        }
        currentTimeStamp = getTimestamp() + offset;
    }

    ASSERT_TRUE(!actualWayPoints.empty());

    //check the ordering of the existing waypoints
    auto expectedItr = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {

        //skip comparing initial location as it might have differing timestamp
        if (actualIt->getCoordinate() == expectedWayPoints.front().getCoordinate()) {
            continue;
        }

        while (expectedItr != expectedWayPoints.cend()
               && expectedItr->getTimestamp().value() != actualIt->getTimestamp().value()) {
            expectedItr++;
        }
        //only if an unexpected coordinate was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedItr, expectedWayPoints.cend());

        EXPECT_EQ(expectedItr->getCoordinate(), actualIt->getCoordinate());
    }
}

TEST_F(NetworkCoordinateProviderCSVTest, testCsvMovementWithSimulatedLocationInPast) {
    Timestamp offset = -100000000000;
    Timestamp currentTimeStamp = getTimestamp() + offset;
    auto csvPath = std::filesystem::path(TEST_DATA_DIRECTORY) / "testCoordinates.csv";
    auto networkCoordinateProvider =
        std::make_shared<NES::Synthetic::Latency::Experimental::NetworkCoordinateProviderCSV>(csvPath, currentTimeStamp);
    auto startTime = networkCoordinateProvider->getStartTime();
    ASSERT_EQ(startTime, currentTimeStamp);

    //Get the expected waypoints
    auto expectedWayPoints = getNCWaypointsFromCSV(csvPath, networkCoordinateProvider->getStartTime());

    auto maxTimeStamp = expectedWayPoints.back().getTimestamp().value();

    //set endtime to 1.5 times the duration until the last timestamp so we are more likely to catch the last location
    auto endTime = maxTimeStamp + (maxTimeStamp - currentTimeStamp) / 2;

    std::vector<NES::Synthetic::DataTypes::Experimental::Waypoint> actualWayPoints;
    while (currentTimeStamp <= endTime) {
        auto currentWayPoint = networkCoordinateProvider->getCurrentWaypoint();
        if (actualWayPoints.empty() || currentWayPoint.getCoordinate() != actualWayPoints.back().getCoordinate()) {
            actualWayPoints.emplace_back(currentWayPoint);
        }
        currentTimeStamp = getTimestamp() + offset;
    }

    ASSERT_TRUE(!actualWayPoints.empty());

    //check the ordering of the existing waypoints
    auto expectedItr = expectedWayPoints.cbegin();
    for (auto actualIt = actualWayPoints.cbegin(); actualIt != actualWayPoints.cend(); ++actualIt) {
        while (expectedItr != expectedWayPoints.cend()
               && expectedItr->getTimestamp().value() != actualIt->getTimestamp().value()) {
            expectedItr++;
        }
        //only if an unexpected coordinate was observed the iterator could have reached the end of the list of expected waypoints
        EXPECT_NE(expectedItr, expectedWayPoints.cend());

        //because the timestamps match, the location must match as well
        EXPECT_EQ(expectedItr->getCoordinate(), actualIt->getCoordinate());
    }
}

}// namespace NES
