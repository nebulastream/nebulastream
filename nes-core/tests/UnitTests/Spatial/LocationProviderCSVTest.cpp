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
#include <chrono>
#include <gtest/gtest.h>

namespace NES {

//todo: add negative tests
class LocationProviderCSVTest : public Testing::NESBaseTest {

  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoSourceCSV.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationProviderCSV test class.");
    }

    /* Will be called before a test is executed. */
    void SetUp() override { Testing::TestWithErrorHandling<testing::Test>::SetUp(); }

    static void TearDownTestCase() { NES_INFO("Tear down LocationProviderCSV test class."); }
};

TEST_F(LocationProviderCSVTest, testCsvMovement) {
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    auto locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);

    //Get the expected waypoints
    auto expectedWayPoints = getWaypointsFromCsv(csvPath, locationProvider->getStartTime());

    auto currentTimeStamp = getTimestamp();
    auto maxTimeStamp = expectedWayPoints.back().getTimestamp();
    std::vector<NES::Spatial::DataTypes::Experimental::Waypoint> actualWayPoints;
    while (currentTimeStamp <= maxTimeStamp) {
        auto currentWayPoint = locationProvider->getCurrentWaypoint();
        if (actualWayPoints.empty() || currentWayPoint.getLocation() != actualWayPoints.back().getLocation()) {
            actualWayPoints.emplace_back(currentWayPoint);
        }
        currentTimeStamp = getTimestamp();
    }

    EXPECT_TRUE(!actualWayPoints.empty());

    /*    auto startTime = locationProvider->getStartTime();
    //start check with 10ms sleep interval and 1ms tolerated time error
    checkDeviceMovement(csvPath, startTime, 4, getLocationFromProvider, std::static_pointer_cast<void>(locationProvider));*/
}

TEST_F(LocationProviderCSVTest, testCsvMovementWithSimulatedLocationInFuture) {
    Timestamp offset = 400000000;
    auto currTime = getTimestamp();
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    Timestamp simulatedStartTime = currTime + offset;
    auto locationProvider =
        std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath, simulatedStartTime);
    auto startTime = locationProvider->getStartTime();
    EXPECT_EQ(startTime, simulatedStartTime);
    //start check with 10ms sleep interval and 1ms tolerated time error
    //    checkDeviceMovement(csvPath, startTime, 4, getLocationFromProvider, std::static_pointer_cast<void>(locationProvider));
}

TEST_F(LocationProviderCSVTest, testCsvMovementWithSimulatedLocationInPast) {
    Timestamp offset = -100000000;
    auto currTime = getTimestamp();
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    Timestamp simulatedStartTime = currTime + offset;
    auto locationProvider =
        std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath, simulatedStartTime);
    auto startTime = locationProvider->getStartTime();
    EXPECT_EQ(startTime, simulatedStartTime);
    //start check with 10ms sleep interval and 1ms tolerated time error
    //    checkDeviceMovement(csvPath, startTime, 4, getLocationFromProvider, std::static_pointer_cast<void>(locationProvider));
}
}// namespace NES
