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
    using Waypoint = std::pair<NES::Spatial::Index::Experimental::LocationPtr, Timestamp>;

  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoSourceCSV.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationProviderCSV test class.");
    }

    //wrapper function so allow the generic utility function to call the mamber function of LocationProvider
    static std::shared_ptr<NES::Spatial::Index::Experimental::Waypoint> getLocationFromProvider(std::shared_ptr<void> provider) {
        auto casted = std::static_pointer_cast<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(provider);
        return casted->getCurrentWaypoint();
    }

    static void TearDownTestCase() { NES_INFO("Tear down LocationProviderCSV test class."); }
};

TEST_F(LocationProviderCSVTest, testCsvMovement) {
    auto csvPath = std::string(TEST_DATA_DIRECTORY) + "testLocations.csv";
    auto locationProvider = std::make_shared<NES::Spatial::Mobility::Experimental::LocationProviderCSV>(csvPath);
    auto startTime = locationProvider->getStartTime();
    //start check with 10ms sleep interval and 1ms tolerated time error
    checkDeviceMovement(csvPath, startTime, 4, getLocationFromProvider, std::static_pointer_cast<void>(locationProvider));
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
    checkDeviceMovement(csvPath, startTime, 4, getLocationFromProvider, std::static_pointer_cast<void>(locationProvider));
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
    checkDeviceMovement(csvPath, startTime, 4, getLocationFromProvider, std::static_pointer_cast<void>(locationProvider));
}
}// namespace NES
