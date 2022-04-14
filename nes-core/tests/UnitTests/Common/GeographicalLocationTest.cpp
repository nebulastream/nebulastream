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

#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <Exceptions/InvalidCoordinateFormatException.hpp>
#include <NesBaseTest.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>

namespace NES {

class GeographicalLocationTest : public Testing::TestWithErrorHandling<testing::Test> {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoLoc.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GeographicalLocation test class.");
    }

    static void TearDownTestCase() { NES_INFO("Tear down GeographilcalLocationUnitTest test class."); }
};

TEST_F(GeographicalLocationTest, testExceptionHandling) {
    EXPECT_THROW(NES::Experimental::Mobility::GeographicalLocation(200, 0),
                 NES::Experimental::Mobility::CoordinatesOutOfRangeException);
    EXPECT_THROW(NES::Experimental::Mobility::GeographicalLocation(200, 200),
                 NES::Experimental::Mobility::CoordinatesOutOfRangeException);
    EXPECT_THROW(NES::Experimental::Mobility::GeographicalLocation::fromString("200, 0"),
                 NES::Experimental::Mobility::CoordinatesOutOfRangeException);
    EXPECT_THROW(NES::Experimental::Mobility::GeographicalLocation::fromString("200. 0"),
                 NES::Experimental::Mobility::InvalidCoordinateFormatException);

    auto geoLoc = NES::Experimental::Mobility::GeographicalLocation::fromString("23, 110");
    EXPECT_EQ(geoLoc.getLatitude(), 23);
    EXPECT_EQ(geoLoc.getLongitude(), 110);
    EXPECT_TRUE(geoLoc.isValid());
    auto invalidGeoLoc1 = NES::Experimental::Mobility::GeographicalLocation();
    auto invalidGeoLoc2 = NES::Experimental::Mobility::GeographicalLocation();
    EXPECT_FALSE(invalidGeoLoc1.isValid());
    EXPECT_TRUE(std::isnan(invalidGeoLoc1.getLatitude()));
    EXPECT_TRUE(std::isnan(invalidGeoLoc1.getLongitude()));

    EXPECT_EQ(invalidGeoLoc1, invalidGeoLoc2);
    EXPECT_NE(geoLoc, invalidGeoLoc1);
}
}// namespace NES
