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

#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <Exceptions/CoordinatesOutOfRangeException.hpp>
#include <Exceptions/InvalidCoordinateFormatException.hpp>
#include <Exceptions/AccessingInvalidCoordinatesException.hpp>

namespace NES {


class GeographicalLocationTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoLoc.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup GeographicalLocation test class.");
    }

    void SetUp() override {}

    static void TearDownTestCase() { NES_INFO("Tear down GeographilcalLocationUnitTest test class."); }
};

TEST_F(GeographicalLocationTest, testExceptionHandling) {
    EXPECT_THROW(GeographicalLocation(200, 0), NES::CoordinatesOutOfRangeException);
    EXPECT_THROW(GeographicalLocation::fromString("200, 0"), NES::CoordinatesOutOfRangeException);
    EXPECT_THROW(GeographicalLocation::fromString("200. 0"), NES::InvalidCoordinateFormatException);

    auto geoLoc = GeographicalLocation::fromString("23, 110");
    EXPECT_EQ(geoLoc.getLatitude(), 23);
    EXPECT_TRUE(geoLoc.isValid());
    geoLoc = GeographicalLocation(200, 200);
    EXPECT_FALSE(geoLoc.isValid());
    double l;
    EXPECT_THROW(l = geoLoc.getLatitude(), NES::AccessingInvalidCoordinatesException);
    EXPECT_THROW(l = geoLoc.getLongitude(), NES::AccessingInvalidCoordinatesException);


}
}// namespace NES
