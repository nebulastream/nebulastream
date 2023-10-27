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
#include <Mobility/LocationProviders/LocationProviderCSV.hpp>
#include <Mobility/ReconnectSchedulePredictors/ReconnectSchedulePredictor.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Mobility/Waypoint.hpp>
#include <Util/TestUtils.hpp>
#include <gtest/gtest.h>
#include <thread>

#ifdef S2DEF
#include <s2/s2earth.h>
#include <s2/s2latlng.h>
#include <s2/s2point.h>
#include <s2/s2polyline.h>
#endif

namespace NES {

class ReconnectSchedulePredictorTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("ReconnectSchedulePredictor.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup ReconnectSchedulePredictor test class.");
    }
};

#ifdef S2DEF
TEST_F(ReconnectSchedulePredictorTest, testFindPathCoverage) {
    S2Point coveringPointOnLine;
    S1Angle coverage = S2Earth::MetersToAngle(1000);

    std::vector<S2Point> pointvec;
    //allow an absolute error of 0.01 millimeters
    S1Angle allowedError = S2Earth::MetersToAngle(0.00001);

    S2Point lineStart(S2LatLng::FromDegrees(52.621011694849, 13.27327187823881));
    S2Point lineEnd(S2LatLng::FromDegrees(52.436244793720014, 13.708629778786937));
    pointvec.push_back(lineStart);
    pointvec.push_back(lineEnd);
    S2Polyline path = S2Polyline(pointvec);

    //we chose a point on the path as a covering point. so its coverage should be equal to the supplied coverage
    coveringPointOnLine = path.Interpolate(0.5);
    std::stringstream ss;
    ss << S2LatLng(coveringPointOnLine);
    NES_DEBUG("coordinates of covering point on line: {}", ss.str());
    std::pair<S2Point, S1Angle> resultOnLinePoint =
        NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::findPathCoverage(path, coveringPointOnLine, coverage);
    NES_DEBUG("point on line coverage in meters: {}", S2Earth::ToMeters(resultOnLinePoint.second));
    EXPECT_TRUE(
        S2::ApproxEquals(resultOnLinePoint.first, S2::GetPointOnLine(coveringPointOnLine, lineEnd, coverage), allowedError));
    ASSERT_TRUE(abs(resultOnLinePoint.second - coverage) < allowedError);
    std::stringstream resultOnLinePointString;
    resultOnLinePointString << S2LatLng(resultOnLinePoint.first);
    NES_DEBUG("coverage end for point on line: {}", resultOnLinePointString.str());

    //create a point whose unit vector is orthogonal to start and end of the polyline
    auto ortoVec = S2::RobustCrossProd(lineStart, lineEnd);
    std::stringstream ortoVecString;
    ortoVecString << S2LatLng(ortoVec);
    NES_DEBUG("Orthogonal vector: {}", ortoVecString.str());
    ortoVec = ortoVec.Normalize();

    //we can use the orthogonal vector as a target to move away from the line in an angle of 90 degrees
    //we create a point which is exactly at the boundary of the coverage area and should therefore not cover anything of the line
    auto coveringPointCovAwayFromLine = S2::GetPointOnLine(coveringPointOnLine, ortoVec, coverage);
    auto resultCovaway =
        NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::findPathCoverage(path,
                                                                                           coveringPointCovAwayFromLine,
                                                                                           coverage);
    ASSERT_TRUE(resultCovaway.second.degrees() == 0);

    //test different distances from the line which are greater than zero but smaller than coverage
    for (int i = 1; i < 100; ++i) {
        double coverageFactor = 0.01 * i;
        NES_DEBUG("testing coverage of point which is coverage * {} away from path", coverageFactor);
        auto coveringPointAwayFromPath = S2::GetPointOnLine(coveringPointOnLine, ortoVec, coverage * coverageFactor);
        std::stringstream coveringPointAsString;
        coveringPointAsString << S2LatLng(coveringPointAwayFromPath);
        NES_DEBUG("covering point coordinates are: {}", coveringPointAsString.str());
        auto result =
            NES::Spatial::Mobility::Experimental::ReconnectSchedulePredictor::findPathCoverage(path,
                                                                                               coveringPointAwayFromPath,
                                                                                               coverage);
        NES_DEBUG("coverage on line in meters is: {}", S2Earth::ToMeters(result.second));
        std::stringstream s2LatLngAsString;
        s2LatLngAsString << S2LatLng(result.first);
        NES_DEBUG("coverage end coordinates are: {}", s2LatLngAsString.str());
        auto comparePoint = S2::GetPointOnLine(coveringPointOnLine, lineEnd, result.second);
        //check that the returned coverage end point is on the path and that the distance covered on line matches
        EXPECT_TRUE(S2::ApproxEquals(result.first, comparePoint, allowedError));
        //check that the returned point has the right distance to the covering node
        ASSERT_TRUE(abs(coverage - S1Angle(result.first, coveringPointAwayFromPath)) < allowedError);
    }
}
#endif
}// namespace NES
