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
#include <Spatial/LocationIndex.hpp>
#include <Spatial/LocationProvider.hpp>
#include <Spatial/LocationProviderCSV.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <s2/s2earth.h>
#include <s2/s2point.h>
#include <s2/s2polyline.h>
#include <thread>

namespace NES {

class LocationIndexTests : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoSourceCSV.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationSourceCSV test class.");
    }

    void SetUp() override {}

    static void TearDownTestCase() { NES_INFO("Tear down LocationSourceCSV test class."); }
};

TEST_F(LocationIndexTests, testCoverageCalculation) {
    S2Point lineStart(S2LatLng::FromDegrees(52.0, 13.0));
    S2Point lineEnd(S2LatLng::FromDegrees(52.0, 13.03));
    std::vector<S2Point> v;
    v.push_back(lineStart);
    v.push_back(lineEnd);
    S2PolylinePtr path = std::make_shared<S2Polyline>(v);
    S2Point node(S2LatLng::FromDegrees(52.002, 13.007));
    S1Angle coverage = S1Angle::Degrees(0.003);

    /*
    NES_DEBUG(S2Earth::MetersToAngle(333.6).degrees())
    NES_DEBUG(S2Earth::ToMeters(coverage))

    auto radius = S2Earth::RadiusKm();
    NES_DEBUG(S1Angle::Radians(0.3336 / radius).degrees())
     */


    auto res = NES::Spatial::Mobility::Experimental::LocationProvider::findPathCoverage(path, node, coverage);

    NES_DEBUG("Reconnect Point " << S2LatLng(res.first));
    NES_DEBUG("distance Reconnect Point <-> Covering Node " << S1Angle(node, res.first).degrees());
    NES_DEBUG("Distance on line" << res.second.degrees());

    //NES_DEBUG(S2LatLng(S2::GetPointOnLine(lineStart, lineEnd, - (coverage * 2))))

    /*
    NES_DEBUG("turn from linestart to line end")
    NES_DEBUG(S2LatLng(S2::GetPointOnLine(lineStart, lineEnd, coverage)));

    S2Point eqStart(S2LatLng::FromDegrees(11.6493340841473, 15.163683204706832));
    S2Point eqEnd(S2LatLng::FromDegrees(8.465375637111197, 15.323786711462892));
    NES_DEBUG(S2LatLng(S2::GetPointOnLine(eqStart, eqEnd, coverage)));
    NES_DEBUG(S2LatLng(S2::GetPointOnLine(eqStart, eqEnd, coverage * 2)));
     */


}
}