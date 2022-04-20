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
#include <Geolocation/LocationSourceCSV.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <chrono>
#include <gtest/gtest.h>
#include <thread>
#include <s2/s2point.h>
#include <s2/s2polyline.h>
#include <Geolocation/WorkerGeospatialInfo.hpp>

namespace NES {

class LocationSourceCSVTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("GeoSourceCSV.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup LocationSourceCSV test class.");
    }

    void SetUp() override {}

    static void TearDownTestCase() { NES_INFO("Tear down LocationSourceCSV test class."); }
};

TEST_F(LocationSourceCSVTest, testCoverageClaculation) {
    S2Point lineStart(S2LatLng::FromDegrees(52.0, 13.0));
    S2Point lineEnd(S2LatLng::FromDegrees(52.0, 13.01));
    std::vector<S2Point> v;
    v.push_back(lineStart);
    v.push_back(lineEnd);
    S2PolylinePtr path = std::make_shared<S2Polyline>(v);
    NES_DEBUG("interpol " << S2LatLng(path->Interpolate(0.5)))
    S2Point node(S2LatLng::FromDegrees(52.05, 13.005));
    S1Angle coverage = S1Angle::Degrees(1.5);

    auto res = NES::Experimental::Mobility::WorkerGeospatialInfo::findPathCoverage(path, node, coverage);

    NES_DEBUG("point " << S2LatLng(res.first));
    NES_DEBUG("dist " << S1Angle(node, res.first).degrees());
    NES_DEBUG("dist on line " << res.second.degrees());

}
}