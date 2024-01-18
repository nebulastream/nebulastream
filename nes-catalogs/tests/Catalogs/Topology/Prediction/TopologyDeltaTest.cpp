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
#include <Util/TopologyLinkInformation.hpp>
#include <Catalogs/Topology/Prediction/TopologyDelta.hpp>
#include <gtest/gtest.h>
namespace NES {
using Experimental::TopologyPrediction::TopologyDelta;

class TopologyDeltaTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("TopologyDeltaTest.log", NES::LogLevel::LOG_DEBUG);
        NES_DEBUG("Set up TopologyDelta test class");
    }
};

TEST_F(TopologyDeltaTest, testToString) {
    TopologyDelta delta({{1, 2}, {1, 3}, {5, 3}}, {{2, 3}, {1, 5}});
    EXPECT_EQ(delta.toString(), "added: {1->2, 1->3, 5->3}, removed: {2->3, 1->5}");
    TopologyDelta emptyDelta({}, {});
    EXPECT_EQ(emptyDelta.toString(), "added: {}, removed: {}");
}

TEST_F(TopologyDeltaTest, testGetEdges) {
    std::vector<TopologyLinkInformation> added = {{1, 2}, {1, 3}, {5, 3}};
    std::vector<TopologyLinkInformation> removed = {{2, 3}, {1, 5}};
    TopologyDelta delta(added, removed);
    EXPECT_EQ(delta.getAdded(), added);
    EXPECT_EQ(delta.getRemoved(), removed);

    TopologyDelta emptyDelta({}, {});
    EXPECT_TRUE(emptyDelta.getAdded().empty());
    EXPECT_TRUE(emptyDelta.getRemoved().empty());
}
}// namespace NES
