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
#include <Catalogs/Topology/Prediction/Edge.hpp>
#include <gtest/gtest.h>

namespace NES {
using Experimental::TopologyPrediction::Edge;

class EdgeTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("EdgeTest.log", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(EdgeTest, testToString) {
    Edge edge(1, 2);
    ASSERT_EQ(edge.toString(), "1->2");
}

TEST_F(EdgeTest, testEquality) {
    Edge edge(1, 2);
    Edge edge2(2, 1);
    Edge edge3(1, 2);
    ASSERT_EQ(edge, edge3);
    ASSERT_NE(edge, edge2);
    ASSERT_NE(edge3, edge2);
}
}// namespace NES
