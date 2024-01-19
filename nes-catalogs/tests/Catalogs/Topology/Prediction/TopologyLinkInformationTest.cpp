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
#include <gtest/gtest.h>

namespace NES {

class TopologyLinkInformationTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() { NES::Logger::setupLogging("TopologyLinkInformationTest.log", NES::LogLevel::LOG_DEBUG); }
};

TEST_F(TopologyLinkInformationTest, testToString) {
    TopologyLinkInformation link(1, 2);
    ASSERT_EQ(link.toString(), "1->2");
}

TEST_F(TopologyLinkInformationTest, testEquality) {
    TopologyLinkInformation link(1, 2);
    TopologyLinkInformation link2(2, 1);
    TopologyLinkInformation link3(1, 2);
    ASSERT_EQ(link, link3);
    ASSERT_NE(link, link2);
    ASSERT_NE(link3, link2);
}
}// namespace NES
