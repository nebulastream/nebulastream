/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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


#include <Util/Logger.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <gtest/gtest.h>

namespace NES {

class TopologyPropertiesTest : public testing::Test {
  public:
    static void SetUpTestCase() { setupLogging(); }

    void SetUp() {}

    void TearDown() { NES_DEBUG("Tear down OperatorPropertiesTest Test."); }

  protected:
    static void setupLogging() {
        NES::setupLogging("OperatorPropertiesTest.log", NES::LOG_DEBUG);
        NES_DEBUG("Setup OperatorPropertiesTest test class.");
    }
};

// test assigning topology properties
TEST_F(TopologyPropertiesTest, testAssignProperties) {
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create a nodes
    auto node = TopologyNode::create(1, "localhost", grpcPort, dataPort, 8);
    node->addProperty("cores",2);
    node->addProperty("architecture", std::string("arm64"));
    node->addProperty("withGPU",false);

    EXPECT_TRUE(node->getProperty("cores").has_value());
    EXPECT_TRUE(node->getProperty("architecture").has_value());
    EXPECT_TRUE(node->getProperty("withGPU").has_value());

    EXPECT_EQ(std::any_cast<int>(node->getProperty("cores")), 2);
    EXPECT_EQ(std::any_cast<std::string>(node->getProperty("architecture")), "arm64");
    EXPECT_EQ(std::any_cast<bool>(node->getProperty("withGPU")), false);
}

// test removing a topology properties
TEST_F(TopologyPropertiesTest, testRemoveAProperty) {
    TopologyPtr topology = Topology::create();
    uint32_t grpcPort = 4000;
    uint32_t dataPort = 5000;

    // create a nodes
    auto node = TopologyNode::create(1, "localhost", grpcPort, dataPort, 8);
    node->addProperty("cores",2);

    ASSERT_TRUE(node->getProperty("cores").has_value());

    node->removeProperty("cores");
    EXPECT_THROW(node->getProperty("cores"), NesRuntimeException);
}


}// namespace NES
