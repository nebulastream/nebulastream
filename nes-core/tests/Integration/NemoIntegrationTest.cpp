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

#include <API/QueryAPI.hpp>
#include <BaseIntegrationTest.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>

namespace NES {
using namespace Configurations;

/**
 * @brief Test the NEMO placement on different topologies to check if shared nodes contain the window operator based on the configs
 * of setDistributedWindowChildThreshold and setDistributedWindowCombinerThreshold.
 */
class NemoIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    Runtime::BufferManagerPtr bufferManager;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("NemoIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NemoIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
    }

    static CSVSourceTypePtr createCSVSourceType(std::string inputPath) {
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setFilePath(std::move(inputPath));
        csvSourceType->setNumberOfTuplesToProducePerBuffer(50);
        csvSourceType->setNumberOfBuffersToProduce(1);
        csvSourceType->setSkipHeader(false);
        csvSourceType->setGatheringInterval(1000);
        return csvSourceType;
    }

    TestHarness createTestHarness(uint64_t layers,
                                  uint64_t nodesPerNode,
                                  uint64_t leafNodesPerNode,
                                  int64_t childThreshold,
                                  int64_t combinerThreshold) {
        std::function<void(CoordinatorConfigurationPtr)> crdFunctor =
            [childThreshold, combinerThreshold](const CoordinatorConfigurationPtr& config) {
                config->optimizer.enableNemoPlacement.setValue(true);
                config->optimizer.distributedWindowChildThreshold.setValue(childThreshold);
                config->optimizer.distributedWindowCombinerThreshold.setValue(combinerThreshold);
            };

        auto inputSchema = Schema::create()
                               ->addField("key", DataTypeFactory::createUInt32())
                               ->addField("value", DataTypeFactory::createUInt32())
                               ->addField("timestamp", DataTypeFactory::createUInt64());

        auto query = Query::from("car")
                         .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                         .byKey(Attribute("key"))
                         .apply(Sum(Attribute("value")));

        TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                      .enableNautilus()
                                      .enableDistributedWindowOptimization()
                                      .addLogicalSource("car", inputSchema);

        std::vector<uint64_t> nodes;
        std::vector<uint64_t> parents;
        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;
        nodes.emplace_back(1);
        parents.emplace_back(1);

        auto cnt = 1;
        for (uint64_t i = 2; i <= layers; i++) {
            std::vector<uint64_t> newParents;
            for (auto parent : parents) {
                uint64_t nodeCnt = nodesPerNode;
                if (i == layers) {
                    nodeCnt = leafNodesPerNode;
                }
                for (uint64_t j = 0; j < nodeCnt; j++) {
                    nodeId++;
                    nodes.emplace_back(nodeId);
                    newParents.emplace_back(nodeId);

                    if (i == layers) {
                        leafNodes++;
                        auto csvSource = createCSVSourceType(std::string(TEST_DATA_DIRECTORY) + "keyed_windows/window_"
                                                             + std::to_string(cnt++) + ".csv");
                        testHarness.attachWorkerWithCSVSourceToWorkerWithId("car", csvSource, parent);
                        NES_DEBUG("NemoIntegrationTest: Adding CSV source for node:{}", nodeId);
                        continue;
                    }
                    testHarness.attachWorkerToWorkerWithId(parent);
                    NES_DEBUG("NemoIntegrationTest: Adding worker to worker with ID:{}", parent);
                }
            }
            parents = newParents;
        }
        testHarness.validate().setupTopology(crdFunctor);
        return testHarness;
    }

    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t id;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };
};

TEST_F(NemoIntegrationTest, testThreeLevelsTopologyTopDown) {
    int64_t childThreshold = 1000;
    int64_t combinerThreshold = 1;
    uint64_t expectedTuples = 80;

    auto testHarness = createTestHarness(3, 2, 4, childThreshold, combinerThreshold);
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedTuples, "BottomUp", "NONE", "IN_MEMORY");
    EXPECT_EQ(actualOutput.size(), expectedTuples);

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("NemoIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_INFO("NemoIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(1, countOccurrences("CENTRALWINDOW", queryPlan->toString()));
}

TEST_F(NemoIntegrationTest, testThreeLevelsTopologyBottomUp) {
    int64_t childThreshold = 1;
    int64_t combinerThreshold = 1000;
    uint64_t expectedTuples = 80;

    auto testHarness = createTestHarness(3, 2, 4, childThreshold, combinerThreshold);
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedTuples, "BottomUp", "NONE", "IN_MEMORY");
    EXPECT_EQ(actualOutput.size(), expectedTuples);

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("NemoIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_INFO("NemoIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(8, countOccurrences("CENTRALWINDOW", queryPlan->toString()));
}

TEST_F(NemoIntegrationTest, testNemoThreelevels) {
    int64_t childThreshold = 1;
    int64_t combinerThreshold = 1;
    uint64_t expectedTuples = 80;

    auto testHarness = createTestHarness(3, 2, 4, childThreshold, combinerThreshold);
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedTuples, "BottomUp", "NONE", "IN_MEMORY");
    EXPECT_EQ(actualOutput.size(), expectedTuples);

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("NemoIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_INFO("NemoIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(2, countOccurrences("CENTRALWINDOW", queryPlan->toString()));
}

}// namespace NES