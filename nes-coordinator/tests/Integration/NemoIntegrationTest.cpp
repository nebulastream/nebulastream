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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>

namespace NES {
using namespace Configurations;
using namespace Runtime;

static const std::string sourceNameLeft = "log_left";
static const std::string sourceNameRight = "log_right";

/**
 * @brief Test the NEMO placement on different topologies to check if shared nodes contain the window operator based on the configs
 * of setDistributedWindowChildThreshold and setDistributedWindowCombinerThreshold.
 */
class NemoIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    BufferManagerPtr bufferManager;
    QueryCompilation::StreamJoinStrategy joinStrategy = QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN;
    QueryCompilation::WindowingStrategy windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("NemoIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup NemoIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<BufferManager>();
    }

    static CSVSourceTypePtr
    createCSVSourceType(std::string logicalSourceNAme, std::string physicalSourceName, std::string inputPath) {
        CSVSourceTypePtr csvSourceType = CSVSourceType::create(logicalSourceNAme, physicalSourceName);
        csvSourceType->setFilePath(std::move(inputPath));
        csvSourceType->setNumberOfTuplesToProducePerBuffer(50);
        csvSourceType->setNumberOfBuffersToProduce(1);
        csvSourceType->setSkipHeader(false);
        csvSourceType->setGatheringInterval(1000);
        return csvSourceType;
    }

    TestHarness createTestHarness(const Query& query,
                                  std::function<void(CoordinatorConfigurationPtr)> crdFunctor,
                                  uint64_t layers,
                                  uint64_t nodesPerNode,
                                  uint64_t leafNodesPerNode) {
        auto inputSchema = Schema::create()
                               ->addField("key", DataTypeFactory::createUInt32())
                               ->addField("value", DataTypeFactory::createUInt32())
                               ->addField("timestamp", DataTypeFactory::createUInt64());

        TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                      .setJoinStrategy(joinStrategy)
                                      .setWindowingStrategy(windowingStrategy)
                                      .addLogicalSource(sourceNameLeft, inputSchema)
                                      .addLogicalSource(sourceNameRight, inputSchema);

        std::vector<uint64_t> nodes;
        std::vector<uint64_t> parents;
        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;
        nodes.emplace_back(1);
        parents.emplace_back(1);

        auto cnt = 1;
        for (uint64_t i = 2; i <= layers; i++) {
            std::vector<uint64_t> newParents;
            std::string sourceName;
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
                        if (leafNodes % 2 == 0) {
                            sourceName = sourceNameLeft;
                        } else {
                            sourceName = sourceNameRight;
                        }

                        auto csvSource = createCSVSourceType(sourceName,
                                                             "src_" + std::to_string(leafNodes),
                                                             std::string(TEST_DATA_DIRECTORY) + "KeyedWindows/window_"
                                                                 + std::to_string(cnt++) + ".csv");
                        testHarness.attachWorkerWithCSVSourceToWorkerWithId(csvSource, parent);
                        NES_DEBUG("NemoIntegrationTest: Adding CSV source:{} for node:{}", sourceName, nodeId);
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
};

TEST_F(NemoIntegrationTest, testThreeLevelsTopologyTopDown) {
    uint64_t expectedTuples = 54;
    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](const CoordinatorConfigurationPtr& config) {
        config->optimizer.enableNemoPlacement.setValue(false);
        //config->optimizer.distributedWindowChildThreshold = 0;
        //config->optimizer.distributedWindowCombinerThreshold = 0;
    };
    Query query = Query::from(sourceNameLeft)
                      .joinWith(Query::from(sourceNameRight))
                      .where(Attribute("value"))
                      .equalsTo(Attribute("value"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    auto testHarness = createTestHarness(query, crdFunctor, 2, 2, 4);
    auto actualOutput = testHarness.runQuery(expectedTuples).getOutput();
    auto outputString = actualOutput[0].toString(testHarness.getOutputSchema(), true);
    NES_DEBUG("NemoIntegrationTest: Output\n{}", outputString);
    EXPECT_EQ(TestUtils::countTuples(actualOutput), expectedTuples);

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("NemoIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_INFO("NemoIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(1, countOccurrences("Join", queryPlan->toString()));
}

}// namespace NES