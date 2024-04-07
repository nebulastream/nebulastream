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
 * @brief Test of distributed NEMO join
 */
class DistributedMatrixJoinIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    BufferManagerPtr bufferManager;
    QueryCompilation::StreamJoinStrategy joinStrategy = QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN;
    QueryCompilation::WindowingStrategy windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("DistributedMatrixJoinIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DistributedMatrixJoinIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<BufferManager>();
    }

    static CSVSourceTypePtr createCSVSourceType(const std::string& logicalSourceNAme,
                                                const std::string& physicalSourceName,
                                                const std::string& inputPath) {
        CSVSourceTypePtr csvSourceType = CSVSourceType::create(logicalSourceNAme, physicalSourceName);
        csvSourceType->setFilePath(inputPath);
        csvSourceType->setNumberOfTuplesToProducePerBuffer(50);
        csvSourceType->setNumberOfBuffersToProduce(1);
        csvSourceType->setSkipHeader(false);
        csvSourceType->setGatheringInterval(1000);
        return csvSourceType;
    }

    /**
     * Creates a TestHarness with a given query and topology parameters. The leafs of the topology are sources.
     * @param query the query
     * @param crdFunctor functor to pass coordinator params
     * @param layers number of layers of the topology tree
     * @param nodesPerNode number of children per node
     * @param leafNodesPerNode number of leaf nodes for the parents of last layer
     * @return the TestHarness
     */
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
                                                             std::string(TEST_DATA_DIRECTORY) + "window.csv");
                        testHarness.attachWorkerWithCSVSourceToWorkerWithId(csvSource, parent);
                        NES_DEBUG("DistributedMatrixJoinIntegrationTest: Adding CSV source:{} for node:{}", sourceName, nodeId);
                        continue;
                    }
                    testHarness.attachWorkerToWorkerWithId(parent);
                    NES_DEBUG("DistributedMatrixJoinIntegrationTest: Adding worker to worker with ID:{}", parent);
                }
            }
            parents = newParents;
        }
        testHarness.validate().setupTopology(crdFunctor);
        return testHarness;
    }
};

TEST_F(DistributedMatrixJoinIntegrationTest, testThreeLevelsTopologyTopDown) {
    uint64_t sourceNo = 4;
    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](const CoordinatorConfigurationPtr& config) {
        config->optimizer.joinOptimizationMode.setValue(Optimizer::DistributedJoinOptimizationMode::MATRIX);
    };
    Query query = Query::from(sourceNameLeft)
                      .joinWith(Query::from(sourceNameRight))
                      .where(Attribute("value"))
                      .equalsTo(Attribute("value"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    // create flat topology with 1 coordinator and 4 sources
    auto testHarness = createTestHarness(query, crdFunctor, 2, 2, sourceNo);
    // 17 join keys
    uint64_t expectedTuples = 17 * pow(2, sourceNo - 1);

    auto actualOutput = testHarness.runQuery(expectedTuples).getOutput();
    auto outputString = actualOutput[0].toString(testHarness.getOutputSchema(), true);
    NES_DEBUG("DistributedMatrixJoinIntegrationTest: Output\n{}", outputString);
    EXPECT_EQ(TestUtils::countTuples(actualOutput), expectedTuples);

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("DistributedMatrixJoinIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_INFO("DistributedMatrixJoinIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(4, countOccurrences("Join", queryPlan->toString()));
}

}// namespace NES