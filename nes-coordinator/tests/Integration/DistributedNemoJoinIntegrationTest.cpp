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
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Runtime/BufferManager.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

namespace NES
{
using namespace Configurations;
using namespace Runtime;

static const std::string sourceNameLeft = "log_left";
static const std::string sourceNameRight = "log_right";

/**
 * @brief Test of distributed NEMO join
 */
class DistributedNemoJoinIntegrationTest : public Testing::BaseIntegrationTest
{
public:
    BufferManagerPtr bufferManager;
    QueryCompilation::StreamJoinStrategy joinStrategy = QueryCompilation::StreamJoinStrategy::NESTED_LOOP_JOIN;
    QueryCompilation::WindowingStrategy windowingStrategy = QueryCompilation::WindowingStrategy::SLICING;

    static void SetUpTestCase()
    {
        NES::Logger::setupLogging("DistributedNemoJoinIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DistributedNemoJoinIntegrationTest test class.");
    }

    void SetUp() override
    {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<BufferManager>();
    }

    static CSVSourceTypePtr
    createCSVSourceType(const std::string& logicalSourceNAme, const std::string& physicalSourceName, const std::string& inputPath)
    {
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
    TestHarness createTestHarness(
        const Query& query,
        std::function<void(CoordinatorConfigurationPtr)> crdFunctor,
        uint64_t layers,
        uint64_t nodesPerNode,
        uint64_t leafNodesPerNode)
    {
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

        std::vector<nlohmann::json> distributionList;
        uint64_t value = 0;

        // create the additional layers
        for (uint64_t i = 2; i <= layers; i++)
        {
            std::vector<uint64_t> newParents;
            std::string logSource;
            // iterate and set the parents of the current layer
            for (auto parent : parents)
            {
                uint64_t nodeCnt = nodesPerNode;
                if (i == layers)
                {
                    nodeCnt = leafNodesPerNode;
                }
                // set the children of the current parents
                for (uint64_t j = 0; j < nodeCnt; j++)
                {
                    nodeId++;
                    // if children are leaf nodes create the sources, else just create intermediate workers
                    if (i == layers)
                    {
                        leafNodes++;
                        // even leaf nodes are part of the LHS join, uneven of the RHS join
                        if (leafNodes % 2 == 0)
                        {
                            logSource = sourceNameLeft;
                        }
                        else
                        {
                            logSource = sourceNameRight;
                            value += 10;
                        }

                        auto phySource = logSource + std::to_string(leafNodes);
                        auto csvSource = createCSVSourceType(logSource, phySource, std::string(TEST_DATA_DIRECTORY) + "window.csv");
                        testHarness.attachWorkerWithCSVSourceToWorkerWithId(csvSource, WorkerId(parent));

                        nlohmann::json request;
                        request["topologyNodeId"] = nodeId;
                        request["logicalSource"] = logSource;
                        request["physicalSource"] = phySource;
                        request["fieldName"] = "key";
                        request["value"] = std::to_string(value);
                        distributionList.emplace_back(request);
                        NES_DEBUG("DistributedNemoJoinIntegrationTest: Adding statistics for {}", request.dump());
                        NES_DEBUG("DistributedNemoJoinIntegrationTest: Adding CSV source:{} to {} for node:{}", logSource, parent, nodeId);
                    }
                    else
                    {
                        testHarness.attachWorkerToWorkerWithId(WorkerId(parent));
                        NES_DEBUG("DistributedNemoJoinIntegrationTest: Adding worker {} to worker with ID:{}", nodeId, parent);
                    }
                    nodes.emplace_back(nodeId);
                    newParents.emplace_back(nodeId);
                }
            }
            parents = newParents;
        }
        testHarness.validate().setupTopology(crdFunctor, distributionList);
        return testHarness;
    }
};

TEST_F(DistributedNemoJoinIntegrationTest, testThreeLevelsTopologyTopDown)
{
    uint64_t layers = 3;
    uint64_t nodesPerNode = 4;
    uint64_t leafNodesPerNode = 2;
    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](const CoordinatorConfigurationPtr& config)
    { config->optimizer.joinOptimizationMode.setValue(Optimizer::DistributedJoinOptimizationMode::NEMO); };
    Query query = Query::from(sourceNameLeft)
                      .joinWith(Query::from(sourceNameRight))
                      .where(Attribute("value") == Attribute("value"))
                      .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    // create flat topology with 1 coordinator and 4 sources
    auto testHarness = createTestHarness(query, crdFunctor, layers, nodesPerNode, leafNodesPerNode);
    // 26 join keys
    uint64_t expectedTuples = 26 * nodesPerNode * leafNodesPerNode;

    auto actualOutput = testHarness.runQuery(expectedTuples).getOutput();
    auto outputString = actualOutput[0].toString(testHarness.getOutputSchema(), true);
    NES_DEBUG("DistributedNemoJoinIntegrationTest: Output\n{}", outputString);
    EXPECT_EQ(TestUtils::countTuples(actualOutput), expectedTuples);

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("DistributedNemoJoinIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_INFO("DistributedNemoJoinIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(nodesPerNode, countOccurrences("Join", queryPlan->toString()));
}

} // namespace NES
