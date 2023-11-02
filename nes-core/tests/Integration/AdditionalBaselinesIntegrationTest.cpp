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
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Topology/Topology.hpp>
#include <Topology/TopologyNode.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gtest/gtest.h>
#include <list>

namespace NES {
using namespace Configurations;

/**
* @brief Test the NEMO placement on different topologies to check if shared nodes contain the window operator based on the configs
* of setDistributedWindowChildThreshold and setDistributedWindowCombinerThreshold.
*/
class AdditionalBaselinesIntegrationTest : public Testing::BaseIntegrationTest {
  public:
    Runtime::BufferManagerPtr bufferManager;

    std::list<std::string> sources = {"100_ms", "13_ms", "24_ms", "47_ms", "54_ms", "62_ms", "67_ms", "74_ms", "98_ms",
                                      "105_ms", "14_ms", "28_ms", "49_ms", "57_ms", "63_ms", "68_ms", "75_ms", "99_ms",
                                      "106_ms", "16_ms", "38_ms", "4_ms",  "58_ms", "64_ms", "69_ms", "88_ms", "10_ms",
                                      "19_ms",  "40_ms", "52_ms", "59_ms", "65_ms", "71_ms", "8_ms",  "12_ms", "23_ms",
                                      "44_ms",  "53_ms", "61_ms", "66_ms", "73_ms", "97_ms"};

    static void SetUpTestCase() {
        NES::Logger::setupLogging("AdditionalBaselinesIntegrationTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AdditionalBaselinesIntegrationTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        bufferManager = std::make_shared<Runtime::BufferManager>(4096, 10);
    }

    static CSVSourceTypePtr createCSVSourceType(std::string inputPath) {
        CSVSourceTypePtr csvSourceType = CSVSourceType::create();
        csvSourceType->setFilePath(std::move(inputPath));
        csvSourceType->setSkipHeader(false);
        return csvSourceType;
    }

    TestHarness createTestHarness(uint64_t layers,
                                  uint64_t nodesPerNode,
                                  uint64_t leafNodesPerNode,
                                  int64_t childThreshold,
                                  int64_t combinerThreshold,
                                  uint64_t maxNumberOfNodes) {
        std::function<void(CoordinatorConfigurationPtr)> crdFunctor =
            [childThreshold, combinerThreshold](const CoordinatorConfigurationPtr& config) {
                config->optimizer.enableNemoPlacement.setValue(true);
                //config->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
                config->optimizer.distributedWindowChildThreshold.setValue(childThreshold);
                config->optimizer.distributedWindowCombinerThreshold.setValue(combinerThreshold);
            };

        auto inputSchema = Schema::create()
                               ->addField("sid", DataTypeFactory::createUInt64())
                               ->addField("ts", DataTypeFactory::createUInt64())
                               ->addField("x", DataTypeFactory::createInt64())
                               ->addField("y", DataTypeFactory::createInt64())
                               ->addField("z", DataTypeFactory::createInt64())
                               ->addField("absolute_v", DataTypeFactory::createUInt64())
                               ->addField("absolute_a", DataTypeFactory::createUInt64())
                               ->addField("vx", DataTypeFactory::createInt64())
                               ->addField("vy", DataTypeFactory::createInt64())
                               ->addField("vz", DataTypeFactory::createInt64())
                               ->addField("ax", DataTypeFactory::createInt64())
                               ->addField("ay", DataTypeFactory::createInt64())
                               ->addField("az", DataTypeFactory::createInt64());

        auto query = Query::from("debs_test")
                         .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1)))
                         .byKey(Attribute("sid"))
                         .apply(Sum(Attribute("absolute_v")));

        TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                      .enableNautilus()
                                      .enableDistributedWindowOptimization()
                                      .addLogicalSource("debs_test", inputSchema);

        std::vector<uint64_t> nodes;
        std::vector<uint64_t> parents;
        uint64_t nodeId = 1;
        uint64_t leafNodes = 0;
        nodes.emplace_back(1);
        parents.emplace_back(1);

        auto cnt = 1;
        uint64_t currentNumberOfNodes = 1;
        for (uint64_t i = 2; i <= layers; i++) {
            std::vector<uint64_t> newParents;
            for (auto parent : parents) {
                uint64_t nodeCnt = nodesPerNode;

                if (currentNumberOfNodes >= maxNumberOfNodes) {
                    break;
                }

                if (i == layers) {
                    nodeCnt = leafNodesPerNode;
                }
                for (uint64_t j = 0; j < nodeCnt; j++) {
                    nodeId++;
                    nodes.emplace_back(nodeId);
                    newParents.emplace_back(nodeId);

                    if (layers == 11) {
                        if (i >= 4) {
                            leafNodes++;
                            auto source = sources.begin();
                            std::advance(source, i);

                            std::string elem = *source;
                            auto csvSource =
                                createCSVSourceType(std::string(TEST_DATA_DIRECTORY) + "debs13_readings/" + elem + ".csv");
                            testHarness.attachWorkerWithCSVSourceToWorkerWithId("debs_test", csvSource, parent);
                            NES_DEBUG("AdditionalBaselinesIntegrationTest: Adding CSV source for node:{}", nodeId);
                            currentNumberOfNodes++;
                            continue;
                        }
                    } else if (layers == 4) {
                        if (i == layers || i == layers - 1) {
                            leafNodes++;
                            auto source = sources.begin();
                            std::advance(source, cnt);

                            std::string elem = *source;
                            auto csvSource =
                                createCSVSourceType(std::string(TEST_DATA_DIRECTORY) + "debs13_readings/" + elem + ".csv");
                            testHarness.attachWorkerWithCSVSourceToWorkerWithId("debs_test", csvSource, parent);
                            NES_DEBUG("AdditionalBaselinesIntegrationTest: Adding CSV source for node:{}", nodeId);
                            currentNumberOfNodes++;
                            continue;
                        }
                    }

                    else {
                        if (i == layers) {
                            leafNodes++;
                            auto source = sources.begin();
                            std::advance(source, cnt);

                            std::string elem = *source;
                            auto csvSource =
                                createCSVSourceType(std::string(TEST_DATA_DIRECTORY) + "debs13_readings/" + elem + ".csv");
                            testHarness.attachWorkerWithCSVSourceToWorkerWithId("debs_test", csvSource, parent);
                            NES_DEBUG("AdditionalBaselinesIntegrationTest: Adding CSV source for node:{}", nodeId);
                            currentNumberOfNodes++;
                            continue;
                        }
                    }

                    testHarness.attachWorkerToWorkerWithId(parent);
                    currentNumberOfNodes++;
                    NES_DEBUG("AdditionalBaselinesIntegrationTest: Adding worker to worker with ID:{}", parent);
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
        uint64_t sid;
        uint64_t absolute_v;

        // overload the == operator to check if two instances are the same
        /**
    bool operator==(Output const& rhs) const {
        return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
    }
     **/
    };
};
//}

TEST_F(AdditionalBaselinesIntegrationTest, testChainApproach) {
    // we only need 8 sources
    int64_t childThreshold = 1000;
    int64_t combinerThreshold = 1;
    uint64_t expectedTuples = 16;

    struct Test {
        uint64_t sid;
        uint64_t ts;
        int64_t x;
        int64_t y;
        int64_t z;
        uint64_t absolute_v;
        uint64_t absolute_a;
        int64_t vx;
        int64_t vy;
        int64_t vz;
        int64_t ax;
        int64_t ay;
        int64_t az;
    };

    //auto testHarness = createTestHarness(11, 1, 1, childThreshold, combinerThreshold);
    auto testHarness = createTestHarness(11, 1, 1, childThreshold, combinerThreshold, 11);

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedTuples, "BottomUp", "NONE", "IN_MEMORY");

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(actualOutput.size(), expectedTuples);

    EXPECT_EQ(1, countOccurrences("CENTRALWINDOW", queryPlan->toString()));
}

TEST_F(AdditionalBaselinesIntegrationTest, testMstApproach) {
    // we only need 8 sources
    int64_t childThreshold = 1000;
    int64_t combinerThreshold = 1;
    uint64_t expectedTuples = 2;

    struct Test {
        uint64_t sid;
        uint64_t ts;
        int64_t x;
        int64_t y;
        int64_t z;
        uint64_t absolute_v;
        uint64_t absolute_a;
        int64_t vx;
        int64_t vy;
        int64_t vz;
        int64_t ax;
        int64_t ay;
        int64_t az;
    };
    //auto testHarness = createTestHarness(11, 1, 1, childThreshold, combinerThreshold);
    auto testHarness = createTestHarness(4, 2, 2, childThreshold, combinerThreshold, 11);

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedTuples, "BottomUp", "NONE", "IN_MEMORY");

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(actualOutput.size(), expectedTuples);

    EXPECT_EQ(1, countOccurrences("CENTRALWINDOW", queryPlan->toString()));
}

TEST_F(AdditionalBaselinesIntegrationTest, testTopDownApproach) {
    // we only need 8 sources
    int64_t childThreshold = 1000;
    int64_t combinerThreshold = 1;
    uint64_t expectedTuples = 2;

    struct Test {
        uint64_t sid;
        uint64_t ts;
        int64_t x;
        int64_t y;
        int64_t z;
        uint64_t absolute_v;
        uint64_t absolute_a;
        int64_t vx;
        int64_t vy;
        int64_t vz;
        int64_t ax;
        int64_t ay;
        int64_t az;
    };
    auto testHarness = createTestHarness(3, 2, 4, childThreshold, combinerThreshold, 11);

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedTuples, "BottomUp", "NONE", "IN_MEMORY");

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(actualOutput.size(), expectedTuples);

    EXPECT_EQ(1, countOccurrences("CENTRALWINDOW", queryPlan->toString()));
}

TEST_F(AdditionalBaselinesIntegrationTest, testBottomUpApproach) {
    int64_t childThreshold = 1;
    int64_t combinerThreshold = 1000;
    uint64_t expectedTuples = 16;

    struct Test {
        uint64_t sid;
        uint64_t ts;
        int64_t x;
        int64_t y;
        int64_t z;
        uint64_t absolute_v;
        uint64_t absolute_a;
        int64_t vx;
        int64_t vy;
        int64_t vz;
        int64_t ax;
        int64_t ay;
        int64_t az;
    };
    auto testHarness = createTestHarness(3, 2, 4, childThreshold, combinerThreshold, 11);

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedTuples, "BottomUp", "NONE", "IN_MEMORY");

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(actualOutput.size(), expectedTuples);

    EXPECT_EQ(8, countOccurrences("CENTRALWINDOW", queryPlan->toString()));
}

TEST_F(AdditionalBaselinesIntegrationTest, testNemoApproach) {
    int64_t childThreshold = 1;
    int64_t combinerThreshold = 1;
    uint64_t expectedTuples = 4;

    struct Test {
        uint64_t sid;
        uint64_t ts;
        int64_t x;
        int64_t y;
        int64_t z;
        uint64_t absolute_v;
        uint64_t absolute_a;
        int64_t vx;
        int64_t vy;
        int64_t vz;
        int64_t ax;
        int64_t ay;
        int64_t az;
    };
    auto testHarness = createTestHarness(3, 2, 4, childThreshold, combinerThreshold, 11);

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedTuples, "BottomUp", "NONE", "IN_MEMORY");

    TopologyPtr topology = testHarness.getTopology();
    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with topology \n{}", topology->toString());
    NES_DEBUG("AdditionalBaselinesIntegrationTest: Executed with plan \n{}", queryPlan->toString());
    EXPECT_EQ(actualOutput.size(), expectedTuples);

    EXPECT_EQ(2, countOccurrences("CENTRALWINDOW", queryPlan->toString()));
}

}// namespace NES