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
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <BaseIntegrationTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop

#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class AllowedLatenessTest : public Testing::BaseIntegrationTest {
  public:
    CSVSourceTypePtr inOrderConf;
    CSVSourceTypePtr outOfOrderConf;
    SchemaPtr inputSchema;

    static void SetUpTestCase() {
        NES::Logger::setupLogging("AllowedLatenessTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup AllowedLatenessTest test class.");
    }

    void SetUp() override {
        Testing::BaseIntegrationTest::SetUp();
        // window-out-of-order.csv contains 12 rows
        outOfOrderConf = CSVSourceType::create("OutOfOrderStream", "OutOfOrderStream1");
        outOfOrderConf->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window-out-of-order.csv");
        outOfOrderConf->setGatheringInterval(1);
        outOfOrderConf->setNumberOfTuplesToProducePerBuffer(2);
        outOfOrderConf->setNumberOfBuffersToProduce(6);
        outOfOrderConf->setSkipHeader(false);

        inOrderConf = CSVSourceType::create("inOrderStream", "inOrderStream1");
        // window-out-of-order.csv contains 12 rows
        inOrderConf->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window-in-order.csv");
        inOrderConf->setGatheringInterval(1);
        inOrderConf->setNumberOfTuplesToProducePerBuffer(2);
        inOrderConf->setNumberOfBuffersToProduce(6);
        inOrderConf->setSkipHeader(false);

        inputSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());
    }

    std::string testName = "AllowedLatenessTest";

    struct Output {
        uint64_t _$start;
        uint64_t _$end;
        uint64_t window$id;
        uint64_t window$value;

        bool operator==(Output const& rhs) const {
            return (_$start == rhs._$start && _$end == rhs._$end && window$id == rhs.window$id
                    && window$value == rhs.window$value);
        }
    };
};

// Test name abbreviations
// SPS: Single Physical Source
// MPS: Multiple Physical Sources
// FT: Flat Topology
// HT: Hierarchical Topology
// IO: In Order
// OO: Out of Order
/*
 * @brief Test allowed lateness using single source, flat topology, in-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_IO_0ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("inOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .validate()
                                  .setupTopology();

    std::vector<Output> expectedOutput = {{1000ULL, 2000ULL, 1ULL, 15ULL},
                                          {2000ULL, 3000ULL, 1ULL, 30ULL},
                                          {3000ULL, 4000ULL, 1ULL, 21ULL}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, in-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_IO_10ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1))) "
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("inOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .validate()
                                  .setupTopology();

    // with allowed lateness=10, the 3000-4000 is closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}, {2000, 3000, 1, 30}, {3000, 4000, 1, 21}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, in-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_IO_250ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("inOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .validate()
                                  .setupTopology();

    // with allowed lateness=250, the 3000-4000 window is not yet closed and up to {5,1,1990} included to the 1000-2000 window
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 15}, {2000, 3000, 1, 30}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, out-of-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_OO_0ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("OutOfOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .validate()
                                  .setupTopology();

    // with allowed lateness = 0, {6,1,1990} is not included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 6}, {2000, 3000, 1, 24}, {3000, 4000, 1, 22}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, out-of-order stream with 50ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_OO_10ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("OutOfOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .validate()
                                  .setupTopology();

    // with allowed lateness = 10, {6,1,1990} is included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 12}, {2000, 3000, 1, 24}, {3000, 4000, 1, 22}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using single source, flat topology, out-of-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_SPS_FT_OO_250ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("OutOfOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .validate()
                                  .setupTopology();

    // with allowed lateness=250, {9,1,1900} included in 1000-2000 window, while the 3000-4000 window is not yet closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 21}, {2000, 3000, 1, 24}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, in-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_IO_0ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("inOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .validate()
                                  .setupTopology();

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 45}, {2000, 3000, 1, 90}, {3000, 4000, 1, 63}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, in-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_IO_10ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("inOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .validate()
                                  .setupTopology();

    // with allowed lateness=10, the 3000-4000 is closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 45}, {2000, 3000, 1, 90}, {3000, 4000, 1, 63}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, in-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_IO_250ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("inOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(inOrderConf)
                                  .validate()
                                  .setupTopology();

    // with allowed lateness=250, the 3000-4000 window is not yet closed and up to {5,1,1990} included to the 1000-2000 window
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 45}, {2000, 3000, 1, 90}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, out-of-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, DISABLED_testAllowedLateness_MPS_FT_OO_0ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](CoordinatorConfigurationPtr config) {
        config->optimizer.distributedWindowChildThreshold.setValue(0);
        config->optimizer.distributedWindowCombinerThreshold.setValue(0);
    };

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("OutOfOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .validate()
                                  .setupTopology(crdFunctor);

    // with allowed lateness = 0, {6,1,1990} is not included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 18}, {2000, 3000, 1, 72}, {3000, 4000, 1, 66}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, out-of-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_OO_10ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](CoordinatorConfigurationPtr config) {
        config->optimizer.distributedWindowChildThreshold.setValue(0);
        config->optimizer.distributedWindowCombinerThreshold.setValue(0);
    };

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("OutOfOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .validate()
                                  .setupTopology(crdFunctor);

    // with allowed lateness = 10, {6,1,1990} is included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 36}, {2000, 3000, 1, 72}, {3000, 4000, 1, 66}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, flat topology, out-of-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_FT_OO_250ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("OutOfOrderStream", inputSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .attachWorkerWithCSVSourceToCoordinator(outOfOrderConf)
                                  .validate()
                                  .setupTopology();

    // with allowed lateness=250, {9,1,1900} included in 1000-2000 window, while the 3000-4000 window is not yet closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 63}, {2000, 3000, 1, 72}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//----Test with Hierarchical Topology----//
//* Topology:
//PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
//|  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
//|  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, in-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_IO_0ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("inOrderStream", inputSchema)
                                  .attachWorkerToCoordinator()//idx 2
                                  .attachWorkerToCoordinator()//idx 3
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 3)
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 3)
                                  .validate()
                                  .setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2ULL);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2ULL);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2ULL);

    std::vector<Output> expectedOutput = {{1000ULL, 2000ULL, 1ULL, 60ULL},
                                          {2000ULL, 3000ULL, 1ULL, 120ULL},
                                          {3000ULL, 4000ULL, 1ULL, 84ULL}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, in-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_IO_10ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("inOrderStream", inputSchema)
                                  .attachWorkerToCoordinator()//idx 2
                                  .attachWorkerToCoordinator()//idx 3
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 3)
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 3)
                                  .validate()
                                  .setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2ULL);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2ULL);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2ULL);

    // with allowed lateness=10, the 3000-4000 is closed
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 60}, {2000, 3000, 1, 120}, {3000, 4000, 1, 84}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, in-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_IO_250ms) {
    string query = "Query::from(\"inOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](CoordinatorConfigurationPtr config) {
        config->optimizer.distributedWindowChildThreshold.setValue(0);
        config->optimizer.distributedWindowCombinerThreshold.setValue(0);
    };

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("inOrderStream", inputSchema)
                                  .attachWorkerToCoordinator()//idx 2
                                  .attachWorkerToCoordinator()//idx 3
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 3)
                                  .attachWorkerWithCSVSourceToWorkerWithId(inOrderConf, 3)
                                  .validate()
                                  .setupTopology(crdFunctor);

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2ULL);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2ULL);

    // with allowed lateness=250, the 3000-4000 window is not yet closed and up to {5,1,1990} included to the 1000-2000 window
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 60}, {2000, 3000, 1, 120}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, out-of-order stream with 0ms allowed lateness
 */
TEST_F(AllowedLatenessTest, DISABLED_testAllowedLateness_MPS_HT_OO_0ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(0), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](CoordinatorConfigurationPtr config) {
        config->optimizer.distributedWindowChildThreshold.setValue(0);
        config->optimizer.distributedWindowCombinerThreshold.setValue(0);
    };

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("OutOfOrderStream", inputSchema)
                                  .attachWorkerToCoordinator()//idx 2
                                  .attachWorkerToCoordinator()//idx 3
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 3)
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 3)
                                  .validate()
                                  .setupTopology(crdFunctor);

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2UL);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2UL);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2UL);

    // with allowed lateness = 0, {6,1,1990} is not included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 24}, {2000, 3000, 1, 96}, {3000, 4000, 1, 88}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, out-of-order stream with 10ms allowed lateness
 */
TEST_F(AllowedLatenessTest, DISABLED_testAllowedLateness_MPS_HT_OO_10ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(10), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](CoordinatorConfigurationPtr config) {
        config->optimizer.distributedWindowChildThreshold.setValue(0);
        config->optimizer.distributedWindowCombinerThreshold.setValue(0);
    };

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("OutOfOrderStream", inputSchema)
                                  .attachWorkerToCoordinator()//idx 2
                                  .attachWorkerToCoordinator()//idx 3
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 3)
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 3)
                                  .validate()
                                  .setupTopology(crdFunctor);

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2UL);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2ULL);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2ULL);

    // with allowed lateness = 10, {6,1,1990} is included to window 1000-2000
    std::vector<Output> expectedOutput = {{1000, 2000, 1, 48}, {2000, 3000, 1, 96}, {3000, 4000, 1, 88}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test allowed lateness using multiple sources, hierarchical topology, out-of-order stream with 250ms allowed lateness
 */
TEST_F(AllowedLatenessTest, testAllowedLateness_MPS_HT_OO_250ms) {
    string query = "Query::from(\"OutOfOrderStream\")"
                   ".assignWatermark(EventTimeWatermarkStrategyDescriptor::create(Attribute(\"timestamp\"),Milliseconds(250), "
                   "Milliseconds()))"
                   ".window(TumblingWindow::of(EventTime(Attribute(\"timestamp\")),Seconds(1)))"
                   ".byKey(Attribute(\"id\"))"
                   ".apply(Sum(Attribute(\"value\")))";

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .enableNewRequestExecutor()
                                  .addLogicalSource("OutOfOrderStream", inputSchema)
                                  .attachWorkerToCoordinator()//idx 2
                                  .attachWorkerToCoordinator()//idx 3
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 2)
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 3)
                                  .attachWorkerWithCSVSourceToWorkerWithId(outOfOrderConf, 3)
                                  .validate()
                                  .setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2ULL);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2ULL);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2ULL);

    // with allowed lateness=250, {9,1,1900} included in 1000-2000 window, while the 3000-4000 window is not yet closed
    std::vector<Output> expectedOutput = {{1000ULL, 2000ULL, 1ULL, 84ULL}, {2000ULL, 3000ULL, 1ULL, 96ULL}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

}// namespace NES
