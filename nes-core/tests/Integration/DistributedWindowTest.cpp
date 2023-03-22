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
#include <NesBaseTest.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/Identifiers.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>

using namespace std;

namespace NES {

using namespace Configurations;

class DistributedWindowTest : public Testing::NESBaseTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DistributedWindowTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DistributedWindowTest test class.");
    }
};

/**
 * @brief This tests applies a window on a three level hierarchy
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=9, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=8, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DistributedWindowTest, testWindowThreeLevel) {
    uint64_t workerNo = 10;
    std::vector<WorkerConfigurationPtr> workerConfigs;

    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](CoordinatorConfigurationPtr config) {
        config->optimizer.distributedWindowChildThreshold.setValue(0);
        config->optimizer.distributedWindowCombinerThreshold.setValue(0);
    };

    for (uint64_t i = 0; i < workerNo; i++) {
        auto workerConfig = WorkerConfiguration::create();
        //workerConfig->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerOptions::QueryCompiler::NAUTILUS_QUERY_COMPILER;
        workerConfigs.emplace_back(workerConfig);
    }

    struct Test {
        uint64_t id;
        uint64_t value;
        uint64_t ts;
    };

    auto testSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("ts", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(3);

    uint64_t workerCnt = 0;
    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness =
        TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
            .addLogicalSource("window", testSchema)
            // Workers
            .attachWorkerToWorkerWithId(1, workerConfigs[workerCnt++])// id=2
            .attachWorkerToWorkerWithId(2, workerConfigs[workerCnt++])// id=3
            .attachWorkerToWorkerWithId(2, workerConfigs[workerCnt++])// id=4
            .attachWorkerToWorkerWithId(1, workerConfigs[workerCnt++])// id=5
            .attachWorkerToWorkerWithId(5, workerConfigs[workerCnt++])// id=6
            .attachWorkerToWorkerWithId(5, workerConfigs[workerCnt++])// id=7
            // Sensors
            .attachWorkerWithCSVSourceToWorkerWithId("window", csvSourceType, 3, workerConfigs[workerCnt++])// id=8
            .attachWorkerWithCSVSourceToWorkerWithId("window", csvSourceType, 4, workerConfigs[workerCnt++])// id=9
            .attachWorkerWithCSVSourceToWorkerWithId("window", csvSourceType, 6, workerConfigs[workerCnt++])// id=10
            .attachWorkerWithCSVSourceToWorkerWithId("window", csvSourceType, 7, workerConfigs[workerCnt++])// id=11
            .validate()
            .setupTopology(crdFunctor);

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 1U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren()[1]->getChildren().size(), 1U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren()[0]->getChildren().size(), 1U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren()[1]->getChildren().size(), 1U);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 68}, {2000, 3000, 2, 112}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    // check that the new window op "CENTRALWINDOW" is in use
    NES_INFO("DistributedWindowTest: Executed with plan \n" << queryPlan->toString());
    ASSERT_TRUE(queryPlan->toString().find("WindowComputationOperator") != std::string::npos);
    ASSERT_TRUE(queryPlan->toString().find("SliceMergingOperator") != std::string::npos);
    ASSERT_TRUE(queryPlan->toString().find("SliceCreationOperator") != std::string::npos);

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief This tests applies a window on a three level hierarchy
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=9, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=8, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=11, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |  |--PhysicalNode[id=10, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DistributedWindowTest, testWindowThreeLevelNemoPlacement) {
    uint64_t workerNo = 10;
    std::vector<WorkerConfigurationPtr> workerConfigs;

    struct Test {
        uint64_t id;
        uint64_t value;
        uint64_t ts;
    };

    auto testSchema = Schema::create()
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("ts", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(3);

    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](CoordinatorConfigurationPtr config) {
        config->optimizer.enableNemoPlacement.setValue(true);
        config->optimizer.distributedWindowCombinerThreshold.setValue(100);
        config->optimizer.distributedWindowChildThreshold.setValue(100);
    };

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("window", testSchema)
                                  // Workers
                                  .attachWorkerToCoordinator()  // id=2
                                  .attachWorkerToWorkerWithId(2)// id=3
                                  .attachWorkerToWorkerWithId(2)// id=4
                                  .attachWorkerToCoordinator()  // id=5
                                  .attachWorkerToWorkerWithId(5)// id=6
                                  .attachWorkerToWorkerWithId(5)// id=7
                                  // Sensors
                                  .attachWorkerWithCSVSourceToWorkerWithId("window", csvSourceType, 3)// id=8
                                  .attachWorkerWithCSVSourceToWorkerWithId("window", csvSourceType, 4)// id=9
                                  .attachWorkerWithCSVSourceToWorkerWithId("window", csvSourceType, 6)// id=10
                                  .attachWorkerWithCSVSourceToWorkerWithId("window", csvSourceType, 7)// id=11
                                  .validate()
                                  .setupTopology(crdFunctor);

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 1U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren()[1]->getChildren().size(), 1U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren()[0]->getChildren().size(), 1U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren()[1]->getChildren().size(), 1U);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t id;
        uint64_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && id == rhs.id && value == rhs.value);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 68}, {2000, 3000, 2, 112}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    QueryPlanPtr queryPlan = testHarness.getQueryPlan();
    // check that the new window op "CENTRALWINDOW" is in use
    NES_INFO("DeepHierarchyTopologyTest: Executed with plan \n" << queryPlan->toString());
    ASSERT_TRUE(queryPlan->toString().find("CENTRALWINDOW") != std::string::npos);

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

}// namespace NES
