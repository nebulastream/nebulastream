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
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/ConfigOptions/CoordinatorConfig.hpp>
#include <Configurations/ConfigOptions/SourceConfig.hpp>
#include <Configurations/ConfigOptions/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {
static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class DeepHierarchyTopologyTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("DeepTopologyHierarchyTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup DeepTopologyHierarchyTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { NES_DEBUG("TearDown DeepTopologyHierarchyTest test class."); }
};

/**
 * @brief This tests just outputs the default stream for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepHierarchyTopologyTest, testOutputAndAllSensors) {
    struct Test {
        uint32_t key;
        uint32_t value;
    };

    auto testSchema =
        Schema::create()->addField("key", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query = R"(Query::from("test"))";
    TestHarness testHarness = TestHarness(query);

    testHarness.addMemorySource("test", testSchema, "test1");
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    EXPECT_EQ(topology->getRoot()->getChildren().size(), 1U);
    EXPECT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 4U);

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 0);
        testHarness.pushElement<Test>({1, 1}, 1);
        testHarness.pushElement<Test>({1, 1}, 2);
        testHarness.pushElement<Test>({1, 1}, 3);
        testHarness.pushElement<Test>({1, 1}, 4);
    }

    struct Output {
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value); }
    };

    std::vector<Output> expectedOutput;
    for (int i = 0; i < 50; ++i) {
        expectedOutput.push_back({1, 1});
    }

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief This tests just outputs the default stream for a hierarchy of two levels where each node produces data
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepHierarchyTopologyTest, testSimpleQueryWithTwoLevelTreeWithDefaultSourceAndAllSensors) {
    struct Test {
        uint32_t key;
        uint32_t value;
    };

    auto testSchema =
        Schema::create()->addField("key", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query = R"(Query::from("test"))";
    TestHarness testHarness = TestHarness(query);

    testHarness.addMemorySource("test", testSchema, "test1");
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("test", testSchema, "test1");
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(3));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(3));

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2U);

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 0);
        testHarness.pushElement<Test>({1, 1}, 1);
        testHarness.pushElement<Test>({1, 1}, 2);
        testHarness.pushElement<Test>({1, 1}, 3);
        testHarness.pushElement<Test>({1, 1}, 4);
        testHarness.pushElement<Test>({1, 1}, 5);
    }

    struct Output {
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value); }
    };

    std::vector<Output> expectedOutput;
    for (int i = 0; i < 60; ++i) {
        expectedOutput.push_back({1, 1});
    }

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief  * @brief This tests just outputs the default stream for a hierarchy with one relay which does not produce data
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepHierarchyTopologyTest, testOutputAndNoSensors) {
    struct Test {
        uint32_t key;
        uint32_t value;
    };

    auto testSchema =
        Schema::create()->addField("key", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query = R"(Query::from("test"))";
    TestHarness testHarness = TestHarness(query);

    testHarness.addMemorySource("test", testSchema, "test1");
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    EXPECT_EQ(topology->getRoot()->getChildren().size(), 1U);
    EXPECT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 4U);

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 0);
        //testHarness.pushElement<Test>({1, 1}, 1);
        testHarness.pushElement<Test>({1, 1}, 2);
        testHarness.pushElement<Test>({1, 1}, 3);
        testHarness.pushElement<Test>({1, 1}, 4);
    }

    struct Output {
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value); }
    };

    std::vector<Output> expectedOutput;
    for (int i = 0; i < 40; ++i) {
        expectedOutput.push_back({1, 1});
    }

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief This tests just outputs the default stream for a hierarchy of two levels where only leaves produce data
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepHierarchyTopologyTest, testSimpleQueryWithTwoLevelTreeWithDefaultSourceAndWorker) {
    struct Test {
        uint32_t key;
        uint32_t value;
    };

    auto testSchema =
        Schema::create()->addField("key", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query = R"(Query::from("test"))";
    TestHarness testHarness = TestHarness(query);

    testHarness.addMemorySource("test", testSchema, "test1");
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));
    testHarness.addNonSourceWorker();
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(3));
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(3));

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2U);

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 0);
        testHarness.pushElement<Test>({1, 1}, 1);
        testHarness.pushElement<Test>({1, 1}, 2);
        //testHarness.pushElement<Test>({1, 1}, 3);
        testHarness.pushElement<Test>({1, 1}, 4);
        testHarness.pushElement<Test>({1, 1}, 5);
    }

    struct Output {
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value); }
    };

    std::vector<Output> expectedOutput;
    for (int i = 0; i < 50; ++i) {
        expectedOutput.push_back({1, 1});
    }

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief This tests just outputs the default stream for a hierarchy of three levels where only leaves produce data
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
TEST_F(DeepHierarchyTopologyTest, DISABLED_testSimpleQueryWithThreeLevelTreeWithDefaultSourceAndWorker) {
    CoordinatorConfigPtr crdConf = CoordinatorConfig::create();
    WorkerConfigPtr wrkConf = WorkerConfig::create();
    SourceConfigPtr srcConf = SourceConfig::create();

    crdConf->setRpcPort(rpcPort);
    crdConf->setRestPort(restPort);
    wrkConf->setCoordinatorPort(rpcPort);

    NES_DEBUG("DeepTopologyHierarchyTest: Start coordinator");
    crdConf->setNumberOfSlots(12);
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(crdConf);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("DeepTopologyHierarchyTest: Coordinator started successfully");
    uint64_t crdTopologyNodeId = crd->getTopology()->getRoot()->getId();

    /**
     * Worker
     */
    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 1");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 10);
    wrkConf->setDataPort(port + 11);
    wrkConf->setNumberOfSlots(1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);//id=2
    EXPECT_TRUE(retStart1);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 1 started successfully");

    TopologyNodeId wrk1TopologyNodeId = wrk1->getTopologyNodeId();
    ASSERT_NE(wrk1TopologyNodeId, INVALID_TOPOLOGY_NODE_ID);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 2");
    wrkConf->resetWorkerOptions();
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 20);
    wrkConf->setDataPort(port + 21);
    wrkConf->setNumberOfSlots(12);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(crdTopologyNodeId, wrk1TopologyNodeId);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 2 started successfully");

    TopologyNodeId wrk2TopologyNodeId = wrk2->getTopologyNodeId();
    ASSERT_NE(wrk2TopologyNodeId, INVALID_TOPOLOGY_NODE_ID);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 3");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 30);
    wrkConf->setDataPort(port + 31);
    wrkConf->setNumberOfSlots(12);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(crdTopologyNodeId, wrk1TopologyNodeId);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 3 started successfully");

    TopologyNodeId wrk3TopologyNodeId = wrk3->getTopologyNodeId();
    ASSERT_NE(wrk3TopologyNodeId, INVALID_TOPOLOGY_NODE_ID);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 4");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 40);
    wrkConf->setDataPort(port + 41);
    wrkConf->setNumberOfSlots(12);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 4 started successfully");

    TopologyNodeId wrk4TopologyNodeId = wrk4->getTopologyNodeId();
    ASSERT_NE(wrk4TopologyNodeId, INVALID_TOPOLOGY_NODE_ID);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 5");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 60);
    wrkConf->setDataPort(port + 61);
    wrkConf->setNumberOfSlots(12);
    NesWorkerPtr wrk5 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart5 = wrk5->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart5);
    wrk5->replaceParent(crdTopologyNodeId, wrk4TopologyNodeId);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 5 started successfully");

    TopologyNodeId wrk5TopologyNodeId = wrk5->getTopologyNodeId();
    ASSERT_NE(wrk5TopologyNodeId, INVALID_TOPOLOGY_NODE_ID);

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 6");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 70);
    wrkConf->setDataPort(port + 71);
    wrkConf->setNumberOfSlots(12);
    NesWorkerPtr wrk6 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Worker);
    bool retStart6 = wrk6->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart6);
    wrk6->replaceParent(crdTopologyNodeId, wrk4TopologyNodeId);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 6 started successfully");

    TopologyNodeId wrk6TopologyNodeId = wrk6->getTopologyNodeId();
    ASSERT_NE(wrk6TopologyNodeId, INVALID_TOPOLOGY_NODE_ID);

    /**
     * Sensors
     */
    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 7");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 80);
    wrkConf->setDataPort(port + 81);
    wrkConf->setNumberOfSlots(12);
    NesWorkerPtr wrk7 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart7 = wrk7->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart7);
    wrk7->replaceParent(crdTopologyNodeId, wrk5TopologyNodeId);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 7 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 8");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 90);
    wrkConf->setDataPort(port + 91);
    wrkConf->setNumberOfSlots(12);
    NesWorkerPtr wrk8 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart8 = wrk8->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart8);
    wrk8->replaceParent(crdTopologyNodeId, wrk6TopologyNodeId);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 8 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 9");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 100);
    wrkConf->setDataPort(port + 101);
    wrkConf->setNumberOfSlots(12);
    NesWorkerPtr wrk9 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart9 = wrk9->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart9);
    wrk9->replaceParent(crdTopologyNodeId, wrk2TopologyNodeId);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 9 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: Start worker 10");
    wrkConf->setCoordinatorPort(port);
    wrkConf->setRpcPort(port + 110);
    wrkConf->setDataPort(port + 111);
    wrkConf->setNumberOfSlots(12);
    NesWorkerPtr wrk10 = std::make_shared<NesWorker>(wrkConf, NesNodeType::Sensor);
    bool retStart10 = wrk10->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart10);
    wrk10->replaceParent(crdTopologyNodeId, wrk3TopologyNodeId);
    NES_DEBUG("DeepTopologyHierarchyTest: Worker 10 started successfully");

    NES_DEBUG("DeepTopologyHierarchyTest: topology: \n" << crd->getTopology()->toString());

    // Check if the topology matches the expected hierarchy
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 1U);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[0]->getChildren()[1]->getChildren().size(), 1U);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren().size(), 2U);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[0]->getChildren().size(), 1U);
    ASSERT_EQ(crd->getTopology()->getRoot()->getChildren()[1]->getChildren()[1]->getChildren().size(), 1U);

    std::string outputFilePath = "testOutput.out";
    remove(outputFilePath.c_str());

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");

    NES_DEBUG("DeepTopologyHierarchyTest: Submit query");
    string query = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath
        + R"(", "CSV_FORMAT", "APPEND"));)";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER,default_logical$value:INTEGER\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("DeepTopologyHierarchyTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 5");
    bool retStopWrk5 = wrk5->stop(true);
    EXPECT_TRUE(retStopWrk5);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 6");
    bool retStopWrk6 = wrk6->stop(true);
    EXPECT_TRUE(retStopWrk6);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 7");
    bool retStopWrk7 = wrk7->stop(true);
    EXPECT_TRUE(retStopWrk7);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 8");
    bool retStopWrk8 = wrk8->stop(true);
    EXPECT_TRUE(retStopWrk8);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 9");
    bool retStopWrk9 = wrk9->stop(true);
    EXPECT_TRUE(retStopWrk9);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop worker 10");
    bool retStopWrk10 = wrk10->stop(true);
    EXPECT_TRUE(retStopWrk10);

    NES_DEBUG("DeepTopologyHierarchyTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("DeepTopologyHierarchyTest: Test finished");
}

/**
 * @brief This tests applies project and selection on a three level hierarchy
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
TEST_F(DeepHierarchyTopologyTest, testSelectProjectThreeLevel) {
    struct Test {
        uint64_t val1;
        uint64_t val2;
        uint64_t val3;
    };

    auto testSchema = Schema::create()
                          ->addField("val1", DataTypeFactory::createUInt64())
                          ->addField("val2", DataTypeFactory::createUInt64())
                          ->addField("val3", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query = R"(Query::from("testStream").filter(Attribute("val1") < 3).project(Attribute("val3")))";
    TestHarness testHarness = TestHarness(query);

    SourceConfigPtr srcConf = SourceConfig::create();
    srcConf->resetSourceOptions();
    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/testCSV.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("testStream");
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);

    // Workers
    testHarness.addNonSourceWorker();                          // id=0
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// id=1
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// id=2
    testHarness.addNonSourceWorker();                          // id=3
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// id=4
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// id=5
    // Sensors
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(1));// id=6
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(2));// id=7
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(4));// id=8
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(5));// id=9

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
        uint64_t val3;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (val3 == rhs.val3); }
    };

    std::vector<Output> expectedOutput = {{3}, {4}, {3}, {4}, {3}, {4}, {3}, {4}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

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
TEST_F(DeepHierarchyTopologyTest, testWindowThreeLevel) {
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

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query);

    SourceConfigPtr srcConf = SourceConfig::create();
    srcConf->resetSourceOptions();
    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(3);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window");
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);

    // Workers
    testHarness.addNonSourceWorker();                          // id=0
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// id=1
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// id=2
    testHarness.addNonSourceWorker();                          // id=3
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// id=4
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// id=5
    // Sensors
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(1));// id=6
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(2));// id=7
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(4));// id=8
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(5));// id=9

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

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief This tests applies a unionWith on a three level hierarchy
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
TEST_F(DeepHierarchyTopologyTest, testUnionThreeLevel) {
    struct Test {
        uint64_t id;
        uint64_t value;
    };

    auto testSchema =
        Schema::create()->addField("id", DataTypeFactory::createUInt64())->addField("value", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query = R"(Query::from("car").unionWith(Query::from("truck")))";
    TestHarness testHarness = TestHarness(query);

    // Workers
    testHarness.addNonSourceWorker();                          // id=0
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// id=1
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// id=2
    testHarness.addNonSourceWorker();                          // id=3
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// id=4
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// id=5
    // Sensors
    testHarness.addMemorySource("truck", testSchema, "physical_truck", testHarness.getWorkerId(1));// id=6
    testHarness.addMemorySource("car", testSchema, "physical_car", testHarness.getWorkerId(2));    // id=7
    testHarness.addMemorySource("truck", testSchema, "physical_truck", testHarness.getWorkerId(4));// id=8
    testHarness.addMemorySource("car", testSchema, "physical_car", testHarness.getWorkerId(5));    // id=9

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 1U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren()[1]->getChildren().size(), 1U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren()[0]->getChildren().size(), 1U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren()[1]->getChildren().size(), 1U);

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 6);
        testHarness.pushElement<Test>({1, 1}, 7);
        testHarness.pushElement<Test>({1, 1}, 8);
        testHarness.pushElement<Test>({1, 1}, 9);
    }

    struct Output {
        uint64_t id;
        uint64_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id && value == rhs.value); }
    };

    std::vector<Output> expectedOutput;
    for (int i = 0; i < 40; ++i) {
        expectedOutput.push_back({1, 1});
    }

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief This tests just outputs the default stream for a hierarchy of two levels where only leaves produce data
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
TEST_F(DeepHierarchyTopologyTest, testSimpleQueryWithThreeLevelTreeWithWindowDataAndWorkerFinal) {
    struct Test {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto testSchema = Schema::create()
                          ->addField("value", DataTypeFactory::createUInt64())
                          ->addField("id", DataTypeFactory::createUInt64())
                          ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10).window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(query);

    SourceConfigPtr srcConf = SourceConfig::create();
    srcConf->resetSourceOptions();
    srcConf->setSourceType("CSVSource");
    srcConf->setSourceConfig("../tests/test_data/window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(5);
    srcConf->setNumberOfBuffersToProduce(3);
    srcConf->setPhysicalStreamName("test_stream");
    srcConf->setLogicalStreamName("window");
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);

    testHarness.addNonSourceWorker();
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(0));
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(0));
    testHarness.addNonSourceWorker();
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(3));
    testHarness.addCSVSource(conf, testSchema, testHarness.getWorkerId(3));

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2U);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (start == rhs.start && end == rhs.end && value == rhs.value); }
    };

    std::vector<Output> expectedOutput = {{0, 2000, 24}, {2000, 4000, 96}, {4000, 6000, 80}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//TODO:add join once it is implemented correctly
}// namespace NES
