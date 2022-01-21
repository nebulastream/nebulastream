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

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/WorkerConfiguration.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>

using namespace std;

namespace NES {

using namespace Configurations;

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
    auto testHarness = TestHarness(query)
                           .addLogicalSource("test", testSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test")     //idx=2
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //idx=3
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //idx=4
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //idx=5
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2);//idx=6

    for (int i = 0; i < 10; ++i) {
        testHarness = testHarness.pushElement<Test>({1, 1}, 2)
                          .pushElement<Test>({1, 1}, 3)
                          .pushElement<Test>({1, 1}, 4)
                          .pushElement<Test>({1, 1}, 5)
                          .pushElement<Test>({1, 1}, 6);
    }

    testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    EXPECT_EQ(topology->getRoot()->getChildren().size(), 1U);
    EXPECT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 4U);

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

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

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
    auto testHarness = TestHarness(query)
                           .addLogicalSource("test", testSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test")     //id=2
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //id=3
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //id=4
                           .attachWorkerWithMemorySourceToCoordinator("test")     //id=5
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 5) //id=6
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 5);//id=7

    for (int i = 0; i < 10; ++i) {
        testHarness = testHarness.pushElement<Test>({1, 1}, 2)
                          .pushElement<Test>({1, 1}, 3)
                          .pushElement<Test>({1, 1}, 4)
                          .pushElement<Test>({1, 1}, 5)
                          .pushElement<Test>({1, 1}, 6)
                          .pushElement<Test>({1, 1}, 7);
    }

    testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2U);

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

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * @brief This tests just outputs the default stream for a hierarchy with one relay which does not produce data
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
    auto testHarness = TestHarness(query)
                           .addLogicalSource("test", testSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test")     //2
                           .attachWorkerToWorkerWithId(2)                         //3
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //4
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //5
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2);//6

    for (int i = 0; i < 10; ++i) {
        testHarness = testHarness
                          .pushElement<Test>({1, 1}, 2)
                          // worker with id 3 does not produce data
                          .pushElement<Test>({1, 1}, 4)
                          .pushElement<Test>({1, 1}, 5)
                          .pushElement<Test>({1, 1}, 6);
    }

    testHarness = testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    EXPECT_EQ(topology->getRoot()->getChildren().size(), 1U);
    EXPECT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 4U);

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

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

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
    auto testHarness = TestHarness(query)
                           .addLogicalSource("test", testSchema)
                           .attachWorkerToCoordinator()                           //2
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //3
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //4
                           .attachWorkerToCoordinator()                           //5
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 5) //6
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 5);//7

    for (int i = 0; i < 10; ++i) {
        // worker with idx 0 does not produce data
        testHarness = testHarness.pushElement<Test>({1, 1}, 3)
                          .pushElement<Test>({1, 1}, 4)
                          // worker with idx 3 does not produce data
                          .pushElement<Test>({1, 1}, 6)
                          .pushElement<Test>({1, 1}, 7);
    }

    testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2U);

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

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

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
TEST_F(DeepHierarchyTopologyTest, testSimpleQueryWithThreeLevelTreeWithDefaultSourceAndWorker) {
    struct Test {
        uint32_t key;
        uint32_t value;
    };

    auto testSchema =
        Schema::create()->addField("key", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query = R"(Query::from("test"))";

    auto testHarness = TestHarness(query)
                           .addLogicalSource("test", testSchema)
                           // Workers
                           .attachWorkerToCoordinator()  //2
                           .attachWorkerToWorkerWithId(2)//3
                           .attachWorkerToWorkerWithId(2)//4
                           .attachWorkerToCoordinator()  //5
                           .attachWorkerToWorkerWithId(5)//6
                           .attachWorkerToWorkerWithId(5)//7
                           // Sensors
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 3) //8
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 4) //9
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 6) //10
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 7);//11

    for (int i = 0; i < 10; ++i) {
        // worker with idx 1-7 do not produce data
        testHarness = testHarness.pushElement<Test>({1, 1}, 8)
                          .pushElement<Test>({1, 1}, 9)
                          .pushElement<Test>({1, 1}, 10)
                          .pushElement<Test>({1, 1}, 11);
    }

    testHarness.validate().setupTopology();

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
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value); }
    };

    std::vector<Output> expectedOutput;
    for (int i = 0; i < 40; ++i) {
        expectedOutput.push_back({1, 1});
    }

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
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

    CSVSourceTypePtr srcConf = CSVSourceType::create();
    srcConf->setFilePath(std::string(TEST_DATA_DIRECTORY) + "testCSV.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    PhysicalSourcePtr conf = PhysicalSource::create("testStream", "test_stream", srcConf);

    std::string query = R"(Query::from("testStream").filter(Attribute("val1") < 3).project(Attribute("val3")))";

    TestHarness testHarness = TestHarness(query)
                                  .addLogicalSource("testStream", testSchema)
                                  // Workers
                                  .attachWorkerToCoordinator()  // id=2
                                  .attachWorkerToWorkerWithId(2)// id=3
                                  .attachWorkerToWorkerWithId(2)// id=4
                                  .attachWorkerToCoordinator()  // id=5
                                  .attachWorkerToWorkerWithId(5)// id=6
                                  .attachWorkerToWorkerWithId(5)// id=7
                                  // Sensors
                                  .attachWorkerWithCSVSourceToWorkerWithId(conf, 3)// id=8
                                  .attachWorkerWithCSVSourceToWorkerWithId(conf, 4)// id=9
                                  .attachWorkerWithCSVSourceToWorkerWithId(conf, 6)// id=10
                                  .attachWorkerWithCSVSourceToWorkerWithId(conf, 7)// id=11
                                  .validate()
                                  .setupTopology();

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

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

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

    CSVSourceTypePtr srcConf = CSVSourceType::create();
    srcConf->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->setNumberOfBuffersToProduce(3);
    PhysicalSourcePtr conf = PhysicalSource::create("window", "test_stream", srcConf);

    std::string query =
        R"(Query::from("window").window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value"))))";

    TestHarness testHarness = TestHarness(query)
                                  .addLogicalSource("window", testSchema)
                                  // Workers
                                  .attachWorkerToCoordinator()  // id=2
                                  .attachWorkerToWorkerWithId(2)// id=3
                                  .attachWorkerToWorkerWithId(2)// id=4
                                  .attachWorkerToCoordinator()  // id=5
                                  .attachWorkerToWorkerWithId(5)// id=6
                                  .attachWorkerToWorkerWithId(5)// id=7
                                  // Sensors
                                  .attachWorkerWithCSVSourceToWorkerWithId(conf, 3)// id=8
                                  .attachWorkerWithCSVSourceToWorkerWithId(conf, 4)// id=9
                                  .attachWorkerWithCSVSourceToWorkerWithId(conf, 6)// id=10
                                  .attachWorkerWithCSVSourceToWorkerWithId(conf, 7)// id=11
                                  .validate()
                                  .setupTopology();

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
    TestHarness testHarness = TestHarness(query)
                                  .addLogicalSource("truck", testSchema)
                                  .addLogicalSource("car", testSchema)
                                  // Workers
                                  .attachWorkerToCoordinator()  // idx=2
                                  .attachWorkerToWorkerWithId(2)// idx=3
                                  .attachWorkerToWorkerWithId(2)// idx=4
                                  .attachWorkerToCoordinator()  // idx=5
                                  .attachWorkerToWorkerWithId(5)// idx=6
                                  .attachWorkerToWorkerWithId(5)// idx=7
                                  // Sensors
                                  .attachWorkerWithMemorySourceToWorkerWithId("truck", 3)// idx=8
                                  .attachWorkerWithMemorySourceToWorkerWithId("car", 4)  // idx=9
                                  .attachWorkerWithMemorySourceToWorkerWithId("truck", 6)// idx=10
                                  .attachWorkerWithMemorySourceToWorkerWithId("car", 7); // idx=11

    for (int i = 0; i < 10; ++i) {
        // worker with idx 0-5 do not produce data
        testHarness = testHarness.pushElement<Test>({1, 1}, 8)
                          .pushElement<Test>({1, 1}, 9)
                          .pushElement<Test>({1, 1}, 10)
                          .pushElement<Test>({1, 1}, 11);
    }

    testHarness.validate().setupTopology();

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
        uint64_t id;
        uint64_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (id == rhs.id && value == rhs.value); }
    };

    std::vector<Output> expectedOutput;
    for (int i = 0; i < 40; ++i) {
        expectedOutput.push_back({1, 1});
    }

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

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

    CSVSourceTypePtr srcConf = CSVSourceType::create();
    srcConf->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    srcConf->setNumberOfTuplesToProducePerBuffer(5);
    srcConf->setNumberOfBuffersToProduce(3);
    PhysicalSourcePtr conf = PhysicalSource::create("window", "test_stream", srcConf);

    std::string query = R"(Query::from("window")
        .filter(Attribute("id") < 15)
        .window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(1))).byKey(Attribute("id")).apply(Sum(Attribute("value")))
        .filter(Attribute("id") < 10).window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2))).apply(Sum(Attribute("value"))))";

    auto testHarness = TestHarness(query)
                           .addLogicalSource("window", testSchema)
                           .attachWorkerToCoordinator()                     //2
                           .attachWorkerWithCSVSourceToWorkerWithId(conf, 2)//3
                           .attachWorkerWithCSVSourceToWorkerWithId(conf, 2)//4
                           .attachWorkerToCoordinator()                     //5
                           .attachWorkerWithCSVSourceToWorkerWithId(conf, 5)//6
                           .attachWorkerWithCSVSourceToWorkerWithId(conf, 5)//7
                           .validate()
                           .setupTopology();

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

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//TODO:add join once it is implemented correctly
}// namespace NES
