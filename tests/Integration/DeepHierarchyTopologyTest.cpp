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

    testHarness.addMemorySource("test", testSchema, "test1");                            //idx=0
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));//idx=1
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));//idx=2
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));//idx=3
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));//idx=4

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

    testHarness.addMemorySource("test", testSchema, "test1");                            // idx=0
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));//idx=1
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));//idx=2
    testHarness.addMemorySource("test", testSchema, "test1");                            //idx=3
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(3));//idx=4
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(3));//idx=5

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

    testHarness.addMemorySource("test", testSchema, "test1");                            //idx=0
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));                          // idx=1
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));//idx=2
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));//idx=3
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));//idx=4

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    EXPECT_EQ(topology->getRoot()->getChildren().size(), 1U);
    EXPECT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 4U);

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 0);
        // worker with idx 1 does not produce data
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

    testHarness.addNonSourceWorker();                                                    // idx = 0
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));// idx = 1
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(0));// idx = 2
    testHarness.addNonSourceWorker();                                                    // idx = 3
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(3));// idx = 4
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(3));// idx = 5

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    ASSERT_EQ(topology->getRoot()->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(topology->getRoot()->getChildren()[1]->getChildren().size(), 2U);

    for (int i = 0; i < 10; ++i) {
        // worker with idx 0 does not produce data
        testHarness.pushElement<Test>({1, 1}, 1);
        testHarness.pushElement<Test>({1, 1}, 2);
        // worker with idx 3 does not produce data
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
    for (int i = 0; i < 40; ++i) {
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
TEST_F(DeepHierarchyTopologyTest, testSimpleQueryWithThreeLevelTreeWithDefaultSourceAndWorker) {
    struct Test {
        uint32_t key;
        uint32_t value;
    };

    auto testSchema =
        Schema::create()->addField("key", DataTypeFactory::createUInt32())->addField("value", DataTypeFactory::createUInt32());

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    std::string query = R"(Query::from("test"))";
    TestHarness testHarness = TestHarness(query);

    // Workers
    testHarness.addNonSourceWorker();                          // idx = 0
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// idx = 1
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// idx = 2
    testHarness.addNonSourceWorker();                          // idx = 3
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// idx = 4
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// idx = 5
    // Sensors
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(1));// idx = 6
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(2));// idx = 7
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(4));// idx = 8
    testHarness.addMemorySource("test", testSchema, "test1", testHarness.getWorkerId(5));// idx = 9

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
        // worker with idx 0-5 do not produce data
        testHarness.pushElement<Test>({1, 1}, 6);
        testHarness.pushElement<Test>({1, 1}, 7);
        testHarness.pushElement<Test>({1, 1}, 8);
        testHarness.pushElement<Test>({1, 1}, 9);
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
    testHarness.addNonSourceWorker();                          // idx=0
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// idx=1
    testHarness.addNonSourceWorker(testHarness.getWorkerId(0));// idx=2
    testHarness.addNonSourceWorker();                          // idx=3
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// idx=4
    testHarness.addNonSourceWorker(testHarness.getWorkerId(3));// idx=5
    // Sensors
    testHarness.addMemorySource("truck", testSchema, "physical_truck", testHarness.getWorkerId(1));// idx=6
    testHarness.addMemorySource("car", testSchema, "physical_car", testHarness.getWorkerId(2));    // idx=7
    testHarness.addMemorySource("truck", testSchema, "physical_truck", testHarness.getWorkerId(4));// idx=8
    testHarness.addMemorySource("car", testSchema, "physical_car", testHarness.getWorkerId(5));    // idx=9

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
        // worker with idx 0-5 do not produce data
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
