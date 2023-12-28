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
#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std;

namespace NES {

using namespace Configurations;

class DeepHierarchyTopologyTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("DeepTopologyHierarchyTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup DeepTopologyHierarchyTest test class.");
    }
};

/**
 * @brief This tests just outputs the default source for a hierarchy with one relay which also produces data by itself
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

    auto query = Query::from("test");
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("test", testSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test")     //idx=2
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //idx=3
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //idx=4
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //idx=5
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2);//idx=6

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 2)
            .pushElement<Test>({1, 1}, 3)
            .pushElement<Test>({1, 1}, 4)
            .pushElement<Test>({1, 1}, 5)
            .pushElement<Test>({1, 1}, 6);
    }

    testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    std::stringstream expectedOutput;
    for (int i = 0; i < 50; ++i) {
        expectedOutput << "1, 1\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief This tests just outputs the default source for a hierarchy of two levels where each node produces data
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

    auto query = Query::from("test");
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("test", testSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test")     //id=2
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //id=3
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //id=4
                           .attachWorkerWithMemorySourceToCoordinator("test")     //id=5
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 5) //id=6
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 5);//id=7

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 2)
            .pushElement<Test>({1, 1}, 3)
            .pushElement<Test>({1, 1}, 4)
            .pushElement<Test>({1, 1}, 5)
            .pushElement<Test>({1, 1}, 6)
            .pushElement<Test>({1, 1}, 7);
    }

    testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    std::stringstream expectedOutput;
    for (int i = 0; i < 60; ++i) {
        expectedOutput << "1, 1\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief This tests just outputs the default source for a hierarchy with one relay which does not produce data
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

    auto query = Query::from("test");
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("test", testSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test")     //2
                           .attachWorkerToWorkerWithId(2)                         //3
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //4
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //5
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2);//6

    for (int i = 0; i < 10; ++i) {
        testHarness
            .pushElement<Test>({1, 1}, 2)
            // worker with id 3 does not produce data
            .pushElement<Test>({1, 1}, 4)
            .pushElement<Test>({1, 1}, 5)
            .pushElement<Test>({1, 1}, 6);
    }

    testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    std::stringstream expectedOutput;
    for (int i = 0; i < 40; ++i) {
        expectedOutput << "1, 1\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief This tests just outputs the default source for a hierarchy of two levels where only leaves produce data
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

    auto query = Query::from("test");
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("test", testSchema)
                           .attachWorkerToCoordinator()                           //2
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //3
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 2) //4
                           .attachWorkerToCoordinator()                           //5
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 5) //6
                           .attachWorkerWithMemorySourceToWorkerWithId("test", 5);//7

    for (int i = 0; i < 10; ++i) {
        // worker with idx 0 does not produce data
        testHarness.pushElement<Test>({1, 1}, 3)
            .pushElement<Test>({1, 1}, 4)
            // worker with idx 3 does not produce data
            .pushElement<Test>({1, 1}, 6)
            .pushElement<Test>({1, 1}, 7);
    }

    testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    std::stringstream expectedOutput;
    for (int i = 0; i < 40; ++i) {
        expectedOutput << "1, 1\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief This tests just outputs the default source for a hierarchy of three levels where only leaves produce data
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

    auto query = Query::from("test");

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
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
        testHarness.pushElement<Test>({1, 1}, 8)
            .pushElement<Test>({1, 1}, 9)
            .pushElement<Test>({1, 1}, 10)
            .pushElement<Test>({1, 1}, 11);
    }

    testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    std::stringstream expectedOutput;
    for (int i = 0; i < 40; ++i) {
        expectedOutput << "1, 1\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
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

    std::vector<CSVSourceTypePtr> csvSourceTypes;
    for (uint64_t i = 0; i < 4; i++) {
        CSVSourceTypePtr csvSourceType = CSVSourceType::create("testStream", "testStream1");
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "testCSV.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
        csvSourceTypes.emplace_back(csvSourceType);
    }

    auto query = Query::from("testStream").filter(Attribute("val1") < 3).project(Attribute("val3"));

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                                  .addLogicalSource("testStream", testSchema)
                                  // Workers
                                  .attachWorkerToCoordinator()  // id=2
                                  .attachWorkerToWorkerWithId(2)// id=3
                                  .attachWorkerToWorkerWithId(2)// id=4
                                  .attachWorkerToCoordinator()  // id=5
                                  .attachWorkerToWorkerWithId(5)// id=6
                                  .attachWorkerToWorkerWithId(5)// id=7
                                  // Sensors
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(0), 3)// id=8
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(1), 4)// id=9
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(2), 6)// id=10
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(3), 7)// id=11
                                  .validate()
                                  .setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    const auto expectedOutput = "3\n"
                                "4\n"
                                "3\n"
                                "4\n"
                                "3\n"
                                "4\n"
                                "3\n"
                                "4\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
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
TEST_F(DeepHierarchyTopologyTest, DISABLED_testDistributedWindowThreeLevel) {
    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](CoordinatorConfigurationPtr config) {
        config->optimizer.distributedWindowChildThreshold.setValue(0);
        config->optimizer.distributedWindowCombinerThreshold.setValue(0);
    };

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

    std::vector<CSVSourceTypePtr> csvSourceTypes;
    for (uint64_t i = 0; i < 4; i++) {
        auto csvSourceType = CSVSourceType::create("window", "window" + std::to_string(i));
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
        csvSourceType->setNumberOfBuffersToProduce(3);
        csvSourceTypes.emplace_back(csvSourceType);
    }

    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));
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
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(0), 3)// id=8
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(1), 4)// id=9
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(2), 6)// id=10
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(3), 7)// id=11
                                  .validate()
                                  .setupTopology(crdFunctor);

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    const auto expectedOutput = "1000, 2000, 1, 68\n"
                                "2000, 3000, 2, 112\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
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
TEST_F(DeepHierarchyTopologyTest, DISABLED_testDistributedWindowThreeLevelNemoPlacement) {
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

    std::vector<CSVSourceTypePtr> csvSourceTypes;
    for (uint64_t i = 0; i < 4; i++) {
        auto csvSourceType = CSVSourceType::create("window", "window" + std::to_string(i));
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
        csvSourceType->setNumberOfBuffersToProduce(3);
        csvSourceTypes.emplace_back(csvSourceType);
    }

    std::function<void(CoordinatorConfigurationPtr)> crdFunctor = [](CoordinatorConfigurationPtr config) {
        config->optimizer.enableNemoPlacement.setValue(true);
        config->optimizer.distributedWindowCombinerThreshold.setValue(100);
        config->optimizer.distributedWindowChildThreshold.setValue(100);
    };

    for (uint64_t i = 0; i < workerNo; i++) {
        auto workerConfig = WorkerConfiguration::create();
        workerConfig->queryCompiler.windowingStrategy.setValue(QueryCompilation::WindowingStrategy::SLICING);
        workerConfigs.emplace_back(workerConfig);
    }

    uint64_t i = 0;
    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("ts")), Seconds(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));
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
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(0), 3)// id=8
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(1), 4)// id=9
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(2), 6)// id=10
                                  .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(3), 7)// id=11
                                  .validate()
                                  .setupTopology(crdFunctor);

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    const auto expectedOutput = "1000, 2000, 1, 68\n"
                                "2000, 3000, 2, 112\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
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

    auto query = Query::from("car").unionWith(Query::from("truck"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

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
        testHarness.pushElement<Test>({1, 1}, 8)
            .pushElement<Test>({1, 1}, 9)
            .pushElement<Test>({1, 1}, 10)
            .pushElement<Test>({1, 1}, 11);
    }

    testHarness.validate().setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    std::stringstream expectedOutput;
    for (int i = 0; i < 40; ++i) {
        expectedOutput << "1, 1\n";
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief This tests just outputs the default source for a hierarchy of two levels where only leaves produce data
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

    std::vector<CSVSourceTypePtr> csvSourceTypes;
    for (uint64_t i = 0; i < 4; i++) {
        auto csvSourceType = CSVSourceType::create("window", "window" + std::to_string(i));
        csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
        csvSourceType->setNumberOfTuplesToProducePerBuffer(5);
        csvSourceType->setNumberOfBuffersToProduce(3);
        csvSourceTypes.emplace_back(csvSourceType);
    }

    auto query = Query::from("window")
                     .filter(Attribute("id") < 15)
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")))
                     .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")))
                     .filter(Attribute("id") < 10)
                     .window(TumblingWindow::of(EventTime(Attribute("start")), Seconds(2)))
                     .apply(Sum(Attribute("value")));

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("window", testSchema)
                           .attachWorkerToCoordinator()                                     //2
                           .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(0), 2)//3
                           .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(1), 2)//4
                           .attachWorkerToCoordinator()                                     //5
                           .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(2), 5)//6
                           .attachWorkerWithCSVSourceToWorkerWithId(csvSourceTypes.at(3), 5)//7
                           .validate()
                           .setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:{}\n", topology->toString());

    // Expected output
    const auto expectedOutput = "0, 2000, 16\n"
                                "2000, 4000, 96\n"
                                "4000, 6000, 80\n"
                                "6000, 8000, 112\n"
                                "8000, 10000, 32\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createDynamicBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

//TODO:add join once it is implemented correctly
}// namespace NES
