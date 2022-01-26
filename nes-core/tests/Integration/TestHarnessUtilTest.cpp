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
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <Common/DataTypes/DataTypeFactory.hpp>
namespace NES {

using namespace Configurations;

class TestHarnessUtilTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("TestHarnessUtilTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup TestHarnessUtilTest test class.");
    }

    void SetUp() override {
        restPort = restPort + 2;
        rpcPort = rpcPort + 30;
    }

    static void TearDownTestCase() { NES_INFO("TestHarnessUtilTest test class TearDownTestCase."); }

    uint32_t restPort = 8080;
    uint32_t rpcPort = 4000;
};

/*
 * Testing testHarness utility using one logical source and one physical source
 */
TEST_F(TestHarnessUtilTest, testHarnessUtilWithSingleSource) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithFilterOperator = R"(Query::from("car").filter(Attribute("key") < 1000))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .pushElement<Car>({40, 40, 40}, 2)
                                  .pushElement<Car>({30, 30, 30}, 2)
                                  .pushElement<Car>({71, 71, 71}, 2)
                                  .pushElement<Car>({21, 21, 21}, 2)
                                  .validate()
                                  .setupTopology();

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };

    std::vector<Output> expectedOutput = {{40, 40, 40},
                                          {21, 21, 21},
                                          {
                                              30,
                                              30,
                                              30,
                                          },
                                          {71, 71, 71}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * Testing testHarness utility using one logical source and two physical sources
 */
TEST_F(TestHarnessUtilTest, testHarnessUtilWithTwoPhysicalSourceOfTheSameLogicalSource) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithFilterOperator = R"(Query::from("car").filter(Attribute("key") < 1000))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")//2
                                  .attachWorkerWithMemorySourceToCoordinator("car")//3
                                  .pushElement<Car>({40, 40, 40}, 2)
                                  .pushElement<Car>({30, 30, 30}, 2)
                                  .pushElement<Car>({71, 71, 71}, 3)
                                  .pushElement<Car>({21, 21, 21}, 3)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };

    std::vector<Output> expectedOutput = {{40, 40, 40},
                                          {21, 21, 21},
                                          {
                                              30,
                                              30,
                                              30,
                                          },
                                          {71, 71, 71}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * Testing testHarness utility using two logical source with one physical source each
 */
TEST_F(TestHarnessUtilTest, testHarnessUtilWithTwoPhysicalSourceOfDifferentLogicalSources) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    struct Truck {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    auto truckSchema = Schema::create()
                           ->addField("key", DataTypeFactory::createUInt32())
                           ->addField("value", DataTypeFactory::createUInt32())
                           ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Truck), truckSchema->getSchemaSizeInBytes());

    std::string queryWithFilterOperator = R"(Query::from("car").unionWith(Query::from("truck")))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .addLogicalSource("truck", truckSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .attachWorkerWithMemorySourceToCoordinator("truck")
                                  .pushElement<Car>({40, 40, 40}, 2)
                                  .pushElement<Car>({30, 30, 30}, 2)
                                  .pushElement<Truck>({71, 71, 71}, 3)
                                  .pushElement<Truck>({21, 21, 21}, 3)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };

    std::vector<Output> expectedOutput = {{40, 40, 40},
                                          {21, 21, 21},
                                          {
                                              30,
                                              30,
                                              30,
                                          },
                                          {71, 71, 71}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * Testing testHarness utility for query with a window operator
 */
TEST_F(TestHarnessUtilTest, testHarnessUtilWithWindowOperator) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithWindowOperator =
        R"(Query::from("car").window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1))).byKey(Attribute("key")).apply(Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")//2
                                  .attachWorkerWithMemorySourceToCoordinator("car")//3
                                  //Source1
                                  .pushElement<Car>({1, 1, 1000}, 2)
                                  .pushElement<Car>({12, 1, 1001}, 2)
                                  .pushElement<Car>({4, 1, 1002}, 2)
                                  .pushElement<Car>({1, 2, 2000}, 2)
                                  .pushElement<Car>({11, 2, 2001}, 2)
                                  .pushElement<Car>({16, 2, 2002}, 2)
                                  .pushElement<Car>({1, 3, 3000}, 2)
                                  .pushElement<Car>({11, 3, 3001}, 2)
                                  .pushElement<Car>({1, 3, 3003}, 2)
                                  .pushElement<Car>({1, 3, 3200}, 2)
                                  .pushElement<Car>({1, 4, 4000}, 2)
                                  .pushElement<Car>({1, 5, 5000}, 2)
                                  //Source2
                                  .pushElement<Car>({1, 1, 1000}, 3)
                                  .pushElement<Car>({12, 1, 1001}, 3)
                                  .pushElement<Car>({4, 1, 1002}, 3)
                                  .pushElement<Car>({1, 2, 2000}, 3)
                                  .pushElement<Car>({11, 2, 2001}, 3)
                                  .pushElement<Car>({16, 2, 2002}, 3)
                                  .pushElement<Car>({1, 3, 3000}, 3)
                                  .pushElement<Car>({11, 3, 3001}, 3)
                                  .pushElement<Car>({1, 3, 3003}, 3)
                                  .pushElement<Car>({1, 3, 3200}, 3)
                                  .pushElement<Car>({1, 4, 4000}, 3)
                                  .pushElement<Car>({1, 5, 5000}, 3)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end);
        }
    };

    std::vector<Output> expectedOutput = {
        {1000, 2000, 1, 2},
        {2000, 3000, 1, 0},
        {3000, 4000, 1, 4},
        {4000, 5000, 1, 0},
        {1000, 2000, 4, 2},
        {2000, 3000, 11, 4},
        {3000, 4000, 11, 0},
        {1000, 2000, 12, 2},
        {2000, 3000, 16, 4},
    };
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Testing testHarness utility for query with a join operator on different streams
 */
TEST_F(TestHarnessUtilTest, testHarnessWithJoinOperator) {
    struct Window1 {
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinOperator =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(queryWithJoinOperator, restPort, rpcPort)
                                  .addLogicalSource("window1", window1Schema)
                                  .addLogicalSource("window2", window2Schema)
                                  .attachWorkerWithMemorySourceToCoordinator("window1")
                                  .attachWorkerWithMemorySourceToCoordinator("window2")
                                  //Source1
                                  .pushElement<Window1>({1, 1000}, 2)
                                  .pushElement<Window2>({12, 1001}, 2)
                                  .pushElement<Window2>({4, 1002}, 2)
                                  .pushElement<Window2>({1, 2000}, 2)
                                  .pushElement<Window2>({11, 2001}, 2)
                                  .pushElement<Window2>({16, 2002}, 2)
                                  .pushElement<Window2>({1, 3000}, 2)
                                  //Source2
                                  .pushElement<Window2>({21, 1003}, 3)
                                  .pushElement<Window2>({12, 1011}, 3)
                                  .pushElement<Window2>({4, 1102}, 3)
                                  .pushElement<Window2>({4, 1112}, 3)
                                  .pushElement<Window2>({1, 2010}, 3)
                                  .pushElement<Window2>({11, 2301}, 3)
                                  .pushElement<Window2>({33, 3100}, 3)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint64_t _$start;
        uint64_t _$end;
        uint64_t _$key;
        uint64_t window1$id1;
        uint64_t window1$timestamp;
        uint64_t window2$id2;
        uint64_t window2$timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (_$start == rhs._$start && _$end == rhs._$end && _$key == rhs._$key && window1$id1 == rhs.window1$id1
                    && window1$timestamp == rhs.window1$timestamp && window2$id2 == rhs.window2$id2
                    && window2$timestamp == rhs.window2$timestamp);
        }
    };
    std::vector<Output> expectedOutput = {{1000, 2000, 4, 4, 1002, 4, 1102},
                                          {1000, 2000, 4, 4, 1002, 4, 1112},
                                          {1000, 2000, 12, 12, 1001, 12, 1011},
                                          {2000, 3000, 11, 11, 2001, 11, 2301}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * Testing testHarness utility for a query with map operator
 */
TEST_F(TestHarnessUtilTest, testHarnessOnQueryWithMapOperator) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithFilterOperator = R"(Query::from("car").map(Attribute("value") = Attribute("value") * Attribute("key")))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .pushElement<Car>({40, 40, 40}, 2)
                                  .pushElement<Car>({30, 30, 30}, 2)
                                  .pushElement<Car>({71, 71, 71}, 2)
                                  .pushElement<Car>({21, 21, 21}, 2)
                                  .validate()
                                  .setupTopology();

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };

    std::vector<Output> expectedOutput = {{40, 1600, 40},
                                          {21, 441, 21},
                                          {
                                              30,
                                              900,
                                              30,
                                          },
                                          {71, 5041, 71}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * Testing testHarness utility for a query with map operator
 */
TEST_F(TestHarnessUtilTest, testHarnesWithHiearchyInTopology) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    std::string queryWithFilterOperator = R"(Query::from("car").map(Attribute("value") = Attribute("value") * Attribute("key")))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator)
                                  .addLogicalSource("car", carSchema)
                                  /**
                                    * Expected topology:
                                        PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=65535, usedResource=0]
                                        |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
                                        |  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
                                        |  |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
                                        |  |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
                                    */
                                  .attachWorkerToCoordinator()                         //idx=2
                                  .attachWorkerToWorkerWithId(2)                       //idx=3
                                  .attachWorkerWithMemorySourceToWorkerWithId("car", 3)//idx=4
                                  .attachWorkerWithMemorySourceToWorkerWithId("car", 3)//idx=5
                                                                                       //Source1
                                  .pushElement<Car>({40, 40, 40}, 4)
                                  .pushElement<Car>({30, 30, 30}, 4)
                                  .pushElement<Car>({71, 71, 71}, 4)
                                  .pushElement<Car>({21, 21, 21}, 4)
                                  //Source2
                                  .pushElement<Car>({40, 40, 40}, 5)
                                  .pushElement<Car>({30, 30, 30}, 5)
                                  .pushElement<Car>({71, 71, 71}, 5)
                                  .pushElement<Car>({21, 21, 21}, 5)
                                  .validate()
                                  .setupTopology();

    TopologyPtr topology = testHarness.getTopology();
    NES_DEBUG("TestHarness: topology:\n" << topology->toString());
    EXPECT_EQ(topology->getRoot()->getChildren().size(), 1U);
    EXPECT_EQ(topology->getRoot()->getChildren()[0]->getChildren().size(), 1U);
    EXPECT_EQ(topology->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 2U);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };

    std::vector<Output> expectedOutput = {{40, 1600, 40},
                                          {21, 441, 21},
                                          {
                                              30,
                                              900,
                                              30,
                                          },
                                          {71, 5041, 71},
                                          {40, 1600, 40},
                                          {21, 441, 21},
                                          {
                                              30,
                                              900,
                                              30,
                                          },
                                          {71, 5041, 71}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * Testing test harness CSV source
 */
TEST_F(TestHarnessUtilTest, testHarnessCsvSource) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    // Content ov testCSV.csv:
    // 1,2,3
    // 1,2,4
    // 4,3,6
    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "testCSV.csv");
    csvSourceType->setSourceFrequency(1);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(1);
    csvSourceType->setSkipHeader(false);

    std::string queryWithFilterOperator = R"(Query::from("car").filter(Attribute("key") < 4))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  //register physical stream
                                  .attachWorkerWithCSVSourceToCoordinator("car", csvSourceType)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };
    std::vector<Output> expectedOutput = {{1, 2, 3}, {1, 2, 4}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * Testing test harness CSV source and memory source
 */
TEST_F(TestHarnessUtilTest, testHarnessCsvSourceAndMemorySource) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    CSVSourceTypePtr csvSourceType = CSVSourceType::create();
    // Content ov testCSV.csv:
    // 1,2,3
    // 1,2,4
    // 4,3,6
    csvSourceType->setFilePath(std::string(TEST_DATA_DIRECTORY) + "testCSV.csv");
    csvSourceType->setSourceFrequency(1);
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(1);
    csvSourceType->setSkipHeader(false);

    std::string queryWithFilterOperator = R"(Query::from("car").filter(Attribute("key") < 4))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  //register physical stream
                                  .attachWorkerWithCSVSourceToCoordinator("car", csvSourceType)//2
                                  // add a memory source
                                  .attachWorkerWithMemorySourceToCoordinator("car")//3
                                  // push two elements to the memory source
                                  .pushElement<Car>({1, 8, 8}, 3)
                                  .pushElement<Car>({1, 9, 9}, 3)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };
    std::vector<Output> expectedOutput = {{1, 2, 3}, {1, 2, 4}, {1, 9, 9}, {1, 8, 8}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * Testing test harness without source (should not work)
 */
TEST_F(TestHarnessUtilTest, testHarnessUtilWithNoSources) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    std::string queryWithFilterOperator = R"(Query::from("car").filter(Attribute("key") < 1000))";

    EXPECT_THROW(TestHarness(queryWithFilterOperator, restPort, rpcPort).validate().setupTopology(), Exception);
}

/*
 * Testing test harness pushing element to non-existent source (should not work)
 */
TEST_F(TestHarnessUtilTest, testHarnessUtilPushToNonExsistentSource) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    std::string queryWithFilterOperator = R"(Query::from("car").filter(Attribute("key") < 1000))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, restPort, rpcPort);

    ASSERT_EQ(testHarness.getWorkerCount(), 0UL);
    EXPECT_THROW(testHarness.pushElement<Car>({30, 30, 30}, 0), NesRuntimeException);
}

/*
 * Testing test harness pushing element push to wrong source (should not work)
 */
TEST_F(TestHarnessUtilTest, testHarnessUtilPushToWrongSource) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    struct Truck {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
        uint64_t length;
        uint64_t weight;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    auto truckSchema = Schema::create()
                           ->addField("key", DataTypeFactory::createUInt32())
                           ->addField("value", DataTypeFactory::createUInt32())
                           ->addField("timestamp", DataTypeFactory::createUInt64());

    std::string queryWithFilterOperator = R"(Query::from("car").unionWith(Query::from("truck")))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator, restPort, rpcPort)
                                  .addLogicalSource("car", carSchema)
                                  .addLogicalSource("truck", truckSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")  //2
                                  .attachWorkerWithMemorySourceToCoordinator("truck");//3


    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    EXPECT_THROW(testHarness.pushElement<Truck>({30, 30, 30, 30, 30}, 2), NesRuntimeException);
}

}// namespace NES