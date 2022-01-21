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
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-copy-dtor"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#pragma clang diagnostic pop
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <iostream>

using namespace std;

namespace NES {

class ComplexSequenceTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("ComplexSequenceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ComplexSequenceTest test class.");
    }

    void SetUp() override {
        restPort = restPort + 2;
        rpcPort = rpcPort + 30;
    }

    void TearDown() override { std::cout << "Tear down ComplexSequenceTest class." << std::endl; }

    uint32_t restPort = 8080;
    uint32_t rpcPort = 4000;
};

/*
 * @brief Test a query with a single window operator and a single join operator running on a single node
 */
TEST_F(ComplexSequenceTest, DISABLED_complexTestSingleNodeSingleWindowSingleJoin) {
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
        R"(Query::from("window1")
            .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
            .window(TumblingWindow::of(EventTime(Attribute("window1$timestamp")), Seconds(2))).byKey(Attribute("window1window2$key")).apply(Sum(Attribute("window1$id1")))
        )";
    TestHarness testHarness = TestHarness(queryWithJoinOperator, restPort, rpcPort);

    testHarness.attachWorkerWithMemorySourceToCoordinator("window1", window1Schema, "window1");
    testHarness.attachWorkerWithMemorySourceToCoordinator("window2", window2Schema, "window2");

    ASSERT_EQ(testHarness.getWorkerCount(), 2u);

    testHarness.pushElement<Window1>({1u, 1000u}, 0u);
    testHarness.pushElement<Window2>({12u, 1001u}, 0u);
    testHarness.pushElement<Window2>({4u, 1002u}, 0u);
    testHarness.pushElement<Window2>({1u, 2000u}, 0u);
    testHarness.pushElement<Window2>({11u, 2001u}, 0u);
    testHarness.pushElement<Window2>({16u, 2002u}, 0u);
    testHarness.pushElement<Window2>({1u, 3000u}, 0u);

    testHarness.pushElement<Window2>({21u, 1003u}, 1u);
    testHarness.pushElement<Window2>({12u, 1011u}, 1u);
    testHarness.pushElement<Window2>({4u, 1102u}, 1u);
    testHarness.pushElement<Window2>({4u, 1112u}, 1u);
    testHarness.pushElement<Window2>({1u, 2010u}, 1u);
    testHarness.pushElement<Window2>({11u, 2301u}, 1u);
    testHarness.pushElement<Window2>({33u, 3100u}, 1u);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        uint64_t window1window2$key;
        uint64_t window1$id1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window1$id1 == rhs.window1$id1);
        }
    };

    std::vector<Output> expectedOutput = {{0, 2000, 4, 8}, {0, 2000, 12, 12}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//* Topology:
//PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=65535, usedResource=0]
//|--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
/*
 * @brief Test a query with a single window operator and a single join operator running on a single node
 */
TEST_F(ComplexSequenceTest, complexTestDistributedNodeSingleWindowSingleJoin) {
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
        R"(Query::from("window1")
            .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
            .window(TumblingWindow::of(EventTime(Attribute("window1$timestamp")), Seconds(1))).byKey(Attribute("window1window2$key")).apply(Sum(Attribute("window1$id1")))
        )";
    TestHarness testHarness = TestHarness(queryWithJoinOperator, restPort, rpcPort);
    testHarness.attachWorkerToCoordinator();
    testHarness.attachWorkerToCoordinator();
    testHarness.attachWorkerWithMemorySourceToWorkerWithId("window1", window1Schema, "window1", testHarness.getWorkerId(0));
    testHarness.attachWorkerWithMemorySourceToWorkerWithId("window2", window2Schema, "window2", testHarness.getWorkerId(1));

    ASSERT_EQ(testHarness.getWorkerCount(), 4UL);

    // Check if the topology matches the expected hierarchy
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren().size(), 2ULL);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren().size(), 1ULL);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[1]->getChildren().size(), 1ULL);

    testHarness.pushElement<Window1>({1UL, 1000UL}, 2UL);
    testHarness.pushElement<Window2>({12UL, 1001UL}, 2UL);
    testHarness.pushElement<Window2>({4UL, 1002UL}, 2UL);
    testHarness.pushElement<Window2>({1UL, 2000UL}, 2UL);
    testHarness.pushElement<Window2>({11UL, 2001UL}, 2UL);
    testHarness.pushElement<Window2>({16UL, 2002UL}, 2UL);
    testHarness.pushElement<Window2>({1UL, 3000UL}, 2UL);

    testHarness.pushElement<Window2>({21UL, 1003UL}, 3UL);
    testHarness.pushElement<Window2>({12UL, 1011UL}, 3UL);
    testHarness.pushElement<Window2>({4UL, 1102UL}, 3UL);
    testHarness.pushElement<Window2>({4UL, 1112UL}, 3UL);
    testHarness.pushElement<Window2>({1UL, 2010UL}, 3UL);
    testHarness.pushElement<Window2>({11UL, 2301UL}, 3UL);
    testHarness.pushElement<Window2>({33UL, 3100UL}, 3UL);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        uint64_t window1window2$key;
        uint64_t window1$id1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window1$id1 == rhs.window1$id1);
        }
    };

    std::vector<Output> expectedOutput = {{1000UL, 2000UL, 4UL, 8UL}, {1000UL, 2000UL, 12UL, 12UL}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test a query with a multiple window operator and a multiple join operator running on a single node
 * Placement:
 * ExecutionNode(id:1, ip:127.0.0.1, topologyId:1)
 *  | QuerySubPlan(queryId:1, querySubPlanId:1)
 *  |  SINK(13)
 *  |    CENTRALWINDOW(14)
 *  |      WATERMARKASSIGNER(11)
 *  |        CENTRALWINDOW(15)
 *  |          WATERMARKASSIGNER(9)
 *  |            Join(8)
 *  |              Join(5)
 *  |                WATERMARKASSIGNER(3)
 *  |                  SOURCE(18,)
 *  |                WATERMARKASSIGNER(4)
 *  |                  SOURCE(20,)
 *  |              WATERMARKASSIGNER(7)
 *  |                SOURCE(16,)
 *  |--ExecutionNode(id:4, ip:127.0.0.1, topologyId:4)
 *  |  | QuerySubPlan(queryId:1, querySubPlanId:4)
 *  |  |  SINK(17)
 *  |  |    SOURCE(6,window3)
 *  |--ExecutionNode(id:2, ip:127.0.0.1, topologyId:2)
 *  |  | QuerySubPlan(queryId:1, querySubPlanId:2)
 *  |  |  SINK(19)
 *  |  |    SOURCE(1,window1)
 *  |--ExecutionNode(id:3, ip:127.0.0.1, topologyId:3)
 *  |  | QuerySubPlan(queryId:1, querySubPlanId:3)
 *  |  |  SINK(21)
 *  |  |    SOURCE(2,window2)
 */
TEST_F(ComplexSequenceTest, DISABLED_ComplexTestSingleNodeMultipleWindowsMultipleJoins) {
    struct Window1 {
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    struct Window3 {
        uint64_t id3;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window3Schema = Schema::create()
                             ->addField("id3", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window3), window3Schema->getSchemaSizeInBytes());

    std::string queryWithJoinAndWindowOperator =
        R"(Query::from("window1")
            .project(Attribute("window1$id1"), Attribute("window1$timestamp"))
            .filter(Attribute("window1$id1")<16)
            .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
            .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2)))
            .window(SlidingWindow::of(EventTime(Attribute("window2$timestamp")),Milliseconds(10),Milliseconds(5))).byKey(Attribute("window1window2window3$key")).apply(Sum(Attribute("window1window2$key")))
            .window(TumblingWindow::of(EventTime(Attribute("window1window2window3$start")),Milliseconds(10))).byKey(Attribute("window1window2window3$key")).apply(Sum(Attribute("window1window2$key")))
            .map(Attribute("window1window2$key") = Attribute("window1window2$key") * 2)
        )";
    TestHarness testHarness = TestHarness(queryWithJoinAndWindowOperator, restPort, rpcPort);

    testHarness.attachWorkerWithMemorySourceToCoordinator("window1", window1Schema, "window1");
    testHarness.attachWorkerWithMemorySourceToCoordinator("window2", window2Schema, "window2");
    testHarness.attachWorkerWithMemorySourceToCoordinator("window3", window3Schema, "window3");

    ASSERT_EQ(testHarness.getWorkerCount(), 3UL);

    testHarness.pushElement<Window1>({1UL, 1000UL}, 0UL);
    testHarness.pushElement<Window2>({12UL, 1001UL}, 0UL);
    testHarness.pushElement<Window2>({4UL, 1002UL}, 0UL);
    testHarness.pushElement<Window2>({4UL, 1005UL}, 0UL);
    testHarness.pushElement<Window2>({4UL, 1006UL}, 0UL);
    testHarness.pushElement<Window2>({1UL, 2000UL}, 0UL);
    testHarness.pushElement<Window2>({11UL, 2001UL}, 0UL);
    testHarness.pushElement<Window2>({16UL, 2002UL}, 0UL);
    testHarness.pushElement<Window2>({4UL, 2802UL}, 0UL);
    testHarness.pushElement<Window2>({4UL, 3642UL}, 0UL);
    testHarness.pushElement<Window2>({1UL, 3000UL}, 0UL);

    testHarness.pushElement<Window2>({21UL, 1003UL}, 1UL);
    testHarness.pushElement<Window2>({12UL, 1011UL}, 1UL);
    testHarness.pushElement<Window2>({12UL, 1013UL}, 1UL);
    testHarness.pushElement<Window2>({12UL, 1015UL}, 1UL);
    testHarness.pushElement<Window2>({4UL, 1102UL}, 1UL);
    testHarness.pushElement<Window2>({4UL, 1112UL}, 1UL);
    testHarness.pushElement<Window2>({1UL, 2010UL}, 1UL);
    testHarness.pushElement<Window2>({11UL, 2301UL}, 1UL);
    testHarness.pushElement<Window2>({4UL, 2022UL}, 1UL);
    testHarness.pushElement<Window2>({4UL, 3012UL}, 1UL);
    testHarness.pushElement<Window2>({33UL, 3100UL}, 1UL);

    testHarness.pushElement<Window3>({4UL, 1013UL}, 2UL);
    testHarness.pushElement<Window3>({12UL, 1010UL}, 2UL);
    testHarness.pushElement<Window3>({8UL, 1105UL}, 2UL);
    testHarness.pushElement<Window3>({76UL, 1132UL}, 2UL);
    testHarness.pushElement<Window3>({19UL, 2210UL}, 2UL);
    testHarness.pushElement<Window3>({1UL, 2501UL}, 2UL);
    testHarness.pushElement<Window2>({4UL, 2432UL}, 2UL);
    testHarness.pushElement<Window2>({4UL, 3712UL}, 2UL);
    testHarness.pushElement<Window3>({45UL, 3120UL}, 2UL);

    // output 2 join 1 window
    struct Output {
        uint64_t window1window2window3$start;
        uint64_t window1window2window3$end;
        uint64_t window1window2window3$key;
        uint64_t window1window2$key;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2window3$start == rhs.window1window2window3$start
                    && window1window2window3$end == rhs.window1window2window3$end
                    && window1window2window3$key == rhs.window1window2window3$key
                    && window1window2$key == rhs.window1window2$key);
        }
    };

    std::vector<Output> expectedOutput = {{1090, 1100, 4, 48}, {1000, 1010, 12, 96}, {1010, 1020, 12, 192}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//* Topology:
//PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=65535, usedResource=0]
//|--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=7, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
/*
 * @brief Test a query with a single window operator and a single join operator running multiple nodes
 */
TEST_F(ComplexSequenceTest, DISABLED_complexTestDistributedNodeMultipleWindowsMultipleJoinsWithTopDown) {
    struct Window1 {
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    struct Window3 {
        uint64_t id3;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window3Schema = Schema::create()
                             ->addField("id3", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window3), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinAndWindowOperator =
        R"(Query::from("window1")
            .project(Attribute("window1$id1"), Attribute("window1$timestamp"))
            .filter(Attribute("window1$id1")<16)
            .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
            .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2)))
            .window(SlidingWindow::of(EventTime(Attribute("window2$timestamp")),Milliseconds(10),Milliseconds(5))).byKey(Attribute("window1window2window3$key")).apply(Sum(Attribute("window1window2$key")))
            .window(TumblingWindow::of(EventTime(Attribute("window1window2window3$start")),Milliseconds(10))).byKey(Attribute("window1window2window3$key")).apply(Sum(Attribute("window1window2$key")))
            .map(Attribute("window1window2$key") = Attribute("window1window2$key") * 2)
        )";
    TestHarness testHarness = TestHarness(queryWithJoinAndWindowOperator, restPort, rpcPort);
    testHarness.attachWorkerToCoordinator();
    testHarness.attachWorkerToCoordinator();
    testHarness.attachWorkerToCoordinator();
    testHarness.attachWorkerWithMemorySourceToWorkerWithId("window1", window1Schema, "window1", testHarness.getWorkerId(0));
    testHarness.attachWorkerWithMemorySourceToWorkerWithId("window2", window2Schema, "window2", testHarness.getWorkerId(1));
    testHarness.attachWorkerWithMemorySourceToWorkerWithId("window3", window3Schema, "window3", testHarness.getWorkerId(2));

    ASSERT_EQ(testHarness.getWorkerCount(), 6U);

    testHarness.getTopology()->print();

    // Check if the topology matches the expected hierarchy
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren().size(), 3U);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren().size(), 1U);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[1]->getChildren().size(), 1U);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[2]->getChildren().size(), 1U);

    testHarness.pushElement<Window1>({1ULL, 1000ULL}, 3ULL);
    testHarness.pushElement<Window1>({12ULL, 1001ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 1002ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 1005ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 1006ULL}, 3ULL);
    testHarness.pushElement<Window1>({1ULL, 2000ULL}, 3ULL);
    testHarness.pushElement<Window1>({11ULL, 2001ULL}, 3ULL);
    testHarness.pushElement<Window1>({16ULL, 2002ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 2802ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 3642ULL}, 3ULL);
    testHarness.pushElement<Window1>({1ULL, 3000ULL}, 3ULL);

    testHarness.pushElement<Window2>({21ULL, 1003ULL}, 4ULL);
    testHarness.pushElement<Window2>({12ULL, 1011ULL}, 4ULL);
    testHarness.pushElement<Window2>({12ULL, 1013ULL}, 4ULL);
    testHarness.pushElement<Window2>({12ULL, 1015ULL}, 4ULL);
    testHarness.pushElement<Window2>({4ULL, 1102ULL}, 4ULL);
    testHarness.pushElement<Window2>({4ULL, 1112ULL}, 4ULL);
    testHarness.pushElement<Window2>({1ULL, 2010ULL}, 4ULL);
    testHarness.pushElement<Window2>({11ULL, 2301ULL}, 4ULL);
    testHarness.pushElement<Window2>({4ULL, 2022ULL}, 4ULL);
    testHarness.pushElement<Window2>({4ULL, 3012ULL}, 4ULL);
    testHarness.pushElement<Window2>({33ULL, 3100ULL}, 4ULL);

    testHarness.pushElement<Window3>({4ULL, 1013ULL}, 5ULL);
    testHarness.pushElement<Window3>({12ULL, 1010ULL}, 5ULL);
    testHarness.pushElement<Window3>({8ULL, 1105ULL}, 5ULL);
    testHarness.pushElement<Window3>({76ULL, 1132ULL}, 5ULL);
    testHarness.pushElement<Window3>({19ULL, 2210ULL}, 5ULL);
    testHarness.pushElement<Window3>({1ULL, 2501ULL}, 5ULL);
    testHarness.pushElement<Window3>({4ULL, 2432ULL}, 5ULL);
    testHarness.pushElement<Window3>({4ULL, 3712ULL}, 5ULL);
    testHarness.pushElement<Window3>({45ULL, 3120ULL}, 5ULL);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        uint64_t window1window2$key;
        uint64_t window1$id1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window1$id1 == rhs.window1$id1);
        }
    };

    std::vector<Output> expectedOutput = {{1090, 1100, 4, 48}, {1000, 1010, 12, 96}, {1010, 1020, 12, 192}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");
    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

//* Topology:
//PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=65535, usedResource=0]
//|--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |--PhysicalNode[id=3, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=8, usedResource=0]
//|  |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=8, usedResource=0]

// Placement:
//ExecutionNode(id:1, ip:127.0.0.1, topologyId:1)
//| QuerySubPlan(queryId:1, querySubPlanId:5)
//|  SINK(16)
//|    MAP(15)
//|      CENTRALWINDOW(17)
//|        WATERMARKASSIGNER(13)
//|          CENTRALWINDOW(18)
//|            WATERMARKASSIGNER(11)
//|              Join(10)
//|                SOURCE(19,)
//|                SOURCE(25,)
//|--ExecutionNode(id:2, ip:127.0.0.1, topologyId:2)
//|  | QuerySubPlan(queryId:1, querySubPlanId:1)
//|  |  SINK(20)
//|  |    WATERMARKASSIGNER(9)
//|  |      SOURCE(8,window3)
//|  | QuerySubPlan(queryId:1, querySubPlanId:4)
//|  |  SINK(26)
//|  |    Join(7)
//|  |      SOURCE(21,)
//|  |      SOURCE(23,)
//|  |--ExecutionNode(id:4, ip:127.0.0.1, topologyId:4)
//|  |  | QuerySubPlan(queryId:1, querySubPlanId:2)
//|  |  |  SINK(22)
//|  |  |    WATERMARKASSIGNER(6)
//|  |  |      SOURCE(4,window2)
//|  |--ExecutionNode(id:3, ip:127.0.0.1, topologyId:3)
//|  |  | QuerySubPlan(queryId:1, querySubPlanId:3)
//|  |  |  SINK(24)
//|  |  |    WATERMARKASSIGNER(5)
//|  |  |      FILTER(3)
//|  |  |        PROJECTION(2, schema=window1$id1:INTEGER window1$timestamp:INTEGER )
//|  |  |          SOURCE(1,window1)

/*
 * @brief Test a query with a single window operator and a single join operator running multiple nodes
 */
TEST_F(ComplexSequenceTest, complexTestDistributedNodeMultipleWindowsMultipleJoinsWithBottomUp) {
    struct Window1 {
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    struct Window3 {
        uint64_t id3;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window3Schema = Schema::create()
                             ->addField("id3", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window3), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinAndWindowOperator =
        R"(Query::from("window1")
            .project(Attribute("window1$id1"), Attribute("window1$timestamp"))
            .filter(Attribute("window1$id1")<16)
            .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500)))
            .joinWith(Query::from("window3")).where(Attribute("id1")).equalsTo(Attribute("id3")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(2)))
            .window(SlidingWindow::of(EventTime(Attribute("window2$timestamp")),Milliseconds(10),Milliseconds(5))).byKey(Attribute("window1window2window3$key")).apply(Sum(Attribute("window1window2$key")))
            .window(TumblingWindow::of(EventTime(Attribute("window1window2window3$start")),Milliseconds(10))).byKey(Attribute("window1window2window3$key")).apply(Sum(Attribute("window1window2$key")))
            .map(Attribute("window1window2$key") = Attribute("window1window2$key") * 2)
        )";
    TestHarness testHarness = TestHarness(queryWithJoinAndWindowOperator, restPort, rpcPort);
    testHarness.attachWorkerToCoordinator();                                                            //wrk0
    testHarness.attachWorkerToWorkerWithId(testHarness.getWorkerId(0));                                  //wrk1
    testHarness.attachWorkerWithMemorySourceToWorkerWithId("window3", window3Schema, "window3", testHarness.getWorkerId(0));//wrk2
    testHarness.attachWorkerWithMemorySourceToWorkerWithId("window1", window1Schema, "window1", testHarness.getWorkerId(1));//wrk3
    testHarness.attachWorkerWithMemorySourceToWorkerWithId("window2", window2Schema, "window2", testHarness.getWorkerId(1));//wrk4

    ASSERT_EQ(testHarness.getWorkerCount(), 5ULL);

    testHarness.getTopology()->print();

    // Check if the topology matches the expected hierarchy
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren().size(), 1U);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren()[0]->getChildren().size(), 2U);
    ASSERT_EQ(testHarness.getTopology()->getRoot()->getChildren()[0]->getChildren()[1]->getChildren().size(), 0U);

    testHarness.pushElement<Window1>({1ULL, 1000ULL}, 3ULL);
    testHarness.pushElement<Window1>({12ULL, 1001ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 1002ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 1005ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 1006ULL}, 3ULL);
    testHarness.pushElement<Window1>({1ULL, 2000ULL}, 3ULL);
    testHarness.pushElement<Window1>({11ULL, 2001ULL}, 3ULL);
    testHarness.pushElement<Window1>({16ULL, 2002ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 2802ULL}, 3ULL);
    testHarness.pushElement<Window1>({4ULL, 3642ULL}, 3ULL);
    testHarness.pushElement<Window1>({1ULL, 3000ULL}, 3ULL);

    testHarness.pushElement<Window2>({21ULL, 1003ULL}, 4ULL);
    testHarness.pushElement<Window2>({12ULL, 1011ULL}, 4ULL);
    testHarness.pushElement<Window2>({12ULL, 1013ULL}, 4ULL);
    testHarness.pushElement<Window2>({12ULL, 1015ULL}, 4ULL);
    testHarness.pushElement<Window2>({4ULL, 1102ULL}, 4ULL);
    testHarness.pushElement<Window2>({4ULL, 1112ULL}, 4ULL);
    testHarness.pushElement<Window2>({1ULL, 2010ULL}, 4ULL);
    testHarness.pushElement<Window2>({11ULL, 2301ULL}, 4ULL);
    testHarness.pushElement<Window2>({4ULL, 2022ULL}, 4ULL);
    testHarness.pushElement<Window2>({4ULL, 3012ULL}, 4ULL);
    testHarness.pushElement<Window2>({33ULL, 3100ULL}, 4ULL);

    testHarness.pushElement<Window3>({4ULL, 1013ULL}, 2ULL);
    testHarness.pushElement<Window3>({12ULL, 1010ULL}, 2ULL);
    testHarness.pushElement<Window3>({8ULL, 1105ULL}, 2ULL);
    testHarness.pushElement<Window3>({76ULL, 1132ULL}, 2ULL);
    testHarness.pushElement<Window3>({19ULL, 2210ULL}, 2ULL);
    testHarness.pushElement<Window3>({1ULL, 2501ULL}, 2ULL);
    testHarness.pushElement<Window3>({4ULL, 2432ULL}, 2ULL);
    testHarness.pushElement<Window3>({4ULL, 3712ULL}, 2ULL);
    testHarness.pushElement<Window3>({45ULL, 3120ULL}, 2ULL);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        uint64_t window1window2$key;
        uint64_t window1$id1;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window1$id1 == rhs.window1$id1);
        }
    };

    std::vector<Output> expectedOutput = {{1090ULL, 1100ULL, 4ULL, 48ULL},
                                          {1000ULL, 1010ULL, 12ULL, 96ULL},
                                          {1010ULL, 1020ULL, 12ULL, 192ULL}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");
    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}// namespace NES
