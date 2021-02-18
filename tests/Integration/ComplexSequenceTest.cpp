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

#include <Util/Logger.hpp>
#include <Util/TestHarness.hpp>
#include <iostream>

using namespace std;

namespace NES {

class ComplexSequenceTest : public testing::Test {
  public:

    static void SetUpTestCase() {
        NES::setupLogging("ComplexSequenceTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup ComplexSequenceTest test class.");
    }

    void SetUp() {
        restPort = restPort + 2;
        rpcPort = rpcPort + 30;
    }

    void TearDown() {
        std::cout << "Tear down ComplexSequenceTest class." << std::endl;
    }

    uint32_t restPort = 8080;
    uint32_t rpcPort = 4000;
};

/*
 * Scenario: 1 query with join and window operator
 * FT vs HT
 * MultiJoin SingleWindow, MultiWindow SingleJoin, MultiWindowMultiJoin
 * Test Abbreviations:
 *  Node Configurations:
 *   SN: Single Node
 *   DN: Distributed Nodes
 *  Query Operators:
 *   SW: Single Window
 *   MW: Multiple Windows
 *   SJ: SingleJoin
 *   MJ: Multiple Joins
 */

/*
 * @brief Test a query with a single window operator and a single join operator running on a single node
 */
TEST_F(ComplexSequenceTest, SN_SW_SJ) {
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
            .joinWith(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
            .windowByKey(Attribute("window1window2$key"), TumblingWindow::of(EventTime(Attribute("window1$timestamp")), Seconds(2)), Sum(Attribute("window1$id1")))
        )";
    TestHarness testHarness = TestHarness(queryWithJoinOperator, restPort, rpcPort);

    testHarness.addMemorySource("window1", window1Schema, "window1");
    testHarness.addMemorySource("window2", window2Schema, "window2");

    ASSERT_EQ(testHarness.getWorkerCount(), 2);

    testHarness.pushElement<Window1>({1, 1000}, 0);
    testHarness.pushElement<Window2>({12, 1001}, 0);
    testHarness.pushElement<Window2>({4, 1002}, 0);
    testHarness.pushElement<Window2>({1, 2000}, 0);
    testHarness.pushElement<Window2>({11, 2001}, 0);
    testHarness.pushElement<Window2>({16, 2002}, 0);
    testHarness.pushElement<Window2>({1, 3000}, 0);

    testHarness.pushElement<Window2>({21, 1003}, 1);
    testHarness.pushElement<Window2>({12, 1011}, 1);
    testHarness.pushElement<Window2>({4, 1102}, 1);
    testHarness.pushElement<Window2>({4, 1112}, 1);
    testHarness.pushElement<Window2>({1, 2010}, 1);
    testHarness.pushElement<Window2>({11, 2301}, 1);
    testHarness.pushElement<Window2>({33, 3100}, 1);

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

    // TODO #1533: Explain the result
    std::vector<Output> expectedOutput = {{0, 2000, 4, 8},
                                          {0, 2000, 12, 12}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size());

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
// TODO #1533: fix the method to push element to the correct source
TEST_F(ComplexSequenceTest, DISABLED_DN_SW_SJ) {
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
            .joinWith(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
            .windowByKey(Attribute("window1window2$key"), TumblingWindow::of(EventTime(Attribute("window1$timestamp")), Seconds(1)), Sum(Attribute("window1$id1")))
        )";
    TestHarness testHarness = TestHarness(queryWithJoinOperator, restPort, rpcPort);
    testHarness.addNonSourceWorker();
    testHarness.addNonSourceWorker();
    testHarness.addMemorySource("window1", window1Schema, "window1", testHarness.getWorkerId(0));
    testHarness.addMemorySource("window2", window2Schema, "window2", testHarness.getWorkerId(1));

    ASSERT_EQ(testHarness.getWorkerCount(), 4);

    testHarness.pushElement<Window1>({1, 1000}, 0);
//    testHarness.pushElement<Window2>({12, 1001}, 2);
//    testHarness.pushElement<Window2>({4, 1002}, 2);
//    testHarness.pushElement<Window2>({1, 2000}, 2);
//    testHarness.pushElement<Window2>({11, 2001}, 2);
//    testHarness.pushElement<Window2>({16, 2002}, 2);
//    testHarness.pushElement<Window2>({1, 3000}, 2);

    testHarness.pushElement<Window2>({21, 1003}, 1);
//    testHarness.pushElement<Window2>({12, 1011}, 3);
//    testHarness.pushElement<Window2>({4, 1102}, 3);
//    testHarness.pushElement<Window2>({4, 1112}, 3);
//    testHarness.pushElement<Window2>({1, 2010}, 3);
//    testHarness.pushElement<Window2>({11, 2301}, 3);
//    testHarness.pushElement<Window2>({33, 3100}, 3);

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

    // TODO #1533: Explain the result
    std::vector<Output> expectedOutput = {{0, 2000, 4, 8},
                                          {0, 2000, 12, 12}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size());

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

}// namespace NES
