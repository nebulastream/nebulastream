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
#include <Util/TestHarness.hpp>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace NES {

class TestHarnessUtilTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("TestHarnessUtilTest.log", NES::LOG_DEBUG);

        NES_INFO("TestHarnessUtilTest test class SetUpTestCase.");
    }
    static void TearDownTestCase() { NES_INFO("TestHarnessUtilTest test class TearDownTestCase."); }
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
    TestHarness testHarness = TestHarness(queryWithFilterOperator);

    testHarness.addSource("car", carSchema, "car1");

    testHarness.pushElement<Car>({40, 40, 40}, 0);
    testHarness.pushElement<Car>({30, 30, 30}, 0);
    testHarness.pushElement<Car>({71, 71, 71}, 0);
    testHarness.pushElement<Car>({21, 21, 21}, 0);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(1);

    std::vector<Output> expectedOutput = {{40, 40, 40},
                                          {21, 21, 21},
                                          {
                                              30,
                                              30,
                                              30,
                                          },
                                          {71, 71, 71}};

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
    TestHarness testHarness = TestHarness(queryWithFilterOperator);

    testHarness.addSource("car", carSchema, "car1");
    testHarness.addSource("car", carSchema, "car2");

    ASSERT_EQ(testHarness.getWorkerCount(), 2);

    testHarness.pushElement<Car>({40, 40, 40}, 0);
    testHarness.pushElement<Car>({30, 30, 30}, 0);
    testHarness.pushElement<Car>({71, 71, 71}, 1);
    testHarness.pushElement<Car>({21, 21, 21}, 1);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(1);

    std::vector<Output> expectedOutput = {{40, 40, 40},
                                          {21, 21, 21},
                                          {
                                              30,
                                              30,
                                              30,
                                          },
                                          {71, 71, 71}};

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * Testing testHarness utility using two logical source with one physical source each
 */
TEST_F(TestHarnessUtilTest, DISABLED_testHarnessUtilWithTwoPhysicalSourceOfDifferentLogicalSources) {
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

    std::string queryWithFilterOperator = R"(Query::from("car").merge(Query::from("truck")))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator);

    testHarness.addSource("car", carSchema, "car1");
    testHarness.addSource("truck", truckSchema, "truck1");

    ASSERT_EQ(testHarness.getWorkerCount(), 2);

    testHarness.pushElement<Car>({40, 40, 40}, 0);
    testHarness.pushElement<Car>({30, 30, 30}, 0);
    testHarness.pushElement<Truck>({71, 71, 71}, 1);
    testHarness.pushElement<Truck>({21, 21, 21}, 1);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(1);

    std::vector<Output> expectedOutput = {{40, 40, 40},
                                          {21, 21, 21},
                                          {
                                              30,
                                              30,
                                              30,
                                          },
                                          {71, 71, 71}};

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

    std::string queryWithWindowOperator = R"(Query::from("car").windowByKey(Attribute("key"), TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)), Sum(Attribute("value"))))";
    TestHarness testHarness = TestHarness(queryWithWindowOperator);

    testHarness.addSource("car", carSchema, "car1");
    testHarness.addSource("car", carSchema, "car2");

    ASSERT_EQ(testHarness.getWorkerCount(), 2);
    testHarness.pushElement<Car>({1, 1, 1000}, 0);
    testHarness.pushElement<Car>({12, 1, 1001}, 0);
    testHarness.pushElement<Car>({4, 1, 1002}, 0);
    testHarness.pushElement<Car>({1, 2, 2000}, 0);
    testHarness.pushElement<Car>({11, 2, 2001}, 0);
    testHarness.pushElement<Car>({16, 2, 2002}, 0);
    testHarness.pushElement<Car>({1, 3, 3000}, 0);
    testHarness.pushElement<Car>({11, 3, 3001}, 0);
    testHarness.pushElement<Car>({1, 3, 3003}, 0);
    testHarness.pushElement<Car>({1, 3, 3200}, 0);
    testHarness.pushElement<Car>({1, 4, 4000}, 0);
    testHarness.pushElement<Car>({1, 5, 5000}, 0);

    testHarness.pushElement<Car>({1, 1, 1000}, 1);
    testHarness.pushElement<Car>({12, 1, 1001}, 1);
    testHarness.pushElement<Car>({4, 1, 1002}, 1);
    testHarness.pushElement<Car>({1, 2, 2000}, 1);
    testHarness.pushElement<Car>({11, 2, 2001}, 1);
    testHarness.pushElement<Car>({16, 2, 2002}, 1);
    testHarness.pushElement<Car>({1, 3, 3000}, 1);
    testHarness.pushElement<Car>({11, 3, 3001}, 1);
    testHarness.pushElement<Car>({1, 3, 3003}, 1);
    testHarness.pushElement<Car>({1, 3, 3200}, 1);
    testHarness.pushElement<Car>({1, 4, 4000}, 1);
    testHarness.pushElement<Car>({1, 5, 5000}, 1);


    struct Output {
        uint64_t start;
        uint64_t end;
        uint32_t key;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && start == rhs.start && end == rhs.end); }
    };

    // TODO: Can we remove the requirement to provide bufferToExpect?
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(3);

    std::vector<Output> expectedOutput = {{1000, 2000, 1, 2},
                                          {2000, 3000, 1, 0},
                                          {3000, 4000, 1, 4},
                                          {4000, 5000, 1, 0},
                                          {1000, 2000, 4, 2},
                                          {2000, 3000, 11, 4},
                                          {3000, 4000, 11, 0},
                                          {1000, 2000, 12, 2},
                                          {2000, 3000, 16, 4},};

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Testing testHarness utility for query with a join operator on different streams
 */
TEST_F(TestHarnessUtilTest, testHarnessWithJoinOperator) {
    struct Window1 {
//        uint32_t win1;
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
//        uint32_t win2;
        uint64_t id2;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
//        ->addField("win1", DataTypeFactory::createUInt32())
        ->addField("id1", DataTypeFactory::createUInt64())
        ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
//        ->addField("win2", DataTypeFactory::createUInt32())
        ->addField("id2", DataTypeFactory::createUInt64())
        ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinOperator = R"(Query::from("window1").join(Query::from("window2"), Attribute("id1"), Attribute("id2"), TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(queryWithJoinOperator);

    testHarness.addSource("window1", window1Schema, "window1");
    testHarness.addSource("window2", window2Schema, "window2");

    ASSERT_EQ(testHarness.getWorkerCount(), 2);

    testHarness.pushElement<Window1>({ 1, 1000}, 0);
    testHarness.pushElement<Window2>({ 12, 1001}, 0);
    testHarness.pushElement<Window2>({ 4, 1002}, 0);
    testHarness.pushElement<Window2>({ 1, 2000}, 0);
    testHarness.pushElement<Window2>({ 11, 2001}, 0);
    testHarness.pushElement<Window2>({ 16, 2002}, 0);
    testHarness.pushElement<Window2>({ 1, 3000}, 0);

    testHarness.pushElement<Window2>({ 21, 1003}, 1);
    testHarness.pushElement<Window2>({ 12, 1011}, 1);
    testHarness.pushElement<Window2>({ 4, 1102}, 1);
    testHarness.pushElement<Window2>({ 4, 1112}, 1);
    testHarness.pushElement<Window2>({ 1, 2010}, 1);
    testHarness.pushElement<Window2>({ 11, 2301}, 1);
    testHarness.pushElement<Window2>({ 33, 3100}, 1);

    struct Output {
        uint64_t start;
        uint64_t end;
        uint64_t key;
        uint64_t left_id1;
        uint64_t left_timestamp;
        uint64_t right_id2;
        uint64_t right_timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && left_id1 == rhs.left_id1 && left_timestamp == rhs.left_timestamp
                    && right_id2 == rhs.right_id2 && right_timestamp == rhs.right_timestamp);
        }
    };
    uint64_t  sizeOfOutput = sizeof(Output);
    NES_DEBUG("TestHarness: size of output " << sizeOfOutput);
    // TODO: Can we remove the requirement to provide bufferToExpect?
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(2);
    std::vector<Output> expectedOutput = {{1000, 2000, 4, 4, 1002, 4, 1102},
                                          {1000, 2000, 4, 4, 1002, 4, 1112},
                                          {1000, 2000, 12, 12, 1001, 12, 1011},
                                          {2000, 3000, 11, 11, 2001, 11, 2301}};

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
    TestHarness testHarness = TestHarness(queryWithFilterOperator);

    ASSERT_EQ(testHarness.getWorkerCount(), 0);

    struct Output {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const { return (key == rhs.key && value == rhs.value && timestamp == rhs.timestamp); }
    };
    EXPECT_THROW(testHarness.getOutput<Output>(1), NesRuntimeException);
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
    TestHarness testHarness = TestHarness(queryWithFilterOperator);

    ASSERT_EQ(testHarness.getWorkerCount(), 0);
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

    std::string queryWithFilterOperator = R"(Query::from("car").merge(Query::from("truck")))";
    TestHarness testHarness = TestHarness(queryWithFilterOperator);

    testHarness.addSource("car", carSchema, "car1");
    testHarness.addSource("truck", truckSchema, "truck1");

    ASSERT_EQ(testHarness.getWorkerCount(), 2);

    EXPECT_THROW(testHarness.pushElement<Truck>({30, 30, 30, 30, 30}, 0), NesRuntimeException);
}



//TODO: more test using window, merge, or join operator (issue #1443)
}// namespace NES