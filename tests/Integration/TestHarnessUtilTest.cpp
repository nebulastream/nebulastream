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
#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <Util/TestHarness.hpp>

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

    testHarness.pushElement<Car>({40,40,40},0);
    testHarness.pushElement<Car>({30,30,30},0);
    testHarness.pushElement<Car>({71,71,71},0);
    testHarness.pushElement<Car>({21,21,21},0);

    std::string output = testHarness.getOutput(1);

    std::string expectedContent = "key:INTEGER,value:INTEGER,timestamp:INTEGER\n"
                                  "40,40,40\n"
                                  "30,30,30\n"
                                  "71,71,71\n"
                                  "21,21,21\n";

    ASSERT_EQ(expectedContent, output);
}

/*
 * Testing testHarness utility using one logical source and two physical sources
 */
//TODO: disabled because the order from which source the tuples in the result are coming is not deterministic (issue #1442)
//TEST_F(TestHarnessUtilTest, testHarnessUtilWithTwoPhysicalSourceOfTheSameLogicalSource) {
//    struct Car {
//        uint32_t key;
//        uint32_t value;
//        uint64_t timestamp;
//    };
//
//    auto carSchema = Schema::create()
//        ->addField("key", DataTypeFactory::createUInt32())
//        ->addField("value", DataTypeFactory::createUInt32())
//        ->addField("timestamp", DataTypeFactory::createUInt64());
//
//    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());
//
//    std::string queryWithFilterOperator = R"(Query::from("car").filter(Attribute("key") < 1000))";
//    TestHarness testHarness = TestHarness(queryWithFilterOperator);
//
//    testHarness.addSource("car", carSchema, "car1");
//    testHarness.addSource("car", carSchema, "car2");
//
//    ASSERT_EQ(testHarness.getWorkerCount(), 2);
//
//    testHarness.pushElement<Car>({40,40,40},0);
//    testHarness.pushElement<Car>({30,30,30},0);
//    testHarness.pushElement<Car>({71,71,71},1);
//    testHarness.pushElement<Car>({21,21,21},1);
//
//    std::string output = testHarness.getOutput(1);
//
//    std::string expectedContent = "key:INTEGER,value:INTEGER,timestamp:INTEGER\n"
//                                  "40,40,40\n"
//                                  "30,30,30\n"
//                                  "71,71,71\n"
//                                  "21,21,21\n";
//
//    EXPECT_EQ(expectedContent, output);
//}

/*
 * Testing testHarness utility using two logical source with one physical source each
 */
//TODO: disabled because the order from which source the tuples in the result are coming is not deterministic (issue #1442)
//TEST_F(TestHarnessUtilTest, testHarnessUtilWithTwoPhysicalSourceOfDifferentLogicalSources) {
//    struct Car {
//        uint32_t key;
//        uint32_t value;
//        uint64_t timestamp;
//    };
//
//    struct Truck {
//        uint32_t key;
//        uint32_t value;
//        uint64_t timestamp;
//    };
//
//    auto carSchema = Schema::create()
//        ->addField("key", DataTypeFactory::createUInt32())
//        ->addField("value", DataTypeFactory::createUInt32())
//        ->addField("timestamp", DataTypeFactory::createUInt64());
//
//    auto truckSchema = Schema::create()
//        ->addField("key", DataTypeFactory::createUInt32())
//        ->addField("value", DataTypeFactory::createUInt32())
//        ->addField("timestamp", DataTypeFactory::createUInt64());
//
//    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());
//    ASSERT_EQ(sizeof(Truck), truckSchema->getSchemaSizeInBytes());
//
//    std::string queryWithFilterOperator = R"(Query::from("car").merge(Query::from("truck")))";
//    TestHarness testHarness = TestHarness(queryWithFilterOperator);
//
//    testHarness.addSource("car", carSchema, "car1");
//    testHarness.addSource("truck", truckSchema, "truck1");
//
//    ASSERT_EQ(testHarness.getWorkerCount(), 2);
//
//    testHarness.pushElement<Car>({40,40,40},0);
//    testHarness.pushElement<Car>({30,30,30},0);
//    testHarness.pushElement<Truck>({71,71,71},1);
//    testHarness.pushElement<Truck>({21,21,21},1);
//
//    std::string output = testHarness.getOutput(1);
//
//    std::string expectedContent = "key:INTEGER,value:INTEGER,timestamp:INTEGER\n"
//                                  "40,40,40\n"
//                                  "30,30,30\n"
//                                  "71,71,71\n"
//                                  "21,21,21\n";
//
//    EXPECT_EQ(expectedContent, output);
//}

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

    EXPECT_THROW(testHarness.getOutput(1), std::runtime_error);
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
    EXPECT_THROW(testHarness.pushElement<Car>({30,30,30},0), std::runtime_error);
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

    EXPECT_THROW(testHarness.pushElement<Truck>({30,30,30,30,30},0), std::runtime_error);
}

//TODO: more test using window, merge, or join operator (issue #1443)
}// namespace NES