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
#include <Catalogs/LambdaSourceStreamConfig.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Coordinator/CoordinatorConfig.hpp>
#include <Configurations/Sources/CSVSourceConfig.hpp>
#include <Configurations/Sources/SourceConfigFactory.hpp>
#include <Configurations/Worker/WorkerConfig.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <Util/TestUtils.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

static uint64_t restPort = 8081;
static uint64_t rpcPort = 4000;

class JoinDeploymentTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("JoinDeploymentTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup JoinDeploymentTest test class.");
    }

    void SetUp() override {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() override { std::cout << "Tear down JoinDeploymentTest class." << std::endl; }
};

/**
 * Test deploying join query with source on two different worker node using top down strategy.
 */
//TODO: this test will be enabled once we have the renaming function using as
//TODO: prevent self join
TEST_F(JoinDeploymentTest, DISABLED_testSelfJoinTumblingWindow) {

    struct Window {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("value", DataTypeFactory::createUInt64())
                            ->addField("id", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());


    string query =
        R"(Query::from("window").as("w1").joinWith(Query::from("window").as("w2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);


    //Setup first physical stream
    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream1");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);

    //Setup second physical stream for same logical stream
//    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream2");
//    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf);
//    testHarness.addCSVSource(conf2, windowSchema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        uint64_t value1;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t value2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && value1 == rhs.value1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && value2 == rhs.value2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 1, 4, 1002},
                                          {1000, 2000, 12, 1, 12, 1001, 1, 12, 1001},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2000},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2001},
                                          {2000, 3000, 16, 2, 16, 2002, 2, 16, 2002}};


    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
* Test deploying join with same data and same schema
 * */
TEST_F(JoinDeploymentTest, testJoinWithSameSchemaTumblingWindow) {
    struct Window {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("value", DataTypeFactory::createUInt64())
                            ->addField("id", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("value", DataTypeFactory::createUInt64())
                             ->addField("id", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id")).equalsTo(Attribute("id")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);

    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window2");

    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf2, window2Schema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        uint64_t value1;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t value2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && value1 == rhs.value1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && value2 == rhs.value2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 1, 4, 1002},
                                          {1000, 2000, 12, 1, 12, 1001, 1, 12, 1001},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2000},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2001},
                                          {2000, 3000, 16, 2, 16, 2002, 2, 16, 2002}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying join with same data but different names in the schema
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentSchemaNamesButSameInputTumblingWindow) {
    struct Window {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("value1", DataTypeFactory::createUInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("value2", DataTypeFactory::createUInt64())
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);

    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window2");

    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf2, window2Schema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        uint64_t value1;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t value2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && value1 == rhs.value1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && value2 == rhs.value2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 1, 4, 1002},
                                          {1000, 2000, 12, 1, 12, 1001, 1, 12, 1001},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2000},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2001},
                                          {2000, 3000, 16, 2, 16, 2002, 2, 16, 2002}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying join with different streams
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentStreamTumblingWindow) {
    struct Window {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("value1", DataTypeFactory::createUInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("value2", DataTypeFactory::createUInt64())
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    srcConf1->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf1);
    testHarness.addCSVSource(conf2, window2Schema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        uint64_t value1;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t value2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && value1 == rhs.value1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && value2 == rhs.value2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying join with different streams
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentNumberOfAttributesTumblingWindow) {
    struct Window {
        int64_t win;
        uint64_t id;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("win", DataTypeFactory::createInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window3.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    srcConf1->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf1);
    testHarness.addCSVSource(conf2, window2Schema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        int64_t win;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && win == rhs.win && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 4, 1002},
                                          {1000, 2000, 12, 1, 12, 1001, 12, 1001},
                                          {2000, 3000, 1, 2, 1, 2000, 1, 2000},
                                          {2000, 3000, 11, 2, 11, 2001, 11, 2001},
                                          {2000, 3000, 16, 2, 16, 2002, 16, 2002}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying join with different streams and different Speed
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentStreamDifferentSpeedTumblingWindow) {
    struct Window {
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp1;
    };

    struct Window2 {
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp2;
    };

    auto windowSchema = Schema::create()
                            ->addField("win1", DataTypeFactory::createInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("win2", DataTypeFactory::createInt64())
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    srcConf->as<CSVSourceConfig>()->setSourceFrequency(0);
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setSourceFrequency(1);
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(2);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(3);
    srcConf1->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf1);
    testHarness.addCSVSource(conf2, window2Schema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp1;
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && win1 == rhs.win1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && win1 == rhs.win1 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying join with different three sources
 */
TEST_F(JoinDeploymentTest, testJoinWithThreeSources) {
    struct Window {
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("win1", DataTypeFactory::createInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("win2", DataTypeFactory::createInt64())
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);
    testHarness.addCSVSource(conf, windowSchema);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf1);
    testHarness.addCSVSource(conf2, window2Schema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp1;
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && win1 == rhs.win1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && win2 == rhs.win2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying join with four different sources
 */
TEST_F(JoinDeploymentTest, testJoinWithFourSources) {
    struct Window {
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("win1", DataTypeFactory::createInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("win2", DataTypeFactory::createInt64())
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")),
        Milliseconds(1000))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);
    testHarness.addCSVSource(conf, windowSchema);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf1);
    testHarness.addCSVSource(conf2, window2Schema);
    testHarness.addCSVSource(conf2, window2Schema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp1;
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && win1 == rhs.win1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && win2 == rhs.win2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},    {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},    {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},    {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},    {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011}, {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011}, {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},    {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},    {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301}, {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301}, {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying join with different streams
 */
TEST_F(JoinDeploymentTest, testJoinWithDifferentStreamSlidingWindow) {
    struct Window {
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("win1", DataTypeFactory::createInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("win2", DataTypeFactory::createInt64())
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(
        SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");

    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window2.csv");
    srcConf1->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf1);
    testHarness.addCSVSource(conf2, window2Schema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp1;
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && win1 == rhs.win1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && win2 == rhs.win2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {1500, 2500, 1, 2, 1, 2000, 2, 1, 2010},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {500, 1500, 4, 1, 4, 1002, 3, 4, 1102},
                                          {500, 1500, 4, 1, 4, 1002, 3, 4, 1112},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301},
                                          {1500, 2500, 11, 2, 11, 2001, 2, 11, 2301},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},
                                          {500, 1500, 12, 1, 12, 1001, 5, 12, 1011}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/**
 * Test deploying join with different streams
 */
TEST_F(JoinDeploymentTest, testSlidingWindowDifferentAttributes) {
    struct Window {
        int64_t win;
        uint64_t id1;
        uint64_t timestamp;
    };

    struct Window2 {
        uint64_t id2;
        uint64_t timestamp;
    };

    auto windowSchema = Schema::create()
                            ->addField("win", DataTypeFactory::createInt64())
                            ->addField("id1", DataTypeFactory::createUInt64())
                            ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createUInt64())
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    string query =
        R"(Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(
        SlidingWindow::of(EventTime(Attribute("timestamp")),Seconds(1),Milliseconds(500))))";
    TestHarness testHarness = TestHarness(query, restPort, rpcPort);

    SourceConfigPtr srcConf = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window.csv");
    srcConf->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf->as<CSVSourceConfig>()->setLogicalStreamName("window1");
    srcConf->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create(srcConf);
    testHarness.addCSVSource(conf, windowSchema);

    SourceConfigPtr srcConf1 = SourceConfigFactory::createSourceConfig("CSVSource");
    srcConf1->as<CSVSourceConfig>()->setLogicalStreamName("window2");
    srcConf1->as<CSVSourceConfig>()->setFilePath("../tests/test_data/window3.csv");
    srcConf1->as<CSVSourceConfig>()->setNumberOfTuplesToProducePerBuffer(3);
    srcConf1->as<CSVSourceConfig>()->setNumberOfBuffersToProduce(2);
    srcConf1->as<CSVSourceConfig>()->setPhysicalStreamName("test_stream");
    srcConf1->as<CSVSourceConfig>()->setSkipHeader(true);

    PhysicalStreamConfigPtr conf2 = PhysicalStreamConfig::create(srcConf1);
    testHarness.addCSVSource(conf2, window2Schema);

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        int64_t win;
        uint64_t id1;
        uint64_t timestamp1;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && win == rhs.win && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{2000, 3000, 1, 2, 1, 2000, 1, 2000},
                                          {1500, 2500, 1, 2, 1, 2000, 1, 2000},
                                          {1000, 2000, 4, 1, 4, 1002, 4, 1002},
                                          {500, 1500, 4, 1, 4, 1002, 4, 1002},
                                          {2000, 3000, 11, 2, 11, 2001, 11, 2001},
                                          {1500, 2500, 11, 2, 11, 2001, 11, 2001},
                                          {1000, 2000, 12, 1, 12, 1001, 12, 1001},
                                          {500, 1500, 12, 1, 12, 1001, 12, 1001},
                                          {2000, 3000, 16, 2, 16, 2002, 16, 2002},
                                          {1500, 2500, 16, 2, 16, 2002, 16, 2002}};

    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}

/*
 * @brief Test a join query that uses fixed-array as keys
 */
TEST_F(JoinDeploymentTest, testJoinWithFixedCharKey) {
    struct Window1 {
        NES::ExecutableTypes::Array<char, 8> id1;
        uint64_t timestamp;
    };

    struct Window2 {
        NES::ExecutableTypes::Array<char, 8> id2;
        uint64_t timestamp;
    };

    auto window1Schema = Schema::create()
                             ->addField("id1", DataTypeFactory::createFixedChar(8))
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    auto window2Schema = Schema::create()
                             ->addField("id2", DataTypeFactory::createFixedChar(8))
                             ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Window1), window1Schema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    std::string queryWithJoinOperator =
        R"(Query::from("window1")
            .joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
            .project(Attribute("window1window2$start"),Attribute("window1window2$end"), Attribute("window1window2$key"),  Attribute("window1$timestamp"))
        )";
    TestHarness testHarness = TestHarness(queryWithJoinOperator, restPort, rpcPort);

    testHarness.addMemorySource("window1", window1Schema, "window1");
    testHarness.addMemorySource("window2", window2Schema, "window2");

    ASSERT_EQ(testHarness.getWorkerCount(), 2u);

    testHarness.pushElement<Window1>({"aaaaaaa", 1000u}, 0u);
    testHarness.pushElement<Window2>({"bbbbbbb", 1001u}, 0u);
    testHarness.pushElement<Window2>({"ccccccc", 1002u}, 0u);
    testHarness.pushElement<Window2>({"aaaaaaa", 2000u}, 0u);
    testHarness.pushElement<Window2>({"ddddddd", 2001u}, 0u);
    testHarness.pushElement<Window2>({"eeeeeee", 2002u}, 0u);
    testHarness.pushElement<Window2>({"aaaaaaa", 3000u}, 0u);

    testHarness.pushElement<Window2>({"fffffff", 1003u}, 1u);
    testHarness.pushElement<Window2>({"bbbbbbb", 1011u}, 1u);
    testHarness.pushElement<Window2>({"ccccccc", 1102u}, 1u);
    testHarness.pushElement<Window2>({"ccccccc", 1112u}, 1u);
    testHarness.pushElement<Window2>({"aaaaaaa", 2010u}, 1u);
    testHarness.pushElement<Window2>({"ddddddd", 2301u}, 1u);
    testHarness.pushElement<Window2>({"ggggggg", 3100u}, 1u);

    struct Output {
        uint64_t window1window2$start;
        uint64_t window1window2$end;
        NES::ExecutableTypes::Array<char, 8> window1window2$key;
        uint64_t window2$timestamp;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (window1window2$start == rhs.window1window2$start && window1window2$end == rhs.window1window2$end
                    && window1window2$key == rhs.window1window2$key && window2$timestamp == rhs.window2$timestamp);
        }
    };

    std::vector<Output> expectedOutput = {{2000, 3000, "ddddddd", 2001},
                                          {1000, 2000, "ccccccc", 1002},
                                          {1000, 2000, "ccccccc", 1002},
                                          {1000, 2000, "bbbbbbb", 1001}};
    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "BottomUp", "NONE", "IN_MEMORY");

    EXPECT_EQ(actualOutput.size(), expectedOutput.size());
    EXPECT_THAT(actualOutput, ::testing::UnorderedElementsAreArray(expectedOutput));
}
}// namespace NES
