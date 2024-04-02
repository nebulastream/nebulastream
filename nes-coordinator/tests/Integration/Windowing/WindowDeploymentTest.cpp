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
#include <API/TestSchemas.hpp>
#include <BaseIntegrationTest.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Common/ExecutableType/Array.hpp>
#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Plans/Global/Query/GlobalQueryPlan.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Services/RequestHandlerService.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/TestHarness/TestHarness.hpp>
#include <iostream>

using namespace std;

namespace NES {

using namespace Configurations;

class WindowDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("WindowDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup WindowDeploymentTest test class.");
    }
};

TEST_F(WindowDeploymentTest, testTumblingWindowEventTimeWithTimeUnit) {

    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp"), Seconds()), Minutes(1)))
                     .byKey(Attribute("value"))
                     .apply(Sum(Attribute("id")));

    auto sourceConfig = CSVSourceType::create("window", "window1");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    // Expected output
    auto expectedOutput = "0, 60000, 1, 9\n"
                          "0, 60000, 12, 1\n"
                          "0, 60000, 4, 1\n"
                          "0, 60000, 11, 5\n"
                          "0, 60000, 16, 2\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralSlidingWindowEventTime) {

    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                     .byKey(Attribute("value"))
                     .apply(Sum(Attribute("id")));

    auto sourceConfig = CSVSourceType::create("window", "window1");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    // Expected output
    auto expectedOutput = "0, 10000, 1, 51\n"
                          "0, 10000, 4, 1\n"
                          "0, 10000, 11, 5\n"
                          "0, 10000, 12, 1\n"
                          "0, 10000, 16, 2\n"
                          "5000, 15000, 1, 95\n"
                          "10000, 20000, 1, 145\n"
                          "15000, 25000, 1, 126\n"
                          "20000, 30000, 1, 41\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief test distributed tumbling window and event time, for now disabled see issue #3324
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeployDistributedTumblingWindowQueryEventTimeTimeUnit) {

    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("ts"), Seconds()), Minutes(1)))
                     .byKey(Attribute("id"))
                     .apply(Sum(Attribute("value")));

    auto sourceConfig1 = CSVSourceType::create("window", "window1");
    sourceConfig1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig1->setGatheringInterval(0);
    sourceConfig1->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig1->setNumberOfBuffersToProduce(3);

    auto sourceConfig2 = CSVSourceType::create("window", "window2");
    sourceConfig2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    sourceConfig2->setGatheringInterval(0);
    sourceConfig2->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig2->setNumberOfBuffersToProduce(3);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig1)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig2)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    // Expected output
    auto expectedOutput = "960000, 1020000, 1, 34\n"
                          "1980000, 2040000, 2, 56\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowEventTime) {

    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    auto query = Query::from("window")
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                     .apply(Sum(Attribute("id")));

    auto sourceConfig = CSVSourceType::create("window", "window2");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(3);
    sourceConfig->setNumberOfBuffersToProduce(3);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    // Expected output
    auto expectedOutput = "1000, 2000, 3\n"
                          "2000, 3000, 6\n"
                          "3000, 4000, 9\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief test central sliding window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeySlidingWindowEventTime) {
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    auto query = Query::from("window")
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(10), Seconds(5)))
                     .apply(Sum(Attribute("id")));

    auto sourceConfig = CSVSourceType::create("window", "window2");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(0);
    sourceConfig->setNumberOfBuffersToProduce(1);

    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("window", testSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
                           .validate()
                           .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);

    // Expected output
    auto expectedOutput = "0, 10000, 60\n"
                          "5000, 15000, 95\n"
                          "10000, 20000, 145\n"
                          "15000, 25000, 126\n"
                          "20000, 30000, 41\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief test central tumbling window and event time
 */
TEST_F(WindowDeploymentTest, testCentralNonKeyTumblingWindowIngestionTime) {
    auto coordinatorConfig = CoordinatorConfiguration::createDefault();

    auto sourceConfig = CSVSourceType::create("windowSource", "test_stream");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(1);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(6);
    sourceConfig->setNumberOfBuffersToProduce(3);

    auto workerConfig = WorkerConfiguration::create();
    workerConfig->physicalSourceTypes.add(sourceConfig);
    workerConfig->coordinatorPort = *rpcCoordinatorPort;
    workerConfig->queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;

    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.queryCompiler.queryCompilerType = QueryCompilation::QueryCompilerType::NAUTILUS_QUERY_COMPILER;

    //register logical source qnv
    auto window = TestSchemas::getSchemaTemplate("id_val_time_u64");

    NES_INFO("WindowDeploymentTest: Start coordinator");
    auto crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    crd->getSourceCatalogService()->registerLogicalSource("windowSource", window);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("WindowDeploymentTest: Coordinator started successfully");

    NES_DEBUG("WindowDeploymentTest: Start worker 1");
    auto wrk1 = std::make_shared<NesWorker>(std::move(workerConfig));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("WindowDeploymentTest: Worker1 started successfully");

    auto requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "testGlobalTumblingWindow.out";
    remove(outputFilePath.c_str());

    NES_INFO("WindowDeploymentTest: Submit query");
    auto query = Query::from("windowSource")
                     .window(TumblingWindow::of(IngestionTime(), Seconds(1)))
                     .apply(Sum(Attribute("value")))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    //todo will be removed once the new window source is in place
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkFileCreationOrTimeout(outputFilePath));

    NES_INFO("WindowDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("WindowDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("WindowDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("WindowDeploymentTest: Test finished");
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithDoubleKey) {
    struct Car {
        double key;
        uint64_t value1;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createDouble())
                         ->addField("value1", DataTypeFactory::createUInt64())
                         ->addField("value2", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value1")));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car>({1.2, 2, 2, 1000}, 2);
    testHarness.pushElement<Car>({1.5, 4, 4, 1500}, 2);
    testHarness.pushElement<Car>({1.7, 5, 5, 2000}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "1000, 2000, 1.2, 2\n"
                          "1000, 2000, 1.5, 4\n"
                          "2000, 3000, 1.7, 5\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFloatKey) {
    struct __attribute__((__packed__)) Car2 {
        float key;
        uint64_t value1;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createFloat())
                         ->addField("value1", DataTypeFactory::createUInt64())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car2), carSchema->getSchemaSizeInBytes());
    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value1")));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car2>({1.2, 2, 1000}, 2);
    testHarness.pushElement<Car2>({1.5, 4, 1500}, 2);
    testHarness.pushElement<Car2>({1.7, 5, 2000}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "1000, 2000, 1.2, 2\n"
                          "1000, 2000, 1.5, 4\n"
                          "2000, 3000, 1.7, 5\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief TODO support bool key for aggregations #4151
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeploymentOfWindowWithBoolKey) {
    struct __attribute__((__packed__)) Car {
        bool key;
        uint32_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createBoolean())
                         ->addField("value2", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value")))
                                       .project(Attribute("value"));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car>({true, 2, 1000}, 2);
    testHarness.pushElement<Car>({false, 4, 1500}, 2);
    testHarness.pushElement<Car>({true, 5, 2000}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "2\n"
                          "4\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief TODO inplace chars are not implemented in Nautilus #2739
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeploymentOfWindowWitCharKey) {
    struct Car {
        char key;
        std::array<char, 3> value1;
        uint32_t value2;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createChar())
                         ->addField("value1", DataTypeFactory::createFixedChar(3))
                         ->addField("value2", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value2")))
                                       .project(Attribute("value2"));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    std::array<char, 3> charArrayValue = {'A', 'B', 'C'};
    testHarness.pushElement<Car>({'A', charArrayValue, 2, 1000}, 2);
    testHarness.pushElement<Car>({'B', charArrayValue, 4, 1500}, 2);
    testHarness.pushElement<Car>({'C', charArrayValue, 5, 2000}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "2\n"
                          "4\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief TODO inplace chars are not implemented in Nautilus
 */
TEST_F(WindowDeploymentTest, DISABLED_testDeploymentOfWindowWithFixedChar) {
    struct Car {
        NES::ExecutableTypes::Array<char, 4> key;
        uint32_t value1;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createFixedChar(4))
                         ->addField("value", DataTypeFactory::createUInt32())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Sum(Attribute("value")));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    NES::ExecutableTypes::Array<char, 4> keyOne = "aaa";
    NES::ExecutableTypes::Array<char, 4> keyTwo = "bbb";
    NES::ExecutableTypes::Array<char, 4> keyThree = "ccc";

    testHarness.pushElement<Car>({keyOne, 2, 1000}, 2);
    testHarness.pushElement<Car>({keyTwo, 4, 1500}, 2);
    testHarness.pushElement<Car>({keyThree, 5, 2000}, 2);

    testHarness.validate().setupTopology();

    // Expected output
    std::stringstream expectedOutput;
    expectedOutput << "1000, 2000, " << keyOne << ", 2\n"
                   << "1000, 2000, " << keyTwo << ", 4\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/*
 * @brief Test if the avg aggregation can be deployed
*/
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithAvgAggregation) {
    struct Car {
        uint64_t key;
        uint64_t value;
        uint64_t timestamp;
        double value1;
    };
    auto carSchema = TestSchemas::getSchemaTemplate("key_val_time_u64")->addField("value1", BasicType::FLOAT64);

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Avg(Attribute("value1")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car>({1, 2, 1000, 2}, 2);
    testHarness.pushElement<Car>({1, 4, 1500, 4}, 2);
    testHarness.pushElement<Car>({1, 5, 2000, 5}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "1000, 2000, 1, 3\n"
                          "2000, 3000, 1, 5\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/*
 * @brief Test if the max aggregation can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMaxAggregation) {
    struct Car {
        uint32_t key;
        uint32_t value;
        uint64_t timestamp;
    };

    auto carSchema = TestSchemas::getSchemaTemplate("key_val_time_u32");

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Max(Attribute("value")));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car>({1, 15, 1000}, 2);
    testHarness.pushElement<Car>({1, 99, 1500}, 2);
    testHarness.pushElement<Car>({1, 20, 2000}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "1000, 2000, 1, 99\n"
                          "2000, 3000, 1, 20\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/*
 * @brief Test if the max aggregation of negative values can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMaxAggregationWithNegativeValues) {
    struct Car {
        int32_t key;
        int32_t value;
        int64_t timestamp;
    };

    auto carSchema = TestSchemas::getSchemaTemplate("key_val_time_u32");

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Max(Attribute("value")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("car", carSchema)

                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car>({1, -15, 1000}, 2);
    testHarness.pushElement<Car>({1, -99, 1500}, 2);
    testHarness.pushElement<Car>({1, -20, 2000}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "1000, 2000, 1, -15\n"
                          "2000, 3000, 1, -20\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/*
 * @brief Test if the max aggregation with uint64 data type can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithMaxAggregationWithUint64AggregatedField) {
    struct Car {
        uint64_t id;
        uint64_t value;
        uint64_t timestamp;
    };

    auto carSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(10)))
                                       .byKey(Attribute("value"))
                                       .apply(Max(Attribute("id")));

    auto sourceConfig = CSVSourceType::create("car", "car1");
    sourceConfig->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    sourceConfig->setGatheringInterval(0);
    sourceConfig->setNumberOfTuplesToProducePerBuffer(28);
    sourceConfig->setNumberOfBuffersToProduce(1);
    sourceConfig->setSkipHeader(false);

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithCSVSourceToCoordinator(sourceConfig)
                           .validate()
                           .setupTopology();

    // Expected output
    auto expectedOutput = "0, 10000, 1, 9\n"
                          "10000, 20000, 1, 19\n"
                          "0, 10000, 4, 1\n"
                          "0, 10000, 11, 3\n"
                          "0, 10000, 12, 1\n"
                          "0, 10000, 16, 2\n"
                          "20000, 30000, 1, 21\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/*
 * @brief Test if the min aggregation with float data type can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFloatMinAggregation) {
    struct Car {
        uint32_t key;
        float value;
        uint64_t timestamp;
    };

    auto carSchema = Schema::create()
                         ->addField("key", DataTypeFactory::createUInt32())
                         ->addField("value", DataTypeFactory::createFloat())
                         ->addField("timestamp", DataTypeFactory::createUInt64());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Min(Attribute("value")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car>({1, 15.0, 1000}, 2);
    testHarness.pushElement<Car>({1, 99.0, 1500}, 2);
    testHarness.pushElement<Car>({1, 20.0, 2000}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "1000, 2000, 1, 15\n"
                          "2000, 3000, 1, 20\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/*
 * @brief Test if the Count aggregation can be deployed
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithCountAggregation) {
    struct Car {
        uint64_t key;
        uint64_t value;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = TestSchemas::getSchemaTemplate("key_2val_time_u64");

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Count());
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 1000ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 99ULL, 88ULL, 1500ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 20ULL, 20ULL, 2000ULL}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "1000, 2000, 1, 2\n"
                          "2000, 3000, 1, 1\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/*
 * @brief Test if the Median aggregation can be deployed
 * TODO enable if median is implemented #4096
*/
TEST_F(WindowDeploymentTest, DISABLED_testDeploymentOfWindowWithMedianAggregation) {
    struct Car {
        uint64_t key;
        double value;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = TestSchemas::getSchemaTemplate("key_val_time_u64")->addField("value2", DataTypeFactory::createDouble());

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Median(Attribute("value2")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car>({1ULL, 30ULL, 15ULL, 1000ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 90ULL, 88ULL, 1500ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 20ULL, 20ULL, 1800ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 60ULL, 20ULL, 2000ULL}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "1000, 2000, 1, 30\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/*
 * @brief Test aggregation with field rename
 */
TEST_F(WindowDeploymentTest, testDeploymentOfWindowWithFieldRename) {
    struct Car {
        uint64_t key;
        uint64_t value;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = TestSchemas::getSchemaTemplate("key_2val_time_u64");

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator = Query::from("car")
                                       .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)))
                                       .byKey(Attribute("key"))
                                       .apply(Count()->as(Attribute("Frequency")));
    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("car", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car");

    ASSERT_EQ(testHarness.getWorkerCount(), 1UL);
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 1000ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 99ULL, 88ULL, 1500ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 20ULL, 20ULL, 2000ULL}, 2);
    testHarness.validate().setupTopology();

    // Expected output
    auto expectedOutput = "1000, 2000, 1, 2\n"
                          "2000, 3000, 1, 1\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

}// namespace NES
