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
#include <Catalogs/Exceptions/InvalidQueryException.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Coordinator/LogicalSourceType.hpp>
#include <Configurations/Coordinator/SchemaType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/DefaultSourceType.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/LambdaSourceType.hpp>
#include <Util/TestHarness/TestHarness.hpp>

using namespace std;

namespace NES {

using namespace Configurations;

class QueryDeploymentTest : public Testing::BaseIntegrationTest {
  public:
    static void SetUpTestCase() {
        NES::Logger::setupLogging("QueryDeploymentTest.log", NES::LogLevel::LOG_DEBUG);
        NES_INFO("Setup QueryDeploymentTest test class.");
    }
};

/**
 * Test deploying unionWith query with source on two different worker node using bottom up strategy.
 */
TEST_F(QueryDeploymentTest, testDeployTwoWorkerMergeUsingBottomUp) {
    struct ResultRecord {
        uint32_t id;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(ResultRecord const& other) const { return (id == other.id && value == other.value); }

        std::string asCSVString() const {
            std::ostringstream oss;
            oss << id << ", " << value;
            return oss.str();
        }
    };

    auto testSchema = TestSchemas::getSchemaTemplate("id_val_u32");
    ASSERT_EQ(sizeof(ResultRecord), testSchema->getSchemaSizeInBytes());

    auto query = Query::from("truck").unionWith(Query::from("car"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("truck", testSchema)
                                  .addLogicalSource("car", testSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("truck")
                                  .attachWorkerWithMemorySourceToCoordinator("car");

    // Expected output
    std::stringstream expectedOutput;
    for (auto i = 0_u32; i < 10; ++i) {
        // Creating two new elements, one for each stream
        ResultRecord elementCarStream = {i % 3, i};
        ResultRecord elementTruckStream = {i + 1000, i % 5};

        // Pushing them into the expected output and the testHarness
        expectedOutput << elementCarStream.asCSVString() << "\n";
        expectedOutput << elementTruckStream.asCSVString() << "\n";
        testHarness.pushElement<ResultRecord>(elementCarStream, 2).pushElement<ResultRecord>(elementTruckStream, 3);
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.validate().setupTopology().runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * Test deploying unionWith query with source on two different worker node using top down strategy.
 */
TEST_F(QueryDeploymentTest, testDeployTwoWorkerMergeUsingTopDown) {
    struct ResultRecord {
        uint32_t id;
        uint32_t value;

        // overload the == operator to check if two instances are the same
        bool operator==(ResultRecord const& other) const { return (id == other.id && value == other.value); }

        std::string asCSVString() const {
            std::ostringstream oss;
            oss << id << ", " << value;
            return oss.str();
        }
    };

    auto testSchema = TestSchemas::getSchemaTemplate("id_val_u32");
    ASSERT_EQ(sizeof(ResultRecord), testSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(ResultRecord), testSchema->getSchemaSizeInBytes());

    auto query = Query::from("car").unionWith(Query::from("truck"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("car", testSchema)
                                  .addLogicalSource("truck", testSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("car")
                                  .attachWorkerWithMemorySourceToCoordinator("truck");

    // Expected output
    std::stringstream expectedOutput;
    for (auto i = 0_u32; i < 10; ++i) {
        // Creating two new elements, one for each stream
        ResultRecord elementCarStream = {i % 3, i};
        ResultRecord elementTruckStream = {i + 1000, i % 5};

        // Pushing them into the expected output and the testHarness
        expectedOutput << elementCarStream.asCSVString() << "\n";
        expectedOutput << elementTruckStream.asCSVString() << "\n";
        testHarness.pushElement<ResultRecord>(elementCarStream, 2).pushElement<ResultRecord>(elementTruckStream, 3);
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.validate().setupTopology().runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromStream(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutput) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema = TestSchemas::getSchemaTemplate("id_val_u32");

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());
    auto workerConfig = WorkerConfiguration::create();
    auto query = Query::from("test");
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("test", defaultLogicalSchema)
                                  .attachWorkerWithMemorySourceToWorkerWithId("test", WorkerId(1), workerConfig);

    const auto noTuplesInBuffer = workerConfig->bufferSizeInBytes / defaultLogicalSchema->getSchemaSizeInBytes();
    const auto tuplesForThreeBuffers = noTuplesInBuffer * 3;
    for (uint32_t i = 0; i < tuplesForThreeBuffers; ++i) {
        testHarness.pushElement<Test>({1, i}, 2);// fills record store of source with id 0-(noTuplesInBuffer * 3)
    }
    testHarness.validate().setupTopology();

    // Expected output
    std::ostringstream expectedOutput;
    for (auto rec = 0_u64; rec < tuplesForThreeBuffers; ++rec) {
        expectedOutput << "1, " << rec << std::endl;
    }

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput.str())).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers =
        TestUtils::createExpectedBufferFromCSVString(expectedOutput.str(), outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief Test deploy query with print sink with one worker using top down strategy
 */
TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputUsingTopDownStrategy) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema = TestSchemas::getSchemaTemplate("id_val_u32");

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    auto query = Query::from("test");
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("test", defaultLogicalSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("test");
    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 2);
    }
    testHarness.validate().setupTopology();

    // Expected output
    const auto expectedOutput = "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(QueryDeploymentTest, testDeployTwoWorkerFileOutput) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema = TestSchemas::getSchemaTemplate("id_val_u32");

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    auto query = Query::from("test");
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("test", defaultLogicalSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("test") //2
                                  .attachWorkerWithMemorySourceToCoordinator("test");//3

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 2).pushElement<Test>({1, 1}, 3);
    }
    testHarness.validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    // Expected output
    const auto expectedOutput = "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(QueryDeploymentTest, testSourceSharing) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.bufferSizeInBytes = 1024;

    std::vector<Configurations::SchemaFieldDetail> schemaFiledDetails = {{"id", "UINT64", "0"},
                                                                         {"value", "UINT64", "0"},
                                                                         {"timestamp", "UINT64", "0"}};
    auto schemaType = Configurations::SchemaType::create(schemaFiledDetails);

    auto logicalSource = Configurations::LogicalSourceType::create("window1", schemaType);
    coordinatorConfig->logicalSourceTypes.add(logicalSource);

    coordinatorConfig->worker.enableSourceSharing = true;
    coordinatorConfig->worker.bufferSizeInBytes = 1024;
    coordinatorConfig->worker.queryCompiler.outputBufferOptimizationLevel = QueryCompilation::OutputBufferOptimizationLevel::NO;

    std::promise<bool> start;
    bool started = false;
    auto func1 = [&start, &started](NES::Runtime::TupleBuffer& buffer, uint64_t numTuples) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        if (!started) {
            start.get_future().get();
            started = true;
        }

        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numTuples; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = 0;
        }
    };

    auto lambdaSourceType1 =
        LambdaSourceType::create("window1", "test_stream1", std::move(func1), 2, 2, GatheringMode::INTERVAL_MODE);
    coordinatorConfig->worker.physicalSourceTypes.add(lambdaSourceType1);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);

    std::string outputFilePath1 = getTestResourceFolder() / "testOutput1.out";
    remove(outputFilePath1.c_str());

    std::string outputFilePath2 = getTestResourceFolder() / "testOutput2.out";
    remove(outputFilePath2.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    NES_INFO("testSourceSharing: Submit query");

    auto query1 = Query::from("window1").sink(FileSinkDescriptor::create(outputFilePath1, "CSV_FORMAT", "APPEND"));

    QueryId queryId1 =
        requestHandlerService->validateAndQueueAddQueryRequest(query1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));

    auto query2 = Query::from("window1").sink(FileSinkDescriptor::create(outputFilePath2, "CSV_FORMAT", "APPEND"));

    QueryId queryId2 =
        requestHandlerService->validateAndQueueAddQueryRequest(query2.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    start.set_value(true);

    string expectedContent1 = "window1$id:INTEGER(64 bits),window1$value:INTEGER(64 bits),window1$timestamp:INTEGER(64 bits)\n"
                              "0,0,0\n"
                              "1,1,0\n"
                              "2,2,0\n"
                              "3,3,0\n"
                              "4,4,0\n"
                              "5,5,0\n"
                              "6,6,0\n"
                              "7,7,0\n"
                              "8,8,0\n"
                              "9,9,0\n"
                              "10,0,0\n"
                              "11,1,0\n"
                              "12,2,0\n"
                              "13,3,0\n"
                              "14,4,0\n"
                              "15,5,0\n"
                              "16,6,0\n"
                              "17,7,0\n"
                              "18,8,0\n"
                              "19,9,0\n"
                              "20,0,0\n"
                              "21,1,0\n"
                              "22,2,0\n"
                              "23,3,0\n"
                              "24,4,0\n"
                              "25,5,0\n"
                              "26,6,0\n"
                              "27,7,0\n"
                              "28,8,0\n"
                              "29,9,0\n"
                              "30,0,0\n"
                              "31,1,0\n"
                              "32,2,0\n"
                              "33,3,0\n"
                              "34,4,0\n"
                              "35,5,0\n"
                              "36,6,0\n"
                              "37,7,0\n"
                              "38,8,0\n"
                              "39,9,0\n"
                              "40,0,0\n"
                              "41,1,0\n"
                              "0,0,0\n"
                              "1,1,0\n"
                              "2,2,0\n"
                              "3,3,0\n"
                              "4,4,0\n"
                              "5,5,0\n"
                              "6,6,0\n"
                              "7,7,0\n"
                              "8,8,0\n"
                              "9,9,0\n"
                              "10,0,0\n"
                              "11,1,0\n"
                              "12,2,0\n"
                              "13,3,0\n"
                              "14,4,0\n"
                              "15,5,0\n"
                              "16,6,0\n"
                              "17,7,0\n"
                              "18,8,0\n"
                              "19,9,0\n"
                              "20,0,0\n"
                              "21,1,0\n"
                              "22,2,0\n"
                              "23,3,0\n"
                              "24,4,0\n"
                              "25,5,0\n"
                              "26,6,0\n"
                              "27,7,0\n"
                              "28,8,0\n"
                              "29,9,0\n"
                              "30,0,0\n"
                              "31,1,0\n"
                              "32,2,0\n"
                              "33,3,0\n"
                              "34,4,0\n"
                              "35,5,0\n"
                              "36,6,0\n"
                              "37,7,0\n"
                              "38,8,0\n"
                              "39,9,0\n"
                              "40,0,0\n"
                              "41,1,0\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent1, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent1, outputFilePath2));

    NES_DEBUG("testSourceSharing: Remove query 1");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_DEBUG("testSourceSharing: Remove query 2");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    NES_DEBUG("testSourceSharing: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("testSourceSharing: Test finished");
}

TEST_F(QueryDeploymentTest, testSourceSharingWithFilter) {
    std::string outputFilePath1 = getTestResourceFolder() / "testOutput1.out";
    remove(outputFilePath1.c_str());

    std::string outputFilePath2 = getTestResourceFolder() / "testOutput2.out";
    remove(outputFilePath2.c_str());

    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    coordinatorConfig->worker.bufferSizeInBytes = 1024;

    std::vector<Configurations::SchemaFieldDetail> schemaFiledDetails = {{"id", "UINT64", ""},
                                                                         {"value", "UINT64", ""},
                                                                         {"timestamp", "UINT64", ""}};
    auto schemaType = Configurations::SchemaType::create(schemaFiledDetails);

    auto logicalSource = Configurations::LogicalSourceType::create("window1", schemaType);

    coordinatorConfig->logicalSourceTypes.add(logicalSource);

    coordinatorConfig->worker.enableSourceSharing = true;
    coordinatorConfig->worker.bufferSizeInBytes = 1024;
    coordinatorConfig->worker.queryCompiler.outputBufferOptimizationLevel = QueryCompilation::OutputBufferOptimizationLevel::NO;

    std::promise<bool> start;
    bool started = false;
    auto func1 = [&start, &started](NES::Runtime::TupleBuffer& buffer, uint64_t numTuples) {
        struct Record {
            uint64_t id;
            uint64_t value;
            uint64_t timestamp;
        };

        if (!started) {
            start.get_future().get();
            started = true;
        }

        auto* records = buffer.getBuffer<Record>();
        for (auto u = 0u; u < numTuples; ++u) {
            records[u].id = u;
            //values between 0..9 and the predicate is > 5 so roughly 50% selectivity
            records[u].value = u % 10;
            records[u].timestamp = 0;
        }
    };

    auto lambdaSourceType1 =
        LambdaSourceType::create("window1", "test_stream1", std::move(func1), 2, 2, GatheringMode::INTERVAL_MODE);
    coordinatorConfig->worker.physicalSourceTypes.add(lambdaSourceType1);

    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    NES_INFO("testSourceSharing: Submit query");

    auto query1 = Query::from("window1")
                      .filter(Attribute("id") < 5)
                      .sink(FileSinkDescriptor::create(outputFilePath1, "CSV_FORMAT", "APPEND"));

    QueryId queryId1 =
        requestHandlerService->validateAndQueueAddQueryRequest(query1.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));

    auto query2 = Query::from("window1")
                      .filter(Attribute("id") > 5)
                      .sink(FileSinkDescriptor::create(outputFilePath2, "CSV_FORMAT", "APPEND"));

    QueryId queryId2 =
        requestHandlerService->validateAndQueueAddQueryRequest(query2.getQueryPlan(), Optimizer::PlacementStrategy::TopDown);
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    start.set_value(true);

    string expectedContent1 = "window1$id:INTEGER(64 bits),window1$value:INTEGER(64 bits),window1$timestamp:INTEGER(64 bits)\n"
                              "0,0,0\n"
                              "1,1,0\n"
                              "2,2,0\n"
                              "3,3,0\n"
                              "4,4,0\n"
                              "0,0,0\n"
                              "1,1,0\n"
                              "2,2,0\n"
                              "3,3,0\n"
                              "4,4,0\n";

    string expectedContent2 = "window1$id:INTEGER(64 bits),window1$value:INTEGER(64 bits),window1$timestamp:INTEGER(64 bits)\n"
                              "6,6,0\n"
                              "7,7,0\n"
                              "8,8,0\n"
                              "9,9,0\n"
                              "10,0,0\n"
                              "11,1,0\n"
                              "12,2,0\n"
                              "13,3,0\n"
                              "14,4,0\n"
                              "15,5,0\n"
                              "16,6,0\n"
                              "17,7,0\n"
                              "18,8,0\n"
                              "19,9,0\n"
                              "20,0,0\n"
                              "21,1,0\n"
                              "22,2,0\n"
                              "23,3,0\n"
                              "24,4,0\n"
                              "25,5,0\n"
                              "26,6,0\n"
                              "27,7,0\n"
                              "28,8,0\n"
                              "29,9,0\n"
                              "30,0,0\n"
                              "31,1,0\n"
                              "32,2,0\n"
                              "33,3,0\n"
                              "34,4,0\n"
                              "35,5,0\n"
                              "36,6,0\n"
                              "37,7,0\n"
                              "38,8,0\n"
                              "39,9,0\n"
                              "40,0,0\n"
                              "41,1,0\n"
                              "6,6,0\n"
                              "7,7,0\n"
                              "8,8,0\n"
                              "9,9,0\n"
                              "10,0,0\n"
                              "11,1,0\n"
                              "12,2,0\n"
                              "13,3,0\n"
                              "14,4,0\n"
                              "15,5,0\n"
                              "16,6,0\n"
                              "17,7,0\n"
                              "18,8,0\n"
                              "19,9,0\n"
                              "20,0,0\n"
                              "21,1,0\n"
                              "22,2,0\n"
                              "23,3,0\n"
                              "24,4,0\n"
                              "25,5,0\n"
                              "26,6,0\n"
                              "27,7,0\n"
                              "28,8,0\n"
                              "29,9,0\n"
                              "30,0,0\n"
                              "31,1,0\n"
                              "32,2,0\n"
                              "33,3,0\n"
                              "34,4,0\n"
                              "35,5,0\n"
                              "36,6,0\n"
                              "37,7,0\n"
                              "38,8,0\n"
                              "39,9,0\n"
                              "40,0,0\n"
                              "41,1,0\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent1, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent2, outputFilePath2));

    NES_DEBUG("testSourceSharing: Remove query 1");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_DEBUG("testSourceSharing: Remove query 2");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    NES_DEBUG("testSourceSharing: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("testSourceSharing: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployTwoWorkerFileOutputUsingTopDownStrategy) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema = TestSchemas::getSchemaTemplate("id_val_u32");

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    auto query = Query::from("test");
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("test", defaultLogicalSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test") //2
                           .attachWorkerWithMemorySourceToCoordinator("test");//3

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 2).pushElement<Test>({1, 1}, 3);
    }

    testHarness.validate().setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    // Expected output
    const auto expectedOutput = "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithFilter) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema = TestSchemas::getSchemaTemplate("id_val_u32");

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    auto query = Query::from("test").filter(Attribute("id") < 5);
    auto testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                           .addLogicalSource("test", defaultLogicalSchema)
                           .attachWorkerWithMemorySourceToCoordinator("test");

    for (int i = 0; i < 5; ++i) {
        testHarness.pushElement<Test>({1, 1}, 2).pushElement<Test>({5, 1}, 2);
    }

    testHarness.validate().setupTopology();

    // Expected output
    const auto expectedOutput = "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n"
                                "1, 1\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithFilterWithInProcessTerminationWhenTwoSourcesRunning) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("stream", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("stream", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1000);
    csvSourceType1->setSkipHeader(true);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    auto query = Query::from("stream")
                     .filter(Attribute("id") < 5)
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");

    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithFilterWithInProcessTerminationWhenOneSourceRunning) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("stream", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("stream", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1000);
    csvSourceType1->setSkipHeader(true);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    auto query = Query::from("stream")
                     .filter(Attribute("id") < 5)
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithFilterWithInProcessTerminationWhenNoSourceRunning) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("stream", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("stream", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1000);
    csvSourceType1->setSkipHeader(true);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    auto query = Query::from("stream")
                     .filter(Attribute("id") < 5)
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));
    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    sleep(2);
    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithProjection) {
    struct Test {
        uint32_t id;
        uint32_t value;
    };

    auto defaultLogicalSchema = TestSchemas::getSchemaTemplate("id_val_u32");

    ASSERT_EQ(sizeof(Test), defaultLogicalSchema->getSchemaSizeInBytes());

    auto query = Query::from("test").project(Attribute("id"));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                                  .addLogicalSource("test", defaultLogicalSchema)
                                  .attachWorkerWithMemorySourceToCoordinator("test");

    for (int i = 0; i < 10; ++i) {
        testHarness.pushElement<Test>({1, 1}, 2);
    }
    testHarness.validate().setupTopology();

    // Expected output
    const auto expectedOutput = "1\n"
                                "1\n"
                                "1\n"
                                "1\n"
                                "1\n"
                                "1\n"
                                "1\n"
                                "1\n"
                                "1\n"
                                "1\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(QueryDeploymentTest, testDeployOneWorkerFileOutputWithWrongProjection) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->coordinatorHealthCheckWaitTime = 1;
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create("default_logical", "physical_car");
    defaultSourceType1->setNumberOfBuffersToProduce(3);
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    workerConfig1->workerHealthCheckWaitTime = 1;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath = getTestResourceFolder() / "test.out";
    NES_INFO("QueryDeploymentTest: Submit query");
    auto query = Query::from("default_logical")
                     .project(Attribute("asd"))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    EXPECT_THROW(
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp),
        InvalidQueryException);

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");
}

TEST_F(QueryDeploymentTest, testDeployUndeployMultipleQueriesTwoWorkerFileOutput) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 1");
    auto wrkConf1 = WorkerConfiguration::create();
    wrkConf1->coordinatorPort = (port);
    auto defaultSource1 = DefaultSourceType::create("default_logical", "x1");
    wrkConf1->physicalSourceTypes.add(defaultSource1);
    wrkConf1->enableStatisticOutput = true;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(wrkConf1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    auto wrkConf2 = WorkerConfiguration::create();
    wrkConf2->coordinatorPort = (port);
    auto defaultSource2 = DefaultSourceType::create("default_logical", "x2");
    wrkConf2->physicalSourceTypes.add(defaultSource2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(wrkConf2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    remove(outputFilePath1.c_str());
    remove(outputFilePath2.c_str());

    NES_INFO("QueryDeploymentTest: Submit query");
    auto query1 = Query::from("default_logical").sink(FileSinkDescriptor::create(outputFilePath1, "CSV_FORMAT", "APPEND"));
    QueryId queryId1 =
        requestHandlerService->validateAndQueueAddQueryRequest(query1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    auto query2 = Query::from("default_logical").sink(FileSinkDescriptor::create(outputFilePath2, "CSV_FORMAT", "APPEND"));
    QueryId queryId2 =
        requestHandlerService->validateAndQueueAddQueryRequest(query2.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    string expectedContent = "default_logical$id:INTEGER(32 bits),default_logical$value:INTEGER(64 bits)\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath2));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryDeploymentTest, testOneQueuePerQueryWithOutput) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("stream1", testSchema);
    crd->getSourceCatalog()->addLogicalSource("stream2", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("stream1", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(1);
    csvSourceType1->setSkipHeader(true);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);

    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("stream2", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType2->setNumberOfBuffersToProduce(1);
    csvSourceType2->setSkipHeader(true);
    workerConfig1->physicalSourceTypes.add(csvSourceType2);

    workerConfig1->sourcePinList = ("0,1");
    workerConfig1->queuePinList = ("0,1");
    workerConfig1->numWorkerThreads = (2);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    auto query1 = Query::from("stream1").sink(FileSinkDescriptor::create(outputFilePath1, "CSV_FORMAT", "APPEND"));
    QueryId queryId1 =
        requestHandlerService->validateAndQueueAddQueryRequest(query1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    auto query2 = Query::from("stream2").sink(FileSinkDescriptor::create(outputFilePath2, "CSV_FORMAT", "APPEND"));
    QueryId queryId2 =
        requestHandlerService->validateAndQueueAddQueryRequest(query2.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    string expectedContent1 = "stream1$id:INTEGER(64 bits),stream1$value:INTEGER(64 bits),stream1$timestamp:INTEGER(64 bits)\n"
                              "12,1,1001\n";

    string expectedContent2 = "stream2$id:INTEGER(64 bits),stream2$value:INTEGER(64 bits),stream2$timestamp:INTEGER(64 bits)\n"
                              "12,1,1001\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent1, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent2, outputFilePath2));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryDeploymentTest, testOneQueuePerQueryWithHardShutdownAndStatic) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("stream1", testSchema);
    crd->getSourceCatalog()->addLogicalSource("stream2", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("stream1", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(100);
    csvSourceType1->setSkipHeader(true);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);

    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("stream2", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType2->setNumberOfBuffersToProduce(100);
    csvSourceType2->setSkipHeader(true);
    workerConfig1->physicalSourceTypes.add(csvSourceType2);

    workerConfig1->sourcePinList = ("0,1");
    workerConfig1->queuePinList = ("0,1");
    workerConfig1->numWorkerThreads = (2);

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    auto query1 = Query::from("stream1").sink(FileSinkDescriptor::create(outputFilePath1, "CSV_FORMAT", "APPEND"));
    QueryId queryId1 =
        requestHandlerService->validateAndQueueAddQueryRequest(query1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    auto query2 = Query::from("stream2").sink(FileSinkDescriptor::create(outputFilePath2, "CSV_FORMAT", "APPEND"));
    QueryId queryId2 =
        requestHandlerService->validateAndQueueAddQueryRequest(query2.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    string expectedContent1 = "stream1$id:INTEGER(64 bits),stream1$value:INTEGER(64 bits),stream1$timestamp:INTEGER(64 bits)\n"
                              "1,12,1001\n";

    string expectedContent2 = "stream2$id:INTEGER(64 bits),stream2$value:INTEGER(64 bits),stream2$timestamp:INTEGER(64 bits)\n"
                              "1,12,1001\n";

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog, std::chrono::seconds(5)));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog, std::chrono::seconds(5)));

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

TEST_F(QueryDeploymentTest, testDeployUndeployMultipleQueriesOnTwoWorkerFileOutputWithQueryMerging) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    DefaultSourceTypePtr defaultSourceType1 = DefaultSourceType::create("default_logical", "physical_car");
    workerConfig1->physicalSourceTypes.add(defaultSourceType1);
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    DefaultSourceTypePtr defaultSourceType2 = DefaultSourceType::create("default_logical", "physical_car");
    workerConfig2->physicalSourceTypes.add(defaultSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    string query1 = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath1
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId1 = requestHandlerService->validateAndQueueAddQueryRequest(query1, Optimizer::PlacementStrategy::BottomUp);
    string query2 = R"(Query::from("default_logical").sink(FileSinkDescriptor::create(")" + outputFilePath2
        + R"(", "CSV_FORMAT", "APPEND"));)";
    QueryId queryId2 = requestHandlerService->validateAndQueueAddQueryRequest(query2, Optimizer::PlacementStrategy::BottomUp);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    string expectedContent = "default_logical$id:INTEGER(32 bits),default_logical$value:INTEGER(64 bits)\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n"
                             "1,1\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath1));
    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath2));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog));

    NES_INFO("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_INFO("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

/**
 * Test deploying joinWith query with source on two different worker node using top down strategy
 */
TEST_F(QueryDeploymentTest, testDeployTwoWorkerJoinUsingTopDownOnSameSchema) {
    struct Test {
        uint64_t id;
        uint64_t value;
        uint64_t timestamp;
    };

    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    ASSERT_EQ(sizeof(Test), testSchema->getSchemaSizeInBytes());

    auto csvSourceType = CSVSourceType::create("window", "window_p1");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(2);
    csvSourceType->setSkipHeader(false);

    auto csvSourceType2 = CSVSourceType::create("window2", "window_p2");
    csvSourceType2->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(false);
    auto query = Query::from("window")
                     .joinWith(Query::from("window2"))
                     .where(Attribute("id") == Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));

    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                                  .addLogicalSource("window", testSchema)
                                  .addLogicalSource("window2", testSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType2)
                                  .validate()
                                  .setupTopology();

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);

    // Expected output
    const auto expectedOutput = "1000, 2000, 4, 1, 1002, 4, 1, 1002\n"
                                "1000, 2000, 1, 1, 1000, 1, 1, 1000\n"
                                "1000, 2000, 12, 1, 1001, 12, 1, 1001\n"
                                "2000, 3000, 1, 2, 2000, 1, 2, 2000\n"
                                "2000, 3000, 11, 2, 2001, 11, 2, 2001\n"
                                "2000, 3000, 16, 2, 2002, 16, 2, 2002\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

TEST_F(QueryDeploymentTest, testOneQueuePerQueryWithHardShutdown) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");
    //register logical source
    auto testSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    crd->getSourceCatalog()->addLogicalSource("stream1", testSchema);
    crd->getSourceCatalog()->addLogicalSource("stream2", testSchema);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    workerConfig1->coordinatorPort = port;
    CSVSourceTypePtr csvSourceType1 = CSVSourceType::create("stream1", "test_stream");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setGatheringInterval(1);
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType1->setNumberOfBuffersToProduce(100);
    csvSourceType1->setSkipHeader(true);
    workerConfig1->physicalSourceTypes.add(csvSourceType1);

    CSVSourceTypePtr csvSourceType2 = CSVSourceType::create("stream2", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(1);
    csvSourceType2->setNumberOfBuffersToProduce(100);
    csvSourceType2->setSkipHeader(true);
    workerConfig1->physicalSourceTypes.add(csvSourceType2);

    workerConfig1->sourcePinList = "0,1";
    workerConfig1->numWorkerThreads = 2;
    workerConfig1->queryManagerMode = Runtime::QueryExecutionMode::Dynamic;

    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    std::string outputFilePath1 = getTestResourceFolder() / "test1.out";
    std::string outputFilePath2 = getTestResourceFolder() / "test2.out";

    NES_INFO("QueryDeploymentTest: Submit query");
    auto query1 = Query::from("stream1").sink(FileSinkDescriptor::create(outputFilePath1, "CSV_FORMAT", "APPEND"));
    QueryId queryId1 =
        requestHandlerService->validateAndQueueAddQueryRequest(query1.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    auto query2 = Query::from("stream2").sink(FileSinkDescriptor::create(outputFilePath2, "CSV_FORMAT", "APPEND"));
    QueryId queryId2 =
        requestHandlerService->validateAndQueueAddQueryRequest(query2.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);
    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();

    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId1, queryCatalog));
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId2, queryCatalog));

    string expectedContent1 = "stream1$value:INTEGER(64 bits),stream1$id:INTEGER(64 bits),stream1$timestamp:INTEGER(64 bits)\n"
                              "1,12,1001\n";

    string expectedContent2 = "stream2$value:INTEGER(64 bits),stream2$id:INTEGER(64 bits),stream2$timestamp:INTEGER(64 bits)\n"
                              "1,12,1001\n";

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId1, queryCatalog, std::chrono::seconds(5)));

    NES_INFO("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId2, queryCatalog, std::chrono::seconds(5)));

    NES_INFO("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_INFO("QueryDeploymentTest: Test finished");

    int response1 = remove(outputFilePath1.c_str());
    EXPECT_EQ(response1, 0);

    int response2 = remove(outputFilePath2.c_str());
    EXPECT_EQ(response2, 0);
}

/**
 * Test deploying join query with source on two different worker node using top down strategy.
 */
//TODO: this test will be enabled once we have the renaming function using as
//TODO: prevent self join #3686
TEST_F(QueryDeploymentTest, DISABLED_testSelfJoinTumblingWindow) {

    struct Window {
        uint64_t value;
        uint64_t id;
        uint64_t timestamp;
    };

    auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());

    auto csvSourceType = CSVSourceType::create("window", "window_p1");
    csvSourceType->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType->setNumberOfBuffersToProduce(2);
    csvSourceType->setSkipHeader(true);

    auto query = Query::from("window")
                     .as("w1")
                     .joinWith(Query::from("window").as("w2"))
                     .where(Attribute("id") == Attribute("id"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("window", windowSchema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType)
                                  .validate()
                                  .setupTopology();

    // Expected output
    const auto expectedOutput = "1000, 2000, 4, 1, 4, 1002, 1, 4, 1002\n"
                                "1000, 2000, 12, 1, 12, 1001, 1, 12, 1001\n"
                                "2000, 3000, 1, 2, 1, 2000, 2, 1, 2000\n"
                                "2000, 3000, 11, 2, 11, 2001, 2, 11, 2001\n"
                                "2000, 3000, 16, 2, 16, 2002, 2, 16, 2002\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * Test deploying join with different sources and different Speed
 */
TEST_F(QueryDeploymentTest, testJoinWithDifferentSourceDifferentSpeedTumblingWindow) {
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

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64");

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    auto csvSourceType1 = CSVSourceType::create("window1", "window_p1");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    csvSourceType1->setGatheringInterval(0);
    csvSourceType1->setSkipHeader(false);

    auto csvSourceType2 = CSVSourceType::create("window2", "window_p2");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setGatheringInterval(1);
    csvSourceType2->setSkipHeader(false);

    auto query = Query::from("window1")
                     .joinWith(Query::from("window2"))
                     .where(Attribute("id") == Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("window1", windowSchema)
                                  .addLogicalSource("window2", window2Schema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType1)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType2)
                                  .validate()
                                  .setupTopology();

    // Expected output
    const auto expectedOutput = "2000, 3000, 1, 2, 2000, 1, 2, 2010\n"
                                "1000, 2000,  4, 1, 1002, 4, 3, 1102\n"
                                "1000, 2000,  4, 1, 1002, 4, 3, 1112\n"
                                "2000, 3000, 11, 2, 2001, 11, 2, 2301\n"
                                "1000, 2000, 12, 1, 1001, 12, 5, 1011\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/*
 * @brief Test if sliding windows properly trigger the join
 */
TEST_F(QueryDeploymentTest, testJoinWithSlidingWindow) {
    struct Car {
        uint64_t id;
        uint64_t value;
        uint64_t value2;
        uint64_t timestamp;
    };

    auto carSchema = TestSchemas::getSchemaTemplate("id_2val_time_u64");

    ASSERT_EQ(sizeof(Car), carSchema->getSchemaSizeInBytes());

    auto queryWithWindowOperator =
        Query::from("car1")
            .joinWith(Query::from("car2")
                          .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                          .byKey(Attribute("id"))
                          .apply(Count())
                          .project(Attribute("start").as("timestamp"), Attribute("end"), Attribute("id"), Attribute("count")))
            .where(Attribute("id") == Attribute("id"))
            .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Seconds(1)));

    auto testHarness = TestHarness(queryWithWindowOperator, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                           .addLogicalSource("car1", carSchema)
                           .addLogicalSource("car2", carSchema)
                           .attachWorkerWithMemorySourceToCoordinator("car1")
                           .attachWorkerWithMemorySourceToCoordinator("car2");

    ASSERT_EQ(testHarness.getWorkerCount(), 2UL);
    // Sliding Windows Produced (start, end, key, value)
    //[0, 1000, 1, 3]
    //[500, 1500, 1, 2]
    //[1000, 2000, 1, 1]
    //[1500, 2500, 1, 1]
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 100ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 700ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 800ULL}, 2);
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 1600ULL}, 2);

    // Tumbling Windows of the Join (start, end, listofentries)
    //[0, 1000, {1,2,3}]
    //[1000, 2000, {4}]
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 100ULL}, 3);
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 700ULL}, 3);
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 800ULL}, 3);
    testHarness.pushElement<Car>({1ULL, 15ULL, 15ULL, 1600ULL}, 3);

    testHarness.validate().setupTopology();

    // Run the query and get the actual dynamic buffers
    auto expectedOutput = "0,1000,1,15,15,100,0,1000,1,3\n"
                          "0,1000,1,15,15,100,500,1500,1,2\n"
                          "0,1000,1,15,15,700,0,1000,1,3\n"
                          "0,1000,1,15,15,700,500,1500,1,2\n"
                          "0,1000,1,15,15,800,0,1000,1,3\n"
                          "0,1000,1,15,15,800,500,1500,1,2\n"
                          "1000,2000,1,15,15,1600,1000,2000,1,1\n"
                          "1000,2000,1,15,15,1600,1500,2500,1,1\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * Test deploying join with different three sources
 */
TEST_F(QueryDeploymentTest, testJoinWithThreeSources) {
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

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64");

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    auto csvSourceType1 = CSVSourceType::create("window1", "window_p1");
    csvSourceType1->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1->setNumberOfBuffersToProduce(2);
    csvSourceType1->setSkipHeader(false);

    auto csvSourceType2 = CSVSourceType::create("window1", "window_p2");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    csvSourceType2->setSkipHeader(false);
    auto csvSourceType3 = CSVSourceType::create("window2", "window_p3");
    csvSourceType3->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    csvSourceType3->setSkipHeader(false);
    auto query = Query::from("window1")
                     .joinWith(Query::from("window2"))
                     .where(Attribute("id") == Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())

                                  .addLogicalSource("window1", windowSchema)
                                  .addLogicalSource("window2", window2Schema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType1)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType2)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType3)
                                  .validate()
                                  .setupTopology();

    // Expected output
    const auto expectedOutput = "1000, 2000, 4, 1, 1002, 4, 3, 1102\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1112\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1102\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1112\n"
                                "1000, 2000, 12, 1, 1001, 12, 5, 1011\n"
                                "1000, 2000, 12, 1, 1001, 12, 5, 1011\n"
                                "2000, 3000, 1, 2, 2000, 1, 2, 2010\n"
                                "2000, 3000, 1, 2, 2000, 1, 2, 2010\n"
                                "2000, 3000, 11, 2, 2001, 11, 2, 2301\n"
                                "2000, 3000, 11, 2, 2001, 11, 2, 2301\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * Test deploying join with four different sources
 */
TEST_F(QueryDeploymentTest, testJoinWithFourSources) {
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

    const auto windowSchema = TestSchemas::getSchemaTemplate("id_val_time_u64");
    const auto window2Schema = TestSchemas::getSchemaTemplate("id2_val2_time_u64");

    ASSERT_EQ(sizeof(Window), windowSchema->getSchemaSizeInBytes());
    ASSERT_EQ(sizeof(Window2), window2Schema->getSchemaSizeInBytes());

    auto csvSourceType1_1 = CSVSourceType::create("window1", "window1_p1");
    csvSourceType1_1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window.csv");
    csvSourceType1_1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1_1->setNumberOfBuffersToProduce(2);
    csvSourceType1_1->setSkipHeader(false);

    auto csvSourceType1_2 = CSVSourceType::create("window1", "window1_p2");
    csvSourceType1_2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType1_2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType1_2->setNumberOfBuffersToProduce(2);
    csvSourceType1_2->setSkipHeader(false);

    auto csvSourceType2_1 = CSVSourceType::create("window2", "window2_p1");
    csvSourceType2_1->setFilePath(std::string(TEST_DATA_DIRECTORY) + "window2.csv");
    csvSourceType2_1->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2_1->setNumberOfBuffersToProduce(2);
    csvSourceType2_1->setSkipHeader(false);

    auto csvSourceType2_2 = CSVSourceType::create("window2", "window2_p2");
    csvSourceType2_2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    csvSourceType2_2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2_2->setNumberOfBuffersToProduce(2);
    csvSourceType2_2->setSkipHeader(false);

    auto query = Query::from("window1")
                     .joinWith(Query::from("window2"))
                     .where(Attribute("id") == Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)));
    TestHarness testHarness = TestHarness(query, *restPort, *rpcCoordinatorPort, getTestResourceFolder())
                                  .addLogicalSource("window1", windowSchema)
                                  .addLogicalSource("window2", window2Schema)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType1_1)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType1_2)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType2_1)
                                  .attachWorkerWithCSVSourceToCoordinator(csvSourceType2_2)
                                  .validate()
                                  .setupTopology();

    // Expected output
    const auto expectedOutput = "1000, 2000, 4, 1, 1002, 4, 3, 1102\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1112\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1102\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1112\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1102\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1112\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1102\n"
                                "1000, 2000, 4, 1, 1002, 4, 3, 1112\n"
                                "1000, 2000, 12, 1, 1001, 12, 5, 1011\n"
                                "1000, 2000, 12, 1, 1001, 12, 5, 1011\n"
                                "1000, 2000, 12, 1, 1001, 12, 5, 1011\n"
                                "1000, 2000, 12, 1, 1001, 12, 5, 1011\n"
                                "2000, 3000, 1, 2, 2000, 1, 2, 2010\n"
                                "2000, 3000, 1, 2, 2000, 1, 2, 2010\n"
                                "2000, 3000, 1, 2, 2000, 1, 2, 2010\n"
                                "2000, 3000, 1, 2, 2000, 1, 2, 2010\n"
                                "2000, 3000, 11, 2, 2001, 11, 2, 2301\n"
                                "2000, 3000, 11, 2, 2001, 11, 2, 2301\n"
                                "2000, 3000, 11, 2, 2001, 11, 2, 2301\n"
                                "2000, 3000, 11, 2, 2001, 11, 2, 2301\n";

    // Run the query and get the actual dynamic buffers
    auto actualBuffers = testHarness.runQuery(Util::countLines(expectedOutput)).getOutput();

    // Comparing equality
    const auto outputSchema = testHarness.getOutputSchema();
    auto tmpBuffers = TestUtils::createExpectedBufferFromCSVString(expectedOutput, outputSchema, testHarness.getBufferManager());
    auto expectedBuffers = TestUtils::createTestTupleBuffers(tmpBuffers, outputSchema);
    EXPECT_TRUE(TestUtils::buffersContainSameTuples(expectedBuffers, actualBuffers));
}

/**
 * @brief This tests just outputs the default source for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0] => Join 2
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0] => Join 1
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
//TODO: this test will be enabled once the following issue is resolved
//TODO: Distributed Window Aggregation #4557
TEST_F(QueryDeploymentTest, DISABLED_testJoin2WithDifferentSourceTumblingWindowDistributed) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto window = Schema::create()
                      ->addField(createField("win1", BasicType::UINT64))
                      ->addField(createField("id1", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("window1", window);

    auto window2 = Schema::create()
                       ->addField(createField("win2", BasicType::UINT64))
                       ->addField(createField("id2", BasicType::UINT64))
                       ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("window2", window2);

    auto window3 = Schema::create()
                       ->addField(createField("win3", BasicType::UINT64))
                       ->addField(createField("id3", BasicType::UINT64))
                       ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("window3", window3);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = *rpcCoordinatorPort;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = *rpcCoordinatorPort;
    auto csvSourceType2 = CSVSourceType::create("window2", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = *rpcCoordinatorPort;
    auto csvSourceType3 = CSVSourceType::create("window3", "test_stream");
    csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    workerConfig3->physicalSourceTypes.add(csvSourceType3);

    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("QueryDeploymentTest: Worker3 started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->coordinatorPort = *rpcCoordinatorPort;
    auto csvSourceType4 = CSVSourceType::create("window3", "test_stream");
    csvSourceType4->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    workerConfig4->physicalSourceTypes.add(csvSourceType4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("QueryDeploymentTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamTumblingWindowDistributed.out";
    remove(outputFilePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");

    auto query = Query::from("window1")
                     .joinWith(Query::from("window2"))
                     .where(Attribute("id1") == Attribute("id2"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                     .joinWith(Query::from("window3"))
                     .where(Attribute("id1") == Attribute("id3"))
                     .window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000)))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2window3$start:INTEGER(64 bits),window1window2window3$end:INTEGER(64 "
                             "bits),window1window2window3$key:INTEGER(64 bits),window1window2$"
                             "start:INTEGER(64 bits),window1window2$end:INTEGER(64 bits),window1window2$key:INTEGER(64 "
                             "bits),window1$win1:INTEGER(64 bits),window1$id1:INTEGER(64 bits),window1$"
                             "timestamp:INTEGER(64 bits),window2$win2:INTEGER(64 bits),window2$id2:INTEGER(64 "
                             "bits),window2$timestamp:INTEGER(64 bits),window3$win3:INTEGER(64 bits),window3$id3:"
                             "INTEGER(64 bits),window3$timestamp:INTEGER(64 bits)\n"
                             "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
                             "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
                             "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("QueryDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("QueryDeploymentTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("QueryDeploymentTest: Test finished");
}

/**
 * @brief This tests just outputs the default source for a hierarchy with one relay which also produces data by itself
 * Topology:
    PhysicalNode[id=1, ip=127.0.0.1, resourceCapacity=12, usedResource=0] => Join 2
    |--PhysicalNode[id=2, ip=127.0.0.1, resourceCapacity=1, usedResource=0] => Join 1
    |  |--PhysicalNode[id=6, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=5, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
    |  |--PhysicalNode[id=4, ip=127.0.0.1, resourceCapacity=12, usedResource=0]
 */
//TODO: this test will be enabled once the following issue is resolved
//TODO: Distributed Window Aggregation #4557
TEST_F(QueryDeploymentTest, DISABLED_testJoin2WithDifferentSourceSlidingWindowDistributed) {
    CoordinatorConfigurationPtr coordinatorConfig = CoordinatorConfiguration::createDefault();
    coordinatorConfig->rpcPort = *rpcCoordinatorPort;
    coordinatorConfig->restPort = *restPort;
    NES_INFO("QueryDeploymentTest: Start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(coordinatorConfig);
    uint64_t port = crd->startCoordinator(/**blocking**/ false);//id=1
    EXPECT_NE(port, 0UL);
    //register logical source qnv
    auto window = Schema::create()
                      ->addField(createField("win1", BasicType::UINT64))
                      ->addField(createField("id1", BasicType::UINT64))
                      ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("window1", window);

    auto window2 = Schema::create()
                       ->addField(createField("win2", BasicType::UINT64))
                       ->addField(createField("id2", BasicType::UINT64))
                       ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("window2", window2);

    auto window3 = Schema::create()
                       ->addField(createField("win3", BasicType::UINT64))
                       ->addField(createField("id3", BasicType::UINT64))
                       ->addField(createField("timestamp", BasicType::UINT64));
    crd->getSourceCatalog()->addLogicalSource("window3", window3);
    NES_DEBUG("QueryDeploymentTest: Coordinator started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 1");
    WorkerConfigurationPtr workerConfig1 = WorkerConfiguration::create();
    workerConfig1->coordinatorPort = port;
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(std::move(workerConfig1));
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("QueryDeploymentTest: Worker1 started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 2");
    WorkerConfigurationPtr workerConfig2 = WorkerConfiguration::create();
    workerConfig2->coordinatorPort = port;
    auto csvSourceType2 = CSVSourceType::create("window2", "test_stream");
    csvSourceType2->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window.csv");
    csvSourceType2->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType2->setNumberOfBuffersToProduce(2);
    workerConfig2->physicalSourceTypes.add(csvSourceType2);
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>(std::move(workerConfig2));
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    wrk2->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("QueryDeploymentTest: Worker2 started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 3");
    WorkerConfigurationPtr workerConfig3 = WorkerConfiguration::create();
    workerConfig3->coordinatorPort = port;
    auto csvSourceType3 = CSVSourceType::create("window3", "test_stream");
    csvSourceType3->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window2.csv");
    csvSourceType3->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType3->setNumberOfBuffersToProduce(2);
    workerConfig3->physicalSourceTypes.add(csvSourceType3);
    NesWorkerPtr wrk3 = std::make_shared<NesWorker>(std::move(workerConfig3));
    bool retStart3 = wrk3->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart3);
    wrk3->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("QueryDeploymentTest: Worker3 started successfully");

    NES_DEBUG("QueryDeploymentTest: Start worker 4");
    WorkerConfigurationPtr workerConfig4 = WorkerConfiguration::create();
    workerConfig4->coordinatorPort = port;
    auto csvSourceType4 = CSVSourceType::create("window3", "test_stream");
    csvSourceType4->setFilePath(std::filesystem::path(TEST_DATA_DIRECTORY) / "window4.csv");
    csvSourceType4->setNumberOfTuplesToProducePerBuffer(3);
    csvSourceType4->setNumberOfBuffersToProduce(2);
    workerConfig4->physicalSourceTypes.add(csvSourceType4);
    NesWorkerPtr wrk4 = std::make_shared<NesWorker>(std::move(workerConfig4));
    bool retStart4 = wrk4->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart4);
    wrk4->replaceParent(WorkerId(1), WorkerId(2));
    NES_INFO("QueryDeploymentTest: Worker4 started successfully");

    std::string outputFilePath = getTestResourceFolder() / "testTwoJoinsWithDifferentStreamSlidingWindowDistributed.out";
    remove(outputFilePath.c_str());

    RequestHandlerServicePtr requestHandlerService = crd->getRequestHandlerService();
    auto queryCatalog = crd->getQueryCatalog();

    NES_INFO("QueryDeploymentTest: Submit query");

    auto query = Query::from("window1")
                     .joinWith(Query::from("window2"))
                     .where(Attribute("id1") == Attribute("id2"))
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                     .joinWith(Query::from("window3"))
                     .where(Attribute("id1") == Attribute("id3"))
                     .window(SlidingWindow::of(EventTime(Attribute("timestamp")), Seconds(1), Milliseconds(500)))
                     .sink(FileSinkDescriptor::create(outputFilePath, "CSV_FORMAT", "APPEND"));

    QueryId queryId =
        requestHandlerService->validateAndQueueAddQueryRequest(query.getQueryPlan(), Optimizer::PlacementStrategy::BottomUp);

    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    EXPECT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk3, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(wrk4, queryId, globalQueryPlan, 2));
    EXPECT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 2));

    string expectedContent = "window1window2window3$start:INTEGER(64 bits),window1window2window3$end:INTEGER(64 "
                             "bits),window1window2window3$key:INTEGER(64 bits),window1window2$"
                             "start:INTEGER(64 bits),window1window2$end:INTEGER(64 bits),window1window2$key:INTEGER(64 "
                             "bits),window1$win1:INTEGER(64 bits),window1$id1:INTEGER(64 bits),window1$"
                             "timestamp:INTEGER(64 bits),window2$win2:INTEGER(64 bits),window2$id2:INTEGER(64 "
                             "bits),window2$timestamp:INTEGER(64 bits),window3$win3:INTEGER(64 bits),window3$id3:"
                             "INTEGER(64 bits),window3$timestamp:INTEGER(64 bits)\n"
                             "1000,2000,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
                             "1000,2000,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
                             "1000,2000,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
                             "1000,2000,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
                             "500,1500,4,1000,2000,4,1,4,1002,3,4,1102,4,4,1001\n"
                             "500,1500,4,1000,2000,4,1,4,1002,3,4,1112,4,4,1001\n"
                             "500,1500,4,500,1500,4,1,4,1002,3,4,1102,4,4,1001\n"
                             "500,1500,4,500,1500,4,1,4,1002,3,4,1112,4,4,1001\n"
                             "1000,2000,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
                             "1000,2000,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n"
                             "500,1500,12,1000,2000,12,1,12,1001,5,12,1011,1,12,1300\n"
                             "500,1500,12,500,1500,12,1,12,1001,5,12,1011,1,12,1300\n";

    EXPECT_TRUE(TestUtils::checkOutputOrTimeout(expectedContent, outputFilePath));

    NES_DEBUG("QueryDeploymentTest: Remove query");
    EXPECT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("QueryDeploymentTest: Stop worker 1");
    bool retStopWrk1 = wrk1->stop(true);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("QueryDeploymentTest: Stop worker 2");
    bool retStopWrk2 = wrk2->stop(true);
    EXPECT_TRUE(retStopWrk2);

    NES_DEBUG("QueryDeploymentTest: Stop worker 3");
    bool retStopWrk3 = wrk3->stop(true);
    EXPECT_TRUE(retStopWrk3);

    NES_DEBUG("QueryDeploymentTest: Stop worker 4");
    bool retStopWrk4 = wrk4->stop(true);
    EXPECT_TRUE(retStopWrk4);

    NES_DEBUG("QueryDeploymentTest: Stop Coordinator");
    bool retStopCord = crd->stopCoordinator(true);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("QueryDeploymentTest: Test finished");
}
}// namespace NES
