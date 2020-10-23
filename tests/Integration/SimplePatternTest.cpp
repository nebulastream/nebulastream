#include <Components/NesCoordinator.hpp>
#include <Components/NesWorker.hpp>
#include <Services/QueryService.hpp>
#include <Util/Logger.hpp>
#include <Util/TestUtils.hpp>
#include <Util/UtilityFunctions.hpp>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>

//used tests: QueryCatalogTest, QueryTest
namespace fs = std::filesystem;
namespace NES {

//FIXME: This is a hack to fix issue with unreleased RPC port after shutting down the servers while running tests in continuous succession
// by assigning a different RPC port for each test case
uint64_t rpcPort = 4000;

class SimplePatternTest : public testing::Test {
  public:
    static void SetUpTestCase() {
        NES::setupLogging("SimplePatternTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SimplePatternTest test class.");
    }

    void SetUp() {
        rpcPort = rpcPort + 30;
        restPort = restPort + 2;
    }

    void TearDown() {
        std::cout << "Tear down SimplePatternTest class." << std::endl;
    }

    std::string ipAddress = "127.0.0.1";
    uint64_t restPort = 8081;
};

/* 1. Test
 * Translation of a simple pattern (1 Stream) into a query
 */
TEST_F(SimplePatternTest, testPatternWithFilter) {
    NES_DEBUG("SimplePatternTest: start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_DEBUG("SimplePatternTest: coordinator started successfully");

    NES_DEBUG("SimplePatternTest: start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_DEBUG("SimplePatternTest: worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    std::string query = R"(Pattern::from("default_logical").filter(Attribute("value") < 42).sink(PrintSinkDescriptor::create()); )";
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    NES_INFO("SimplePatternTest: Remove query");
    queryService->validateAndQueueStopRequest(queryId);
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    NES_DEBUG("SimplePatternTest: stop worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("SimplePatternTest: stop coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("SimplePatternTest: test finished");
}

/* 2.Test
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and check the output
 */
TEST_F(SimplePatternTest, testPatternWithTestStream) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_DEBUG("coordinator started successfully");

    NES_DEBUG("start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>(ipAddress, std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_DEBUG("worker1 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    //TODO: update CHAR (sensor id is in result set )
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfigPtr conf = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/QnV_short.csv",
                                                                   1, 0, 1,
                                                                   "test_stream", "QnV");
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Pattern::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 1));

    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    //TODO Patternname waiting for String support in map operator

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|sensor_id:CHAR|timestamp:UINT64|velocity:FLOAT32|quantity:UINT64|PatternId:INT32|\n"
        "+----------------------------------------------------+\n"
        "|R2000073|1543624020000|102.629631|8|1|\n"
        "|R2000070|1543625280000|108.166664|5|1|\n"
        "+----------------------------------------------------+";

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    NES_DEBUG("content=" << content);
    NES_DEBUG("expContent=" << expectedContent);
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 3.Test
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and check the output
 */
TEST_F(SimplePatternTest, testPatternWithTestStreamAndMultiWorkers) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("SimplePatternTest: Coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    NES_INFO("SimplePatternTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("SimplePatternTest: Worker2 started successfully");

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();

    //register logical stream qnv
    //TODO: update CHAR (sensor id is in result set )
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);

   //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/QnV_short_R2000070.csv",
                                                                  1, 0, 1,
                                                                  "test_stream_R2000070", "QnV");
    wrk1->registerPhysicalStream(conf70);

    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/QnV_short_R2000073.csv",
                                                                  1, 0, 1,
                                                                  "test_stream_R2000073", "QnV");
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath =
        "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Pattern::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
                        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);
    auto globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));

    ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string line;
    int rowNumber = 0;
    bool resultWrk1 = false;
    bool resultWrk2 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        rowNumber++;
        if ((rowNumber > 3 && rowNumber < 6) || (rowNumber > 8 && rowNumber < 11) ) {
            std::vector<string> content = UtilityFunctions::split(line, '|');
            if (content.at(0) == "R2000073") {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): content=" << content.at(2));
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): expContent= 102.629631");
                EXPECT_EQ(content.at(2), "102.629631");
                resultWrk1 = true;
            }
            else {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): content=" << content.at(2));
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): expContent= 108.166664");
                EXPECT_EQ(content.at(2), "108.166664");
                resultWrk2 = true;
            }
        }
    }

    EXPECT_TRUE((resultWrk1 && resultWrk2));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* 6.Test
 * Here, we test if we can use merge operator for patterns and create complex events with it
 */
TEST_F(SimplePatternTest, testPatternWithTestStreamAndMultiWorkerMerge) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>(ipAddress, restPort, rpcPort);
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_INFO("SimplePatternTest: Coordinator started successfully");

    NES_INFO("SimplePatternTest: Start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_INFO("SimplePatternTest: Worker1 started successfully");

    NES_INFO("QueryDeploymentTest: Start worker 2");
    NesWorkerPtr wrk2 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 20, port + 21, NodeType::Sensor);
    bool retStart2 = wrk2->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart2);
    NES_INFO("SimplePatternTest: Worker2 started successfully");

    //register logical stream qnv
    //TODO: update CHAR (sensor id is in result set )
    std::string qnv =
        R"(Schema::create()->addField("sensor_id", DataTypeFactory::createFixedChar(8))->addField(createField("timestamp", UINT64))->addField(createField("velocity", FLOAT32))->addField(createField("quantity", UINT64));)";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);
    wrk2->registerLogicalStream("QnV1", testSchemaFileName);


    //register physical stream R2000070
    PhysicalStreamConfigPtr conf70 = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/QnV_short_R2000070.csv",
                                                                  1, 0, 2,
                                                                  "test_stream_R2000070", "QnV");
    wrk1->registerPhysicalStream(conf70);

    //register physical stream R2000073
    PhysicalStreamConfigPtr conf73 = PhysicalStreamConfig::create("CSVSource", "../tests/test_data/QnV_short_R2000073.csv",
                                                                  1, 0, 2,
                                                                  "test_stream_R2000073", "QnV1");
    wrk2->registerPhysicalStream(conf73);

    std::string outputFilePath =
        "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    NES_INFO("SimplePatternTest: Submit merge pattern");
    std::string query = R"(Pattern::from("QnV").merge(Pattern::from("QnV1")).filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")" + outputFilePath + "\"));";

    QueryServicePtr queryService = crd->getQueryService();
    QueryCatalogPtr queryCatalog = crd->getQueryCatalog();
    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");


    GlobalQueryPlanPtr globalQueryPlan = crd->getGlobalQueryPlan();
    ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 2));
    //ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 2));
    //ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, globalQueryPlan, 4));

    EXPECT_NE(queryId, INVALID_QUERY_ID);


   // ASSERT_TRUE(TestUtils::waitForQueryToStart(queryId, queryCatalog));
    //ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, globalQueryPlan, 1));
    //ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk2, queryId, globalQueryPlan, 1));

    //ASSERT_TRUE(queryService->validateAndQueueStopRequest(queryId));
    //ASSERT_TRUE(TestUtils::checkStoppedOrTimeout(queryId, queryCatalog));

    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());

    std::string line;
    int rowNumber = 0;
    bool resultWrk1 = false;
    bool resultWrk2 = false;

    while (std::getline(ifs, line)) {
        NES_INFO("print line from content" << line);
        rowNumber++;
        if ((rowNumber > 3 && rowNumber < 6) || (rowNumber > 8 && rowNumber < 11) ) {
            std::vector<string> content = UtilityFunctions::split(line, '|');
            if (content.at(0) == "R2000073") {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): content=" << content.at(2));
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): expContent= 102.629631");
                EXPECT_EQ(content.at(2), "102.629631");
                resultWrk1 = true;
            }
            else {
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): content=" << content.at(2));
                NES_INFO("SimplePatternTest (testPatternWithTestStreamAndMultiWorkers): expContent= 108.166664");
                EXPECT_EQ(content.at(2), "108.166664");
                resultWrk2 = true;
            }
        }
    }

    EXPECT_TRUE((resultWrk1 && resultWrk2));

    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    bool retStopWrk2 = wrk2->stop(false);
    EXPECT_TRUE(retStopWrk2);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}// namespace NES