#include <iostream>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>
#include <Util/TestUtils.hpp>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <Services/QueryService.hpp>

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
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

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
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("127.0.0.1", std::to_string(port), "127.0.0.1", port + 10, port + 11, NodeType::Sensor);
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
    PhysicalStreamConfig conf;
    conf.logicalStreamName = "QnV";
    conf.physicalStreamName = "test_stream";
    conf.sourceType = "CSVSource";
    conf.sourceConfig = "../tests/test_data/QnV_short.csv";
    conf.numberOfBuffersToProduce = 1;
    conf.sourceFrequency = 1;
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testPatternWithTestStream.out";
    remove(outputFilePath.c_str());

    //register query
    std::string query = R"(Pattern::from("QnV").filter(Attribute("velocity") > 100).sink(FileSinkDescriptor::create(")"
        + outputFilePath + "\")); ";

    QueryId queryId = queryService->validateAndQueueAddRequest(query, "BottomUp");
    EXPECT_NE(queryId, INVALID_QUERY_ID);

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, queryCatalog, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, queryCatalog, 1));

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

}// namespace NES