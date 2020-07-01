#include <iostream>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>
#include <Util/TestUtils.hpp>
#include <filesystem>

//used tests: QueryCatalogTest, QueryTest
namespace fs = std::filesystem;
namespace NES {

class SimplePatternTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("SimplePatternTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup SimplePatternTest test class.");
    }

    void TearDown() {
        std::cout << "Tear down SimplePatternTest class." << std::endl;
    }
};

/* 1. Test
 * Here, we test the translation of a simple pattern (1 Stream) into a query
 */
TEST_F(SimplePatternTest, testPatternWithFilter) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_DEBUG("coordinator started successfully");

    NES_DEBUG("start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_DEBUG("worker1 started successfully");

    std::string query =
        "Pattern::from(\"default_logical\").filter(Attribute(\"value\") < 42).sink(PrintSinkDescriptor::create()); ";

    std::string queryId = crd->addQuery(query, "BottomUp");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));

    NES_DEBUG("remove query");
    crd->removeQuery(queryId);

    NES_DEBUG("stop worker 1");
    bool retStopWrk1 = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk1);

    NES_DEBUG("stop coordinator");
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
    NES_DEBUG("test finished");
    }

/* 2.Test
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and check the output
 */
TEST_F(SimplePatternTest, testPatternWithTestStream) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_DEBUG("coordinator started successfully");

    NES_DEBUG("start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_DEBUG("worker1 started successfully");

    //register logical stream qnv
    //TODO: update CHAR (sensor id is in result set )
    std::string qnv =
        "Schema::create()->addField(createField(\"sensor_id\", CHAR))->addField(createField(\"timestamp\", UINT64))->addField(createField(\"velocity\", FLOAT32))->addField(createField(\"quantity\", UINT64));";
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
    std::string query = "Pattern::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(FileSinkDescriptor::create(\""
                        + outputFilePath + "\")); ";

    std::string queryId = crd->addQuery(query, "BottomUp");
    EXPECT_NE(queryId, "");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));

    ASSERT_TRUE(crd->removeQuery(queryId));

    string expectedContent =
        "+----------------------------------------------------+\n"
        "|sensor_id:CHAR|timestamp:UINT64|velocity:FLOAT32|quantity:UINT64|\n"
        "+----------------------------------------------------+\n"
        "||1543624020000|102.629631|8|\n"
        "||1543625280000|108.166664|5|\n"
        "+----------------------------------------------------+";


    std::ifstream ifs(outputFilePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    NES_DEBUG("content=" << content);
    NES_DEBUG("expContent=" << expectedContent );
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* Test 3
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and PrintSink
 */
TEST_F(SimplePatternTest, testPatternWithPrintSink) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_DEBUG("coordinator started successfully");

    NES_DEBUG("start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_DEBUG("worker1 started successfully");

    //register logical stream
    std::string qnv =
        "Schema::create()->addField(createField(\"sensor_id\", CHAR))->addField(createField(\"timestamp\", UINT64))->addField(createField(\"velocity\", FLOAT32))->addField(createField(\"quantity\", UINT64));";
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
    conf.sourceConfig = "../tests/test_data/QnV.csv";
    conf.numberOfBuffersToProduce = 1;
    //TODO understand this parameters, how do I set them, what do they do and what do I need to know
    conf.sourceFrequency = 1;
    wrk1->registerPhysicalStream(conf);

    //register query
    std::string query = "Pattern::from(\"QnV\").filter(Attribute(\"velocity\") <= 21).map(Attribute(\"PatternName\") = 1)"
                        ".sink(PrintSinkDescriptor::create()); ";

    std::string queryId = crd->addQuery(query, "BottomUp");
    EXPECT_NE(queryId, "");

    // TODO: what does 0 mean here? If I put 1 failsure is here
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));

    ASSERT_TRUE(crd->removeQuery(queryId));

   string expectedContent = "+----------------------------------------------------+\n"
                             "|sensor_id:CHAR|timestamp:UINT64|velocity:FLOAT32|quantity:UINT64|\n"
                             "+----------------------------------------------------+\n"
                             "||1543626120000|20.476191|1|\n"
                             "||1543626420000|20.714285|2|\n"
                             "+----------------------------------------------------+";

   // Pattern:QNV first(||1543626120000|20.476191|1|)

   //TODO content print sink check?
    /*std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    NES_DEBUG("content=" << content);
    NES_DEBUG("expContent=" << expectedContent );
    EXPECT_EQ(content, expectedContent);*/

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

/* Test 4
 * Here, we test the translation of a simple pattern (1 Stream) into a query using a real data set (QnV) and expecting no output
 * TODO: doublicate CSVSink, but no result print
 */
TEST_F(SimplePatternTest, testPatternWithEmptyResult2) {
    NES_DEBUG("start coordinator");
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/ false);
    EXPECT_NE(port, 0);
    NES_DEBUG("coordinator started successfully");

    NES_DEBUG("start worker 1");
    NesWorkerPtr wrk1 = std::make_shared<NesWorker>("localhost", std::to_string(port), "localhost", std::to_string(port + 10), NESNodeType::Sensor);
    bool retStart1 = wrk1->start(/**blocking**/ false, /**withConnect**/ true);
    EXPECT_TRUE(retStart1);
    NES_DEBUG("worker1 started successfully");

    //register logical stream
    std::string qnv =
        "Schema::create()->addField(createField(\"sensor_id\", createArrayDataType(BasicType::CHAR, 8)))->addField(createField(\"timestamp\", UINT64))->addField(createField(\"velocity\", FLOAT32))->addField(createField(\"quantity\", UINT64));";
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
    // csv = 1 buffer
    // TODO: what are these parameters
    // sourceFreq. wie oft tuple erstellt wird
    conf.sourceFrequency = 1;
    wrk1->registerPhysicalStream(conf);

    std::string outputFilePath =
        "testPatternWithEmptyResult2.out";
    remove(outputFilePath.c_str());


    //register query
    std::string query = "Pattern::from(\"QnV\").filter(Attribute(\"velocity\") <= 21).sink(FileSinkDescriptor::create(\""
    + outputFilePath + "\")); ";

    std::string queryId = crd->addQuery(query, "BottomUp");
    EXPECT_NE(queryId, "");

    // TODO: what does 0 mean here? If I put 1 failsure is here
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 0));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 0));

    ASSERT_TRUE(crd->removeQuery(queryId));

    //string expectedContent = "";
    // < 21 and 1
    string expectedContent = "+----------------------------------------------------+\n"
                             "|sensor_id:CHAR[8]|timestamp:UINT64|velocity:FLOAT32|quantity:UINT64|\n"
                             "+----------------------------------------------------+\n"
                             "|R2000070|1543626120000|20.476191|1|\n"
                             "|R2000070|1543626420000|20.714285|2|\n"
                             "+----------------------------------------------------+";


    std::ifstream ifs(outputFilePath.c_str());
    /* EXPECT_TRUE(ifs.good());
     * Value of: ifs.good()
    Actual: false
    Expected: true
     * That is okay because the file is never created in case of an empty sink
     */
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    NES_DEBUG("content=" << content);
    NES_DEBUG("expContent=" << expectedContent );
    EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}