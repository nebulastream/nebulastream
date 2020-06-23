#include <iostream>
#include <Util/Logger.hpp>
#include <gtest/gtest.h>
#include <API/Types/DataTypes.hpp>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>
#include <Util/TestUtils.hpp>

//used tests: QueryCatalogTest, QueryTest

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

    //register logical stream
    std::string qnv =
        "Schema::create()->addField(createField(\"sensor_id\", createArrayDataType(BasicType::CHAR, 10)))->addField(createField(\"timestamp\", UINT64))->addField(createField(\"velocity\", UINT64))->addField(createField(\"quantity\", UINT64));";
    std::string testSchemaFileName = "QnV.hpp";
    std::ofstream out(testSchemaFileName);
    out << qnv;
    out.close();
    wrk1->registerLogicalStream("QnV", testSchemaFileName);

    //register physical stream
    PhysicalStreamConfig conf;
    conf.logicalStreamName = "QnV";
    conf.physicalStreamName = "test_stream";
    conf.sourceType = "DefaultSource";
    conf.sourceConfig = "../tests/test_data/sample_qnv_V0.3.csv";
    conf.numberOfBuffersToProduce = 1;
    // TODO: what are these parameters
    conf.sourceFrequency = 1;
    wrk1->registerPhysicalStream(conf);

    std::string filePath = "QnVTestOut.csv";
    remove(filePath.c_str());

    //register query
    std::string query = "Pattern::from(\"QnV\").filter(Attribute(\"velocity\") > 100).sink(PrintSinkDescriptor::create()); ";

    std::string queryId = crd->addQuery(query, "BottomUp");
    EXPECT_NE(queryId, "");

    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(wrk1, queryId, 1));
    ASSERT_TRUE(TestUtils::checkCompleteOrTimeout(crd, queryId, 1));

    ASSERT_TRUE(crd->removeQuery(queryId));

    //string expectedContent ="" ;

    std::ifstream ifs(filePath.c_str());
    EXPECT_TRUE(ifs.good());
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        (std::istreambuf_iterator<char>()));

    cout << "content=" << content << endl;
    //cout << "expContent=" << expectedContent << endl;

    //EXPECT_EQ(content, expectedContent);

    bool retStopWrk = wrk1->stop(false);
    EXPECT_TRUE(retStopWrk);

    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

}