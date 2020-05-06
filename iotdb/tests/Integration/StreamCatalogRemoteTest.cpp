#include <gtest/gtest.h>
#include <Util/Logger.hpp>
#include <Catalogs/PhysicalStreamConfig.hpp>
#include <Components/NesWorker.hpp>
#include <Components/NesCoordinator.hpp>

namespace NES {

class StreamCatalogRemoteTest : public testing::Test {
  public:
    std::string host = "localhost";
    std::string queryString =
        "InputQuery inputQueryPtr = InputQuery::from(default_logical).filter(default_logical[\"id\"] < 42).print(std::cout); "
        "return inputQueryPtr;";

    void SetUp() {
        NES::setupLogging("StreamCatalogRemoteTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup StreamCatalogRemoteTest test class.");
    }

    void TearDown() {
        std::cout << "Tear down StreamCatalogRemoteTest test class." << std::endl;
    }
};
TEST_F(StreamCatalogRemoteTest, test_add_log_stream_remote_test) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    //create test schema
    std::string testSchema =
        "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
        "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool registered = wrk->registerLogicalStream("testStream1", testSchemaFileName);
    EXPECT_TRUE(registered);

    SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
        "testStream1");
    EXPECT_NE(sPtr, nullptr);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, test_add_existing_log_stream_remote_test) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    //create test schema
    std::string testSchema =
        "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
        "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool success = wrk->registerLogicalStream("default_logical", testSchemaFileName);
    EXPECT_TRUE(!success);

    SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
        "default_logical");
    EXPECT_NE(sPtr, nullptr);

    //check if schma was not overwritten
    SchemaPtr sch = StreamCatalog::instance().getSchemaForLogicalStream(
        "default_logical");
    EXPECT_NE(sch, nullptr);

    map<std::string, SchemaPtr> allLogicalStream = StreamCatalog::instance()
        .getAllLogicalStream();
    string exp = "id:UINT32 value:UINT64 ";
    EXPECT_EQ(allLogicalStream.size(), 2);

    SchemaPtr defaultSchema = allLogicalStream["default_logical"];
    EXPECT_EQ(exp, defaultSchema->toString());

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, test_add_remove_empty_log_stream_remote_test) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    //create test schema
    std::string testSchema =
        "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
        "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool success = wrk->registerLogicalStream("testStream", testSchemaFileName);
    EXPECT_TRUE(success);

    SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
        "testStream");
    EXPECT_NE(sPtr, nullptr);

    bool success2 = wrk->deregisterLogicalStream("testStream");
    EXPECT_TRUE(success2);

    SchemaPtr sPtr2 = StreamCatalog::instance().getSchemaForLogicalStream(
        "testStream");
    EXPECT_EQ(sPtr2, nullptr);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, test_add_remove_not_empty_log_stream_remote_test) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool success = wrk->deregisterLogicalStream("default_logical");
    EXPECT_TRUE(!success);

    SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
        "default_logical");
    EXPECT_NE(sPtr, nullptr);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, add_physical_to_existing_logical_stream_remote_test) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    PhysicalStreamConfig conf;
    conf.logicalStreamName = "default_logical";
    conf.physicalStreamName = "physical_test";
    conf.sourceType = "DefaultSource";
    conf.numberOfBuffersToProduce = 2;

    bool success = wrk->registerPhysicalStream(conf);
    EXPECT_TRUE(success);

    cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
         << endl;
    std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
        .getPhysicalStreams("default_logical");

    EXPECT_EQ(phys.size(), 2);
    EXPECT_EQ(phys[0]->getPhysicalName(), "default_physical");
    EXPECT_EQ(phys[1]->getPhysicalName(), "physical_test");

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, add_physical_to_new_logical_stream_remote_test) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    //create test schema
    std::string testSchema =
        "Schema::create()->addField(\"id\", BasicType::UINT32)->addField("
        "\"value\", BasicType::UINT64);";
    std::string testSchemaFileName = "testSchema.hpp";
    std::ofstream out(testSchemaFileName);
    out << testSchema;
    out.close();

    bool success = wrk->registerLogicalStream("testStream", testSchemaFileName);
    EXPECT_TRUE(success);

    PhysicalStreamConfig conf;
    conf.logicalStreamName = "testStream";
    conf.physicalStreamName = "physical_test";
    conf.sourceType = "DefaultSource";
    conf.numberOfBuffersToProduce = 2;

    bool success2 = wrk->registerPhysicalStream(conf);
    EXPECT_TRUE(success2);

    cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
         << endl;
    std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
        .getPhysicalStreams("testStream");

    EXPECT_EQ(phys.size(), 1);
    EXPECT_EQ(phys[0]->getPhysicalName(), "physical_test");

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, remove_physical_from_new_logical_stream_remote_test) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool success = wrk->deregisterPhysicalStream("default_logical", "default_physical");
    EXPECT_TRUE(success);

    cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
         << endl;
    std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
        .getPhysicalStreams("default_logical");

    EXPECT_EQ(phys.size(), 0);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}

TEST_F(StreamCatalogRemoteTest, remove_not_existing_stream_remote_test) {
    NesCoordinatorPtr crd = std::make_shared<NesCoordinator>();
    size_t port = crd->startCoordinator(/**blocking**/false);
    EXPECT_NE(port, 0);
    cout << "coordinator started successfully" << endl;

    cout << "start worker" << endl;
    NesWorkerPtr wrk = std::make_shared<NesWorker>();
    bool retStart = wrk->start(/**blocking**/false, /**withConnect**/true, port, "localhost");
    EXPECT_TRUE(retStart);
    cout << "worker started successfully" << endl;

    bool success = wrk->deregisterPhysicalStream("default_logical2", "default_physical");
    EXPECT_TRUE(!success);

    SchemaPtr sPtr = StreamCatalog::instance().getSchemaForLogicalStream(
        "default_logical");
    EXPECT_NE(sPtr, nullptr);

    cout << StreamCatalog::instance().getPhysicalStreamAndSchemaAsString()
         << endl;
    std::vector<StreamCatalogEntryPtr> phys = StreamCatalog::instance()
        .getPhysicalStreams("default_logical");

    EXPECT_EQ(phys.size(), 1);

    cout << "stopping worker" << endl;
    bool retStopWrk = wrk->stop(false);
    EXPECT_TRUE(retStopWrk);

    cout << "stopping coordinator" << endl;
    bool retStopCord = crd->stopCoordinator(false);
    EXPECT_TRUE(retStopCord);
}
}
